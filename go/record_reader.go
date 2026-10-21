// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bigquery

import (
	"bytes"
	"context"
	"errors"
	"log"
	"log/slog"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"
)

// MetadataKeyBigqueryQueryID is the Arrow schema metadata key under which the
// driver publishes the BigQuery job ID that produced the result set.
const MetadataKeyBigqueryQueryID = "BIGQUERY:query_id"

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.RecordBatch
	curChIndex int
	rec        arrow.RecordBatch
	errMu      sync.RWMutex
	err        error

	cancelFn context.CancelFunc
}

func checkContext(ctx context.Context, maybeErr error) error {
	if maybeErr != nil {
		return maybeErr
	} else if errors.Is(ctx.Err(), context.Canceled) {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusCancelled}
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusTimeout}
	}
	return ctx.Err()
}

func getQueryParameter(values arrow.RecordBatch, row int, parameterMode string) ([]bigquery.QueryParameter, error) {
	parameters := make([]bigquery.QueryParameter, values.NumCols())
	includeName := parameterMode == OptionValueQueryParameterModeNamed
	schema := values.Schema()
	for i, v := range values.Columns() {
		pi, err := arrowValueToQueryParameterValue(schema.Field(i), v, row)
		if err != nil {
			return nil, err
		}
		parameters[i] = pi
		if includeName {
			parameters[i].Name = values.ColumnName(i)
		}
	}
	return parameters, nil
}

func queryRecordWithSchemaCallback(ctx context.Context, logger *slog.Logger, group *errgroup.Group, client *bigquery.Client, query *bigquery.Query, rec arrow.RecordBatch, ch chan arrow.RecordBatch, parameterMode string, alloc memory.Allocator, rdrSchema func(schema *arrow.Schema), st *statement) (int64, error) {
	totalRows := int64(-1)
	for i := range int(rec.NumRows()) {
		parameters, err := getQueryParameter(rec, i, parameterMode)
		if err != nil {
			return -1, err
		}
		if parameters != nil {
			query.Parameters = parameters
		}

		arrowIterator, jobStatistics, jobID, rows, err := runQuery(ctx, logger, client, query, false, st)
		if err != nil {
			return -1, err
		}
		totalRows = rows
		rdr, schema, err := ipcReaderFromArrowIterator(arrowIterator, jobStatistics, jobID, alloc)
		if err != nil {
			return -1, err
		}
		rdrSchema(schema)
		group.Go(func() error {
			defer rdr.Release()
			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.RecordBatch()
				rec.Retain()
				select {
				case ch <- rec:
				case <-ctx.Done():
					rec.Release()
					return checkContext(ctx, nil)
				}
			}
			return checkContext(ctx, rdr.Err())
		})
	}
	return totalRows, nil
}

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, logger *slog.Logger, client *bigquery.Client, query *bigquery.Query, boundParameters array.RecordReader, parameterMode string, alloc memory.Allocator, resultRecordBufferSize, prefetchConcurrency int, st *statement) (bigqueryRdr array.RecordReader, totalRows int64, err error) {
	if boundParameters == nil {
		return runPlainQuery(ctx, logger, client, query, alloc, resultRecordBufferSize, st)
	}
	defer boundParameters.Release()

	totalRows = 0
	// BigQuery can expose result sets as multiple streams when using certain APIs
	// for now lets keep this and set the number of channels to 1
	// when we need to adapt to multiple streams we can change the value here
	chs := make([]chan arrow.RecordBatch, 1)

	ch := make(chan arrow.RecordBatch, resultRecordBufferSize)
	group, ctx := errgroup.WithContext(ctx)
	// TODO(lidavidm): we never actually make use of concurrency
	group.SetLimit(prefetchConcurrency)
	ctx, cancelFn := context.WithCancel(ctx)
	chs[0] = ch

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	rdr := &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		schema:   nil,
	}

	for boundParameters.Next() {
		rec := boundParameters.RecordBatch()
		// Each call to Record() on the record reader is allowed to release the previous record
		// and since we're doing this sequentially
		// we don't need to call rec.Retain() here and call call rec.Release() in queryRecordWithSchemaCallback
		batchRows, err := queryRecordWithSchemaCallback(ctx, logger, group, client, query, rec, ch, parameterMode, alloc, func(schema *arrow.Schema) {
			rdr.schema = schema
		}, st)
		if err != nil {
			return nil, -1, err
		}
		totalRows += batchRows
	}
	rdr.setError(group.Wait())
	defer close(ch)
	return rdr, totalRows, nil
}

func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		r.cancelFn()
		for _, ch := range r.chs {
			for rec := range ch {
				rec.Release()
			}
		}
	}
}

func (r *reader) Err() error {
	r.errMu.RLock()
	defer r.errMu.RUnlock()
	return r.err
}

func (r *reader) setError(err error) {
	r.errMu.Lock()
	r.err = errors.Join(r.err, err)
	r.errMu.Unlock()
}

func (r *reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if r.curChIndex >= len(r.chs) {
		return false
	}
	var ok bool
	for r.curChIndex < len(r.chs) {
		if r.rec, ok = <-r.chs[r.curChIndex]; ok {
			break
		}
		r.curChIndex++
	}
	return r.rec != nil
}

func (r *reader) Schema() *arrow.Schema {
	return r.schema
}

func (r *reader) Record() arrow.RecordBatch {
	return r.rec
}

func (r *reader) RecordBatch() arrow.RecordBatch {
	return r.rec
}

type emptyArrowIterator struct {
	schema bigquery.Schema
}

func (e emptyArrowIterator) Next() (*bigquery.ArrowRecordBatch, error) {
	return nil, errors.New("Next should never be invoked on an empty iterator")
}

func (e emptyArrowIterator) Schema() bigquery.Schema {
	return e.schema
}

func (e emptyArrowIterator) SerializedArrowSchema() []byte {
	emptySchema := arrow.NewSchema([]arrow.Field{}, nil)

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(emptySchema))

	err := writer.Close()
	if err != nil {
		log.Fatalf("Error serializing an empty schema: %v", err)
	}

	return buf.Bytes()
}

var _ array.RecordReader = (*reader)(nil)
