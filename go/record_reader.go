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
	"sync/atomic"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"
)

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.RecordBatch
	curChIndex int
	rec        arrow.RecordBatch
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

func runQuery(ctx context.Context, logger *slog.Logger, query *bigquery.Query, executeUpdate bool) (bigquery.ArrowIterator, *bigquery.JobStatus, int64, error) {
	job, err := query.Run(ctx)
	if err != nil {
		return nil, nil, -1, errToAdbcErr(adbc.StatusInternal, err, "run query")
	}

	// XXX: Google SDK badness.  We can't use Wait here because queries that
	// *fail* with a rateLimitExceeded (e.g. too many metadata operations)
	// will get the *polling* retried infinitely in Google's SDK (I believe
	// the SDK wants to retry "polling for job status" rate limit exceeded but
	// doesn't differentiate between them because googleapi.CheckResponse
	// appears to put the API error from the response object as an error of
	// the API call, from digging around using a debugger.  In other words, it
	// seems to be confusing "I got an error that my API request was rate
	// limited" and "I got an error that my job was rate limited" because
	// their internal APIs mix both errors into a single error path.)
	js, err := safeWaitForJob(ctx, logger, job)
	if err != nil {
		return nil, nil, -1, err
	}

	if err := js.Err(); err != nil {
		return nil, js, -1, errToAdbcErr(adbc.StatusInternal, err, "complete job")
	} else if !js.Done() {
		return nil, js, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[bq] Query job did not complete",
		}
	}

	isSelectOrCall := false
	stats, statsOk := js.Statistics.Details.(*bigquery.QueryStatistics)
	if executeUpdate {
		if statsOk {
			return nil, js, stats.NumDMLAffectedRows, nil
		}
		return nil, js, -1, nil
	} else if query.DryRun {
		return nil, js, js.Statistics.TotalBytesProcessed, nil
	} else if statsOk {
		isSelectOrCall = stats.StatementType == "SELECT" || stats.StatementType == "CALL"
	}

	// XXX: the Google SDK badness also applies here; it makes a similar
	// mistake with the retry, so we wait for the job above.
	iter, err := job.Read(ctx)
	if err != nil {
		return nil, js, -1, errToAdbcErr(adbc.StatusInternal, err, "read query results")
	}

	var arrowIterator bigquery.ArrowIterator
	// We need to detect if this actually returned data. Originally we
	// checked for the presence of a schema, but it turns out statements
	// like CREATE VIEW return a schema! Then we checked if there are
	// rows, but it turns out that bigquery-emulator returns
	// iter.TotalRows == 0 (this is valid as per the API: the field is not
	// _necessarily_ populated until after a call to Next). Finally we use
	// job statistics instead
	if isSelectOrCall {
		// !IsAccelerated() -> failed to get Arrow stream -> we are
		// probably lacking permissions.  readSessionUser may sound
		// unrelated but creating a "read session" is the first step
		// of using the Storage API.  Note that Google swallows the
		// real error, so this is the best we can do.
		// https://cloud.google.com/bigquery/docs/reference/storage#create_a_session
		if !iter.IsAccelerated() {
			return nil, js, -1, adbc.Error{
				Code: adbc.StatusUnauthorized,
				Msg:  "[bq] Arrow reader requires roles/bigquery.readSessionUser, see https://github.com/apache/arrow-adbc/issues/3282",
			}
		}
		if arrowIterator, err = iter.ArrowIterator(); err != nil {
			return nil, js, -1, errToAdbcErr(adbc.StatusInternal, err, "read Arrow query results")
		}
	} else {
		arrowIterator = emptyArrowIterator{iter.Schema}
	}
	totalRows := int64(iter.TotalRows)
	return arrowIterator, js, totalRows, nil
}

func ipcReaderFromArrowIterator(arrowIterator bigquery.ArrowIterator, jobStatistics *bigquery.JobStatistics, alloc memory.Allocator) (*ipc.Reader, *arrow.Schema, error) {
	arrowItReader := bigquery.NewArrowIteratorReader(arrowIterator)
	rdr, err := ipc.NewReader(arrowItReader, ipc.WithAllocator(alloc))

	fields := make([]arrow.Field, len(arrowIterator.Schema()))
	for i, field := range arrowIterator.Schema() {
		fields[i], err = buildField(field, 0)
		if err != nil {
			return nil, nil, err
		}
	}

	if err != nil {
		return nil, nil, err
	}

	metadata, err := metadataFromJobStatistics(jobStatistics)
	if err != nil {
		return nil, nil, err
	}
	return rdr, arrow.NewSchema(fields, metadata), nil
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

func makeDryRunReader(js *bigquery.JobStatus) (array.RecordReader, error) {
	metadata, err := metadataFromJobStatistics(js.Statistics)
	if err != nil {
		return nil, err
	}

	statistics, ok := js.Statistics.Details.(*bigquery.QueryStatistics)
	var schema *arrow.Schema
	if !ok {
		// No schema, return an empty schema
		schema = arrow.NewSchema([]arrow.Field{}, metadata)
	} else {
		bqSchema := statistics.Schema
		fields := make([]arrow.Field, len(bqSchema))
		for i, field := range bqSchema {
			var err error
			fields[i], err = buildField(field, 0)
			if err != nil {
				return nil, err
			}
		}
		schema = arrow.NewSchema(fields, metadata)
	}
	rdr, _ := array.NewRecordReader(schema, []arrow.RecordBatch{})
	return rdr, nil
}

func runPlainQuery(ctx context.Context, logger *slog.Logger, query *bigquery.Query, alloc memory.Allocator, resultRecordBufferSize int) (bigqueryRdr array.RecordReader, totalRows int64, err error) {
	arrowIterator, jobStatus, totalRows, err := runQuery(ctx, logger, query, false)
	if err != nil {
		return nil, -1, err
	} else if query.DryRun || arrowIterator == nil {
		// Dry run queries don't have an arrow iterator, so return an empty reader
		rdr, err := makeDryRunReader(jobStatus)
		if err != nil {
			return nil, -1, err
		}
		return rdr, totalRows, nil
	}

	rdr, schema, err := ipcReaderFromArrowIterator(arrowIterator, jobStatus.Statistics, alloc)
	if err != nil {
		return nil, -1, err
	}

	chs := make([]chan arrow.RecordBatch, 1)
	ctx, cancelFn := context.WithCancel(ctx)
	ch := make(chan arrow.RecordBatch, resultRecordBufferSize)
	chs[0] = ch

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	bigqueryRdr = &reader{
		refCount:   1,
		chs:        chs,
		curChIndex: 0,
		err:        nil,
		cancelFn:   cancelFn,
		schema:     schema,
	}

	go func() {
		defer rdr.Release()
		for rdr.Next() && ctx.Err() == nil {
			rec := rdr.RecordBatch()
			rec.Retain()
			ch <- rec
		}

		// TODO(lidavidm): doesn't this discard the error?
		err = checkContext(ctx, rdr.Err())
		defer close(ch)
	}()
	return bigqueryRdr, totalRows, nil
}

func queryRecordWithSchemaCallback(ctx context.Context, logger *slog.Logger, group *errgroup.Group, query *bigquery.Query, rec arrow.RecordBatch, ch chan arrow.RecordBatch, parameterMode string, alloc memory.Allocator, rdrSchema func(schema *arrow.Schema)) (int64, error) {
	totalRows := int64(-1)
	for i := range int(rec.NumRows()) {
		parameters, err := getQueryParameter(rec, i, parameterMode)
		if err != nil {
			return -1, err
		}
		if parameters != nil {
			query.Parameters = parameters
		}

		arrowIterator, jobStatus, rows, err := runQuery(ctx, logger, query, false)
		if err != nil {
			return -1, err
		} else if arrowIterator == nil {
			// Dry run
			rdr, err := makeDryRunReader(jobStatus)
			if err != nil {
				return -1, err
			}
			rdrSchema(rdr.Schema())
			rdr.Release()
			continue
		}
		totalRows = rows
		rdr, schema, err := ipcReaderFromArrowIterator(arrowIterator, jobStatus.Statistics, alloc)
		if err != nil {
			return -1, err
		}
		rdrSchema(schema)
		group.Go(func() error {
			defer rdr.Release()
			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.RecordBatch()
				rec.Retain()
				ch <- rec
			}
			return checkContext(ctx, rdr.Err())
		})
	}
	return totalRows, nil
}

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, logger *slog.Logger, query *bigquery.Query, boundParameters array.RecordReader, parameterMode string, alloc memory.Allocator, resultRecordBufferSize, prefetchConcurrency int) (bigqueryRdr array.RecordReader, totalRows int64, err error) {
	if boundParameters == nil {
		return runPlainQuery(ctx, logger, query, alloc, resultRecordBufferSize)
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
		batchRows, err := queryRecordWithSchemaCallback(ctx, logger, group, query, rec, ch, parameterMode, alloc, func(schema *arrow.Schema) {
			rdr.schema = schema
		})
		if err != nil {
			return nil, -1, err
		}
		totalRows += batchRows
	}
	rdr.err = group.Wait()
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
	return r.err
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
