// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Row-based Arrow iterator used when the caller opts out of the Storage Read
// API (see OptionQueryUseStorageApiDisabledClient). Pseudo-columns like
// _PARTITIONDATE and _PARTITIONTIME are silently nulled out by the Storage
// API, so this path walks bigquery.RowIterator directly, materializing
// batches of rows into Arrow record batches and re-serializing them through
// IPC so downstream code that expects bigquery.ArrowIterator sees the same
// interface.

package bigquery

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/api/iterator"
)

// RowBasedArrowIterator wraps a bigquery.RowIterator and implements
// bigquery.ArrowIterator. Used when the Storage Read API can't be used
// (e.g. to read pseudo-columns like _PARTITIONTIME).
type RowBasedArrowIterator struct {
	iter   *bigquery.RowIterator
	schema bigquery.Schema
	alloc  memory.Allocator
	done   bool
}

func newRowBasedArrowIterator(iter *bigquery.RowIterator, alloc memory.Allocator) bigquery.ArrowIterator {
	return &RowBasedArrowIterator{
		iter:   iter,
		schema: iter.Schema,
		alloc:  alloc,
	}
}

// Next returns the next batch of rows as an Arrow record batch, IPC-encoded
// so the returned bytes plug directly into
// bigquery.NewArrowIteratorReader downstream.
func (l *RowBasedArrowIterator) Next() (*bigquery.ArrowRecordBatch, error) {
	if l.done {
		return nil, iterator.Done
	}

	const batchSize = 1000
	rows := make([][]bigquery.Value, 0, batchSize)

	for i := 0; i < batchSize; i++ {
		var row []bigquery.Value
		err := l.iter.Next(&row)
		if err == iterator.Done {
			l.done = true
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return nil, iterator.Done
	}

	batch, err := rowsToArrowRecordBatch(l.schema, rows, l.alloc)
	if err != nil {
		return nil, err
	}
	defer batch.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()), ipc.WithAllocator(l.alloc))
	if err := writer.Write(batch); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	return &bigquery.ArrowRecordBatch{
		Data: buf.Bytes(),
	}, nil
}

// Schema returns the BigQuery schema of the underlying row iterator.
func (l *RowBasedArrowIterator) Schema() bigquery.Schema {
	return l.schema
}

// SerializedArrowSchema returns the Arrow schema (from `buildField`) as IPC
// bytes so it can be fed into an ipc.Reader on the consuming side.
func (l *RowBasedArrowIterator) SerializedArrowSchema() []byte {
	fields := make([]arrow.Field, len(l.schema))
	for i, field := range l.schema {
		f, err := buildField(field, 0)
		if err != nil {
			log.Fatalf("Error building field %s: %v", field.Name, err)
		}
		fields[i] = f
	}
	arrowSchema := arrow.NewSchema(fields, nil)

	var buf bytes.Buffer
	_ = ipc.NewWriter(&buf, ipc.WithSchema(arrowSchema))
	return buf.Bytes()
}

// rowsToArrowRecordBatch converts a slice of bigquery.Value rows into an
// Arrow record batch matching the given BigQuery schema.
func rowsToArrowRecordBatch(schema bigquery.Schema, rows [][]bigquery.Value, alloc memory.Allocator) (arrow.RecordBatch, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows to convert")
	}

	fields := make([]arrow.Field, len(schema))
	for i, field := range schema {
		f, err := buildField(field, 0)
		if err != nil {
			return nil, err
		}
		fields[i] = f
	}
	arrowSchema := arrow.NewSchema(fields, nil)

	builders := make([]array.Builder, len(schema))
	for i, field := range fields {
		builders[i] = array.NewBuilder(alloc, field.Type)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	for _, row := range rows {
		for colIdx, val := range row {
			if val == nil {
				builders[colIdx].AppendNull()
				continue
			}

			switch builder := builders[colIdx].(type) {
			case *array.Date32Builder:
				// BigQuery returns civil.Date for DATE columns.
				if d, ok := val.(civil.Date); ok {
					t := time.Date(d.Year, time.Month(d.Month), d.Day, 0, 0, 0, 0, time.UTC)
					builder.Append(arrow.Date32FromTime(t))
				} else if t, ok := val.(time.Time); ok {
					builder.Append(arrow.Date32FromTime(t))
				} else {
					builder.AppendNull()
				}
			case *array.TimestampBuilder:
				if ts, ok := val.(time.Time); ok {
					builder.Append(arrow.Timestamp(ts.UnixMicro()))
				} else {
					builder.AppendNull()
				}
			// TODO: Add support for other types as needed. The Storage-API-
			// disabled path is intended for pseudo-column queries which are
			// almost always DATE/TIMESTAMP; extend here if more types come up.
			default:
				return nil, fmt.Errorf("USE_STORAGE_API_DISABLED_CLIENT is enabled, unsupported type conversion for column type %s of value %v", builder.Type().String(), val)
			}
		}
	}

	arrays := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrays[i] = b.NewArray()
	}

	return array.NewRecordBatch(arrowSchema, arrays, int64(len(rows))), nil
}
