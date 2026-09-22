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
// API (see OptionQueryDisableStorageApi). Pseudo-columns like
// _PARTITIONDATE and _PARTITIONTIME are silently nulled out by the Storage
// API, so this path walks bigquery.RowIterator directly, materializing
// batches of rows into Arrow record batches and re-serializing them through
// IPC so downstream code that expects bigquery.ArrowIterator sees the same
// interface.

package bigquery

import (
	"bytes"
	"fmt"
	"math/big"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
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

	buf    bytes.Buffer
	writer *ipc.Writer
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
		return nil, l.finish()
	}

	const batchSize = 1000
	rows := make([][]bigquery.Value, 0, batchSize)

	for range batchSize {
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
		return nil, l.finish()
	}

	batch, err := rowsToArrowRecordBatch(l.schema, rows, l.alloc)
	if err != nil {
		return nil, err
	}
	defer batch.Release()

	data, err := l.serialize(batch)
	if err != nil {
		return nil, err
	}

	return &bigquery.ArrowRecordBatch{
		Data: data,
	}, nil
}

func (l *RowBasedArrowIterator) serialize(batch arrow.RecordBatch) ([]byte, error) {
	if l.writer == nil {
		l.writer = ipc.NewWriter(&l.buf, ipc.WithSchema(batch.Schema()), ipc.WithAllocator(l.alloc))
	}

	l.buf.Reset()
	if err := l.writer.Write(batch); err != nil {
		return nil, err
	}

	data := make([]byte, l.buf.Len())
	copy(data, l.buf.Bytes())
	return data, nil
}

func (l *RowBasedArrowIterator) finish() error {
	if l.writer == nil {
		return iterator.Done
	}

	err := l.writer.Close()
	l.writer = nil
	l.buf.Reset()
	if err != nil {
		return err
	}
	return iterator.Done
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
			return nil
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
		if len(row) != len(builders) {
			return nil, fmt.Errorf("[bq] row has %d values but schema has %d columns", len(row), len(builders))
		}
		for colIdx, val := range row {
			if err := appendRowValue(builders[colIdx], val); err != nil {
				return nil, err
			}
		}
	}

	arrays := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrays[i] = b.NewArray()
	}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()

	return array.NewRecordBatch(arrowSchema, arrays, int64(len(rows))), nil
}

func appendRowValue(builder array.Builder, val bigquery.Value) error {
	if lb, ok := builder.(*array.ListBuilder); ok {
		items, isList := val.([]bigquery.Value)
		if val != nil && !isList {
			return unsupportedRowValue(builder, val)
		}
		lb.Append(true)
		for _, item := range items {
			if err := appendRowValue(lb.ValueBuilder(), item); err != nil {
				return err
			}
		}
		return nil
	}

	if val == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		v, ok := val.(bool)
		if !ok {
			return unsupportedRowValue(builder, val)
		}
		b.Append(v)
	case *array.Int64Builder:
		v, ok := val.(int64)
		if !ok {
			return unsupportedRowValue(builder, val)
		}
		b.Append(v)
	case *array.Float64Builder:
		v, ok := val.(float64)
		if !ok {
			return unsupportedRowValue(builder, val)
		}
		b.Append(v)
	case *array.StringBuilder:
		v, ok := val.(string)
		if !ok {
			return unsupportedRowValue(builder, val)
		}
		b.Append(v)
	case *array.BinaryBuilder:
		v, ok := val.([]byte)
		if !ok {
			return unsupportedRowValue(builder, val)
		}
		b.Append(v)
	case *array.Date32Builder:
		switch v := val.(type) {
		case civil.Date:
			b.Append(arrow.Date32FromTime(v.In(time.UTC)))
		case time.Time:
			b.Append(arrow.Date32FromTime(v))
		default:
			return unsupportedRowValue(builder, val)
		}
	case *array.Time64Builder:
		v, ok := val.(civil.Time)
		if !ok {
			return unsupportedRowValue(builder, val)
		}
		micros := int64(v.Hour)*int64(time.Hour/time.Microsecond) +
			int64(v.Minute)*int64(time.Minute/time.Microsecond) +
			int64(v.Second)*int64(time.Second/time.Microsecond) +
			int64(v.Nanosecond)/int64(time.Microsecond)
		b.Append(arrow.Time64(micros))
	case *array.TimestampBuilder:
		switch v := val.(type) {
		case time.Time:
			b.Append(arrow.Timestamp(v.UnixMicro()))
		case civil.DateTime:
			b.Append(arrow.Timestamp(v.In(time.UTC).UnixMicro()))
		default:
			return unsupportedRowValue(builder, val)
		}
	case *array.Decimal128Builder:
		v, ok := val.(*big.Rat)
		if !ok {
			return unsupportedRowValue(builder, val)
		}
		unscaled, err := unscaledRat(v, b.Type().(*arrow.Decimal128Type).Scale)
		if err != nil {
			return err
		}
		b.Append(decimal128.FromBigInt(unscaled))
	case *array.Decimal256Builder:
		v, ok := val.(*big.Rat)
		if !ok {
			return unsupportedRowValue(builder, val)
		}
		unscaled, err := unscaledRat(v, b.Type().(*arrow.Decimal256Type).Scale)
		if err != nil {
			return err
		}
		b.Append(decimal256.FromBigInt(unscaled))
	case *array.MonthDayNanoIntervalBuilder:
		v, ok := val.(*bigquery.IntervalValue)
		if !ok {
			return unsupportedRowValue(builder, val)
		}
		b.Append(arrow.MonthDayNanoInterval{
			Months: v.Years*12 + v.Months,
			Days:   v.Days,
			Nanoseconds: int64(v.Hours)*int64(time.Hour) +
				int64(v.Minutes)*int64(time.Minute) +
				int64(v.Seconds)*int64(time.Second) +
				int64(v.SubSecondNanos),
		})
	case *array.StructBuilder:
		switch v := val.(type) {
		case []bigquery.Value:
			if len(v) != b.NumField() {
				return fmt.Errorf("[bq] record has %d values but struct has %d fields", len(v), b.NumField())
			}
			b.Append(true)
			for i, field := range v {
				if err := appendRowValue(b.FieldBuilder(i), field); err != nil {
					return err
				}
			}
		case *bigquery.RangeValue:
			if b.NumField() != 2 {
				return fmt.Errorf("[bq] range has %d fields, expected 2", b.NumField())
			}
			b.Append(true)
			if err := appendRowValue(b.FieldBuilder(0), v.Start); err != nil {
				return err
			}
			if err := appendRowValue(b.FieldBuilder(1), v.End); err != nil {
				return err
			}
		default:
			return unsupportedRowValue(builder, val)
		}
	default:
		return unsupportedRowValue(builder, val)
	}
	return nil
}

func unscaledRat(r *big.Rat, scale int32) (*big.Int, error) {
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	quo, rem := new(big.Int).QuoRem(new(big.Int).Mul(r.Num(), pow), r.Denom(), new(big.Int))
	if rem.Sign() != 0 {
		return nil, fmt.Errorf("[bq] cannot represent %s with scale %d", r.RatString(), scale)
	}
	return quo, nil
}

func unsupportedRowValue(builder array.Builder, val bigquery.Value) error {
	return fmt.Errorf("storage API is disabled, unsupported type conversion for column type %s of value %v", builder.Type().String(), val)
}
