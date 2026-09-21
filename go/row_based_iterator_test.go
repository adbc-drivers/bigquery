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

package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

type serializedBatchIterator struct {
	inner   *RowBasedArrowIterator
	batches []arrow.RecordBatch
	pos     int
}

func (s *serializedBatchIterator) Next() (*bigquery.ArrowRecordBatch, error) {
	if s.pos >= len(s.batches) {
		return nil, s.inner.finish()
	}
	data, err := s.inner.serialize(s.batches[s.pos])
	if err != nil {
		return nil, err
	}
	s.pos++
	return &bigquery.ArrowRecordBatch{Data: data}, nil
}

func (s *serializedBatchIterator) Schema() bigquery.Schema { return s.inner.Schema() }

func (s *serializedBatchIterator) SerializedArrowSchema() []byte {
	return s.inner.SerializedArrowSchema()
}

func TestRowBasedArrowIteratorSpansMultipleBatches(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	bqSchema := bigquery.Schema{
		{Name: "x", Type: bigquery.IntegerFieldType},
	}
	arrowField, err := buildField(bqSchema[0], 0)
	require.NoError(t, err)
	arrowSchema := arrow.NewSchema([]arrow.Field{arrowField}, nil)

	const batchSize = 1000
	const numBatches = 2

	batches := make([]arrow.RecordBatch, 0, numBatches)
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	next := int64(1)
	for range numBatches {
		builder := array.NewInt64Builder(alloc)
		for range batchSize {
			builder.Append(next)
			next++
		}
		arr := builder.NewArray()
		batches = append(batches, array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, batchSize))
		arr.Release()
		builder.Release()
	}

	it := &serializedBatchIterator{
		inner:   &RowBasedArrowIterator{schema: bqSchema, alloc: alloc},
		batches: batches,
	}

	rdr, err := ipc.NewReader(bigquery.NewArrowIteratorReader(it), ipc.WithAllocator(alloc))
	require.NoError(t, err)
	defer rdr.Release()

	expected := int64(1)
	total := int64(0)
	for rdr.Next() {
		rec := rdr.RecordBatch()
		col := rec.Column(0).(*array.Int64)
		for i := range col.Len() {
			require.Equal(t, expected, col.Value(i))
			expected++
		}
		total += rec.NumRows()
	}
	require.NoError(t, rdr.Err())

	require.Equal(t, int64(batchSize*numBatches), total)
}

func TestRowBasedArrowIteratorFinishWithoutBatches(t *testing.T) {
	it := &RowBasedArrowIterator{alloc: memory.DefaultAllocator}
	require.Equal(t, iterator.Done, it.finish())
}
