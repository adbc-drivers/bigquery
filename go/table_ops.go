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

// Table-mutation helpers ported from the legacy dbt-labs/arrow-adbc bigquery
// driver. Each helper is dispatched from statement.ExecuteQuery when the
// corresponding statement option is set.

package bigquery

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// emptyResult returns a record reader with an empty schema/no batches, used

// for statement execution paths that produce no result set.

func emptyResult() (array.RecordReader, int64, error) {
	emptySchema := arrow.NewSchema([]arrow.Field{}, nil)
	reader, err := array.NewRecordReader(emptySchema, []arrow.RecordBatch{})
	if err != nil {
		return nil, -1, err
	}
	return reader, 0, nil
}

// executeUpdateTableColumnsMetadata updates column-level descriptions

// on the table referenced by queryConfig.Dst. The input is a JSON object

// {column: string}. Columns not present in the map are left untouched.

func (st *statement) executeUpdateTableColumnsMetadata(ctx context.Context) (array.RecordReader, int64, error) {
	if st.queryConfig.Dst == nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[bq] update_columns requires a destination table",
		}
	}

	var columnDescriptions map[string]string
	if st.updateTableColumnsDescription != "" {
		if err := json.Unmarshal([]byte(st.updateTableColumnsDescription), &columnDescriptions); err != nil {
			return nil, -1, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] parse column descriptions JSON: %v", err),
			}
		}
	}

	table := st.queryConfig.Dst
	md, err := table.Metadata(ctx)
	if err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "get table metadata")
	}

	newSchema := make([]*bigquery.FieldSchema, len(md.Schema))
	for i, field := range md.Schema {
		nf := &bigquery.FieldSchema{
			Name:        field.Name,
			Type:        field.Type,
			Description: field.Description,
			Repeated:    field.Repeated,
			Required:    field.Required,
			Schema:      field.Schema,
			PolicyTags:  field.PolicyTags,
		}
		if d, ok := columnDescriptions[field.Name]; ok {
			nf.Description = d
		}
		newSchema[i] = nf
	}
	if _, err := table.Update(ctx, bigquery.TableMetadataToUpdate{Schema: newSchema}, md.ETag); err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "update table schema")
	}
	return emptyResult()
}
