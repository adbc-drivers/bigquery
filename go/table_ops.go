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

	// For compatibility with dbt-core, perform a blind write when updating the table.
	// dbt-core uses Client.update_table on a new_table instance,
	// which does not carry an ETag from an existing table fetch:
	// https://github.com/dbt-labs/dbt-adapters/blob/9fce78f44db248ba33832c0f65c884a5139c0169/dbt-bigquery/src/dbt/adapters/bigquery/impl.py#L788-L789
	//
	// According to BigQuery documentation, if table.etag is set, updates will only succeed
	// if the server table's ETag matches:
	// https://docs.cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client#google_cloud_bigquery_client_Client_update_table
	// Fetching a table, modifying its fields, and updating with the ETag ensures optimistic concurrency,
	// i.e., changes only persist if there were no intervening updates.
	//
	// Passing an empty ETag string here conforms to dbt-core's expectation:
	// it causes a blind write regardless of the server-side ETag.
	// See also: https://pkg.go.dev/cloud.google.com/go/bigquery#Table.Update
	//
	// BigQuery table metadata is eventually consistent. Even if a Tables.Get
	// request fetches an ETag immediately after a metadata-changing DDL (such as
	// ALTER TABLE ... SET OPTIONS(...)), transient mismatches or update errors may still occur
	// due to propagation delays—even if operations are sequential.
	if _, err := table.Update(ctx, bigquery.TableMetadataToUpdate{Schema: newSchema}, ""); err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "update table schema")
	}
	return emptyResult()
}
