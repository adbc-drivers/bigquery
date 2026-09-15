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
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyArrowIteratorNext(t *testing.T) {
	iter := emptyArrowIterator{}
	res, err := iter.Next()

	require.Nil(t, res)
	require.Error(t, err)
}

func TestEmptyArrowIteratorSchema(t *testing.T) {
	iter := emptyArrowIterator{}
	schema := iter.Schema()

	require.Empty(t, schema)
}

func TestEmptyArrowIteratorSerializedArrowSchema(t *testing.T) {
	iter := emptyArrowIterator{}
	serializedSchema := iter.SerializedArrowSchema()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	rdr, schema, err := ipcReaderFromArrowIterator(iter, nil, "", alloc)
	require.NoError(t, err)
	defer rdr.Release()
	require.Empty(t, schema.Fields(), "serialized schema had %d bytes", len(serializedSchema))
	require.Empty(t, rdr.Schema().Fields(), "serialized schema had %d bytes", len(serializedSchema))
}

func TestMetadataFromJobStatistics(t *testing.T) {
	stats := sampleJobStatistics()

	md, err := metadataFromJobStatistics(stats, "")
	require.NoError(t, err)
	metadata := md.ToMap()

	assert.Equal(t, "2026-01-02T03:04:05.000000006Z", metadata["BIGQUERY:statistics:creation_time"])
	assert.Equal(t, "1024", metadata["BIGQUERY:statistics:total_bytes_processed"])
	assert.Equal(t, "2000000", metadata["BIGQUERY:statistics:total_slot_duration"])
	assert.Equal(t, "5000000", metadata["BIGQUERY:statistics:final_execution_duration"])
	assert.Equal(t, "ENTERPRISE", metadata["BIGQUERY:statistics:edition"])
	assert.Equal(t, "0.75", metadata["BIGQUERY:statistics:completion_ratio"])
	assert.Equal(t, "3", metadata["BIGQUERY:statistics:query:billing_tier"])
	assert.Equal(t, "true", metadata["BIGQUERY:statistics:query:cache_hit"])
	assert.Equal(t, "SELECT", metadata["BIGQUERY:statistics:query:statement_type"])
	assert.Equal(t, "2048", metadata["BIGQUERY:statistics:query:total_bytes_billed"])
	assert.Equal(t, "4096", metadata["BIGQUERY:statistics:query:total_bytes_processed"])
	assert.Equal(t, "PRECISE", metadata["BIGQUERY:statistics:query:total_bytes_processed_accuracy"])
	assert.Equal(t, "6", metadata["BIGQUERY:statistics:query:num_dml_affected_rows"])
	assert.Equal(t, "33", metadata["BIGQUERY:statistics:query:slot_millis"])
	assert.Equal(t, "CREATE_TABLE", metadata["BIGQUERY:statistics:query:ddl_operation_performed"])

	var reservationUsage []bigquery.ReservationUsage
	require.NoError(t, json.Unmarshal([]byte(metadata["BIGQUERY:statistics:reservation_usage"]), &reservationUsage))
	require.Len(t, reservationUsage, 1)
	assert.Equal(t, "primary", reservationUsage[0].Name)
	assert.Equal(t, int64(12), reservationUsage[0].SlotMillis)

	var dmlStats bigquery.DMLStatistics
	require.NoError(t, json.Unmarshal([]byte(metadata["BIGQUERY:statistics:query:dml_stats"]), &dmlStats))
	assert.Equal(t, int64(1), dmlStats.InsertedRowCount)
	assert.Equal(t, int64(2), dmlStats.DeletedRowCount)
	assert.Equal(t, int64(3), dmlStats.UpdatedRowCount)

	var exportStats bigquery.ExportDataStatistics
	require.NoError(t, json.Unmarshal([]byte(metadata["BIGQUERY:statistics:query:export_data_statistics"]), &exportStats))
	assert.Equal(t, int64(4), exportStats.FileCount)
	assert.Equal(t, int64(5), exportStats.RowCount)

	assert.NotContains(t, metadata, "BIGQUERY:statistics:query:query_plan")
	assert.NotContains(t, metadata, "BIGQUERY:statistics:query:timeline")
	assert.NotContains(t, metadata, "BIGQUERY:statistics:query:referenced_tables")
	assert.NotContains(t, metadata, "BIGQUERY:statistics:query:schema")

	assert.Equal(t, "2026-01-02T03:04:05.000000006Z", metadata["BIGQUERY:Statistics:CreationTime"])
	assert.Equal(t, "1024", metadata["BIGQUERY:Statistics:TotalBytesProcessed"])
	assert.Equal(t, "2000000", metadata["BIGQUERY:Statistics:TotalSlotDuration"])
	assert.Equal(t, "5000000", metadata["BIGQUERY:Statistics:FinalExecutionDuration"])
	assert.Equal(t, "ENTERPRISE", metadata["BIGQUERY:Statistics:Edition"])
	assert.Equal(t, "0.75", metadata["BIGQUERY:Statistics:CompletionRatio"])
	assert.Equal(t, "3", metadata["BIGQUERY:Statistics:Query:BillingTier"])
	assert.Equal(t, "true", metadata["BIGQUERY:Statistics:Query:CacheHit"])
	assert.Equal(t, "SELECT", metadata["BIGQUERY:Statistics:Query:StatementType"])
	assert.Equal(t, "2048", metadata["BIGQUERY:Statistics:Query:TotalBytesBilled"])
	assert.Equal(t, "4096", metadata["BIGQUERY:Statistics:Query:TotalBytesProcessed"])
	assert.Equal(t, "PRECISE", metadata["BIGQUERY:Statistics:Query:TotalBytesProcessedAccuracy"])
	assert.Equal(t, "6", metadata["BIGQUERY:Statistics:Query:NumDMLAffectedRows"])
	assert.Equal(t, "33", metadata["BIGQUERY:Statistics:Query:SlotMillis"])
	assert.Equal(t, "CREATE_TABLE", metadata["BIGQUERY:Statistics:Query:DDLOperationPerformed"])

	var legacyReservationUsage []bigquery.ReservationUsage
	require.NoError(t, json.Unmarshal([]byte(metadata["BIGQUERY:Statistics:ReservationUsage"]), &legacyReservationUsage))
	require.Len(t, legacyReservationUsage, 1)
	assert.Equal(t, "primary", legacyReservationUsage[0].Name)
	assert.Equal(t, int64(12), legacyReservationUsage[0].SlotMillis)

	var legacyDmlStats bigquery.DMLStatistics
	require.NoError(t, json.Unmarshal([]byte(metadata["BIGQUERY:Statistics:Query:DMLStats"]), &legacyDmlStats))
	assert.Equal(t, int64(1), legacyDmlStats.InsertedRowCount)
	assert.Equal(t, int64(2), legacyDmlStats.DeletedRowCount)
	assert.Equal(t, int64(3), legacyDmlStats.UpdatedRowCount)

	var legacyExportStats bigquery.ExportDataStatistics
	require.NoError(t, json.Unmarshal([]byte(metadata["BIGQUERY:Statistics:Query:ExportDataStatistics"]), &legacyExportStats))
	assert.Equal(t, int64(4), legacyExportStats.FileCount)
	assert.Equal(t, int64(5), legacyExportStats.RowCount)

	assert.NotContains(t, metadata, "BIGQUERY:Statistics:Query:QueryPlan")
	assert.NotContains(t, metadata, "BIGQUERY:Statistics:Query:Timeline")
	assert.NotContains(t, metadata, "BIGQUERY:Statistics:Query:ReferencedTables")
	assert.NotContains(t, metadata, "BIGQUERY:Statistics:Query:Schema")
}

func TestIpcReaderFromArrowIteratorAttachesJobStatisticsMetadata(t *testing.T) {
	iter := emptyArrowIterator{}

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	rdr, schema, err := ipcReaderFromArrowIterator(iter, sampleJobStatistics(), "job-abc", alloc)
	require.NoError(t, err)
	defer rdr.Release()

	metadata := schema.Metadata().ToMap()
	assert.Equal(t, "3", metadata["BIGQUERY:statistics:query:billing_tier"])
	assert.Equal(t, "SELECT", metadata["BIGQUERY:statistics:query:statement_type"])
	assert.Equal(t, "job-abc", metadata[MetadataKeyBigqueryQueryID])
}

func TestMakeDryRunReaderAttachesJobStatisticsMetadata(t *testing.T) {
	rdr, err := makeDryRunReader(&bigquery.JobStatus{
		Statistics: sampleJobStatistics(),
	}, "dryrun-job")
	require.NoError(t, err)
	defer rdr.Release()

	metadata := rdr.Schema().Metadata().ToMap()
	assert.Equal(t, "3", metadata["BIGQUERY:statistics:query:billing_tier"])
	assert.Equal(t, "SELECT", metadata["BIGQUERY:statistics:query:statement_type"])
	assert.Equal(t, "dryrun-job", metadata[MetadataKeyBigqueryQueryID])
}

func sampleJobStatistics() *bigquery.JobStatistics {
	return &bigquery.JobStatistics{
		CreationTime:           time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
		StartTime:              time.Date(2026, 1, 2, 3, 4, 6, 7, time.UTC),
		EndTime:                time.Date(2026, 1, 2, 3, 4, 7, 8, time.UTC),
		TotalBytesProcessed:    1024,
		TotalSlotDuration:      2 * time.Millisecond,
		ReservationUsage:       []*bigquery.ReservationUsage{{Name: "primary", SlotMillis: 12}},
		ReservationID:          "reservation-id",
		NumChildJobs:           2,
		ParentJobID:            "parent-job-id",
		TransactionInfo:        &bigquery.TransactionInfo{TransactionID: "transaction-id"},
		SessionInfo:            &bigquery.SessionInfo{SessionID: "session-id"},
		FinalExecutionDuration: 5 * time.Millisecond,
		Edition:                bigquery.ReservationEditionEnterprise,
		CompletionRatio:        0.75,
		Details: &bigquery.QueryStatistics{
			BillingTier:                   3,
			CacheHit:                      true,
			StatementType:                 "SELECT",
			TotalBytesBilled:              2048,
			TotalBytesProcessed:           4096,
			TotalBytesProcessedAccuracy:   "PRECISE",
			QueryPlan:                     []*bigquery.ExplainQueryStage{{ID: 1, Name: "omitted"}},
			NumDMLAffectedRows:            6,
			DMLStats:                      &bigquery.DMLStatistics{InsertedRowCount: 1, DeletedRowCount: 2, UpdatedRowCount: 3},
			Timeline:                      []*bigquery.QueryTimelineSample{{Elapsed: time.Millisecond}},
			ReferencedTables:              []*bigquery.Table{{ProjectID: "project", DatasetID: "dataset", TableID: "table"}},
			Schema:                        bigquery.Schema{&bigquery.FieldSchema{Name: "omitted", Type: bigquery.StringFieldType}},
			SlotMillis:                    33,
			UndeclaredQueryParameterNames: []string{"parameter"},
			DDLOperationPerformed:         "CREATE_TABLE",
			ExportDataStatistics:          &bigquery.ExportDataStatistics{FileCount: 4, RowCount: 5},
		},
	}
}
