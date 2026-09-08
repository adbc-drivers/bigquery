// Copyright (c) 2026 ADBC Drivers Contributors
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
	"encoding/json"
	"reflect"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
)

func metadataFromQueryStatistics(metadata map[string]string, queryStatistics *bigquery.QueryStatistics) error {
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:query:bi_engine_statistics", "BIGQUERY:Statistics:Query:BIEngineStatistics", queryStatistics.BIEngineStatistics); err != nil {
		return err
	}
	addIntMetadata(metadata, "BIGQUERY:statistics:query:billing_tier", "BIGQUERY:Statistics:Query:BillingTier", queryStatistics.BillingTier)
	addBoolMetadata(metadata, "BIGQUERY:statistics:query:cache_hit", "BIGQUERY:Statistics:Query:CacheHit", queryStatistics.CacheHit)
	addStringMetadata(metadata, "BIGQUERY:statistics:query:statement_type", "BIGQUERY:Statistics:Query:StatementType", queryStatistics.StatementType)
	addIntMetadata(metadata, "BIGQUERY:statistics:query:total_bytes_billed", "BIGQUERY:Statistics:Query:TotalBytesBilled", queryStatistics.TotalBytesBilled)
	addIntMetadata(metadata, "BIGQUERY:statistics:query:total_bytes_processed", "BIGQUERY:Statistics:Query:TotalBytesProcessed", queryStatistics.TotalBytesProcessed)
	addStringMetadata(metadata, "BIGQUERY:statistics:query:total_bytes_processed_accuracy", "BIGQUERY:Statistics:Query:TotalBytesProcessedAccuracy", queryStatistics.TotalBytesProcessedAccuracy)
	addIntMetadata(metadata, "BIGQUERY:statistics:query:num_dml_affected_rows", "BIGQUERY:Statistics:Query:NumDMLAffectedRows", queryStatistics.NumDMLAffectedRows)
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:query:dml_stats", "BIGQUERY:Statistics:Query:DMLStats", queryStatistics.DMLStats); err != nil {
		return err
	}
	addIntMetadata(metadata, "BIGQUERY:statistics:query:slot_millis", "BIGQUERY:Statistics:Query:SlotMillis", queryStatistics.SlotMillis)
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:query:undeclared_query_parameter_names", "BIGQUERY:Statistics:Query:UndeclaredQueryParameterNames", queryStatistics.UndeclaredQueryParameterNames); err != nil {
		return err
	}
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:query:ddl_target_table", "BIGQUERY:Statistics:Query:DDLTargetTable", queryStatistics.DDLTargetTable); err != nil {
		return err
	}
	addStringMetadata(metadata, "BIGQUERY:statistics:query:ddl_operation_performed", "BIGQUERY:Statistics:Query:DDLOperationPerformed", queryStatistics.DDLOperationPerformed)
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:query:ddl_target_routine", "BIGQUERY:Statistics:Query:DDLTargetRoutine", queryStatistics.DDLTargetRoutine); err != nil {
		return err
	}
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:query:export_data_statistics", "BIGQUERY:Statistics:Query:ExportDataStatistics", queryStatistics.ExportDataStatistics); err != nil {
		return err
	}
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:query:performance_insights", "BIGQUERY:Statistics:Query:PerformanceInsights", queryStatistics.PerformanceInsights); err != nil {
		return err
	}
	return nil
}

func metadataFromJobStatistics(stats *bigquery.JobStatistics, jobID string) (*arrow.Metadata, error) {
	if stats == nil && jobID == "" {
		return nil, nil
	}

	metadata := make(map[string]string)
	addStringMetadata(metadata, MetadataKeyBigqueryQueryID, "", jobID)
	if stats == nil {
		return new(arrow.MetadataFrom(metadata)), nil
	}
	addTimeMetadata(metadata, "BIGQUERY:statistics:creation_time", "BIGQUERY:Statistics:CreationTime", stats.CreationTime)
	addTimeMetadata(metadata, "BIGQUERY:statistics:start_time", "BIGQUERY:Statistics:StartTime", stats.StartTime)
	addTimeMetadata(metadata, "BIGQUERY:statistics:end_time", "BIGQUERY:Statistics:EndTime", stats.EndTime)
	addIntMetadata(metadata, "BIGQUERY:statistics:total_bytes_processed", "BIGQUERY:Statistics:TotalBytesProcessed", stats.TotalBytesProcessed)
	addDurationMetadata(metadata, "BIGQUERY:statistics:total_slot_duration", "BIGQUERY:Statistics:TotalSlotDuration", stats.TotalSlotDuration)
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:reservation_usage", "BIGQUERY:Statistics:ReservationUsage", stats.ReservationUsage); err != nil {
		return nil, err
	}
	addStringMetadata(metadata, "BIGQUERY:statistics:reservation_id", "BIGQUERY:Statistics:ReservationID", stats.ReservationID)
	addIntMetadata(metadata, "BIGQUERY:statistics:num_child_jobs", "BIGQUERY:Statistics:NumChildJobs", stats.NumChildJobs)
	addStringMetadata(metadata, "BIGQUERY:statistics:parent_job_id", "BIGQUERY:Statistics:ParentJobID", stats.ParentJobID)
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:script_statistics", "BIGQUERY:Statistics:ScriptStatistics", stats.ScriptStatistics); err != nil {
		return nil, err
	}
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:transaction_info", "BIGQUERY:Statistics:TransactionInfo", stats.TransactionInfo); err != nil {
		return nil, err
	}
	if err := addJSONMetadata(metadata, "BIGQUERY:statistics:session_info", "BIGQUERY:Statistics:SessionInfo", stats.SessionInfo); err != nil {
		return nil, err
	}
	addDurationMetadata(metadata, "BIGQUERY:statistics:final_execution_duration", "BIGQUERY:Statistics:FinalExecutionDuration", stats.FinalExecutionDuration)
	addStringMetadata(metadata, "BIGQUERY:statistics:edition", "BIGQUERY:Statistics:Edition", string(stats.Edition))
	addFloatMetadata(metadata, "BIGQUERY:statistics:completion_ratio", "BIGQUERY:Statistics:CompletionRatio", stats.CompletionRatio)

	queryStatistics, ok := stats.Details.(*bigquery.QueryStatistics)
	if ok && queryStatistics != nil {
		if err := metadataFromQueryStatistics(metadata, queryStatistics); err != nil {
			return nil, err
		}
	}
	return new(arrow.MetadataFrom(metadata)), nil
}

func addTimeMetadata(metadata map[string]string, key, legacyKey string, value time.Time) {
	if value.IsZero() {
		return
	}
	addStringMetadata(metadata, key, legacyKey, value.Format(time.RFC3339Nano))
}

func addDurationMetadata(metadata map[string]string, key, legacyKey string, value time.Duration) {
	addStringMetadata(metadata, key, legacyKey, strconv.FormatInt(int64(value), 10))
}

func addIntMetadata(metadata map[string]string, key, legacyKey string, value int64) {
	addStringMetadata(metadata, key, legacyKey, strconv.FormatInt(value, 10))
}

func addFloatMetadata(metadata map[string]string, key, legacyKey string, value float64) {
	addStringMetadata(metadata, key, legacyKey, strconv.FormatFloat(value, 'g', -1, 64))
}

func addBoolMetadata(metadata map[string]string, key, legacyKey string, value bool) {
	addStringMetadata(metadata, key, legacyKey, strconv.FormatBool(value))
}

func addStringMetadata(metadata map[string]string, key, legacyKey string, value string) {
	if value == "" {
		return
	}
	metadata[key] = value
	if legacyKey != "" {
		metadata[legacyKey] = value
	}
}

func addJSONMetadata(metadata map[string]string, key, legacyKey string, value any) error {
	if isNilOrEmpty(value) {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	addStringMetadata(metadata, key, legacyKey, string(encoded))
	return nil
}

func isNilOrEmpty(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil() || (reflected.Kind() == reflect.Slice && reflected.Len() == 0)
	case reflect.Array:
		return reflected.Len() == 0
	default:
		return false
	}
}
