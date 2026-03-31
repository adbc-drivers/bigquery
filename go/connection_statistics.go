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
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/api/iterator"
)

// Custom BigQuery-specific statistic keys
const (
	// StatisticTotalLogicalBytesKey is the key for total logical bytes.
	// Total logical storage size from PARTITIONS aggregate (in bytes).
	StatisticTotalLogicalBytesKey = 1024
	// StatisticTotalLogicalBytesName is the name for total logical bytes statistic.
	StatisticTotalLogicalBytesName = "bigquery.statistic.total_logical_bytes"

	// StatisticTotalBillableBytesKey is the key for total billable bytes.
	// Billable storage size from PARTITIONS aggregate (in bytes).
	StatisticTotalBillableBytesKey = 1025
	// StatisticTotalBillableBytesName is the name for total billable bytes statistic.
	StatisticTotalBillableBytesName = "bigquery.statistic.total_billable_bytes"

	// StatisticTotalPhysicalBytesKey is the key for total physical bytes.
	// Physical storage including compression from TABLE_STORAGE (in bytes).
	StatisticTotalPhysicalBytesKey = 1026
	// StatisticTotalPhysicalBytesName is the name for total physical bytes statistic.
	StatisticTotalPhysicalBytesName = "bigquery.statistic.total_physical_bytes"

	// StatisticActiveLogicalBytesKey is the key for active logical bytes.
	// Recently modified data (<90 days) from TABLE_STORAGE (in bytes).
	StatisticActiveLogicalBytesKey = 1027
	// StatisticActiveLogicalBytesName is the name for active logical bytes statistic.
	StatisticActiveLogicalBytesName = "bigquery.statistic.active_logical_bytes"

	// StatisticLongTermLogicalBytesKey is the key for long-term logical bytes.
	// Data unmodified >90 days from TABLE_STORAGE (in bytes).
	StatisticLongTermLogicalBytesKey = 1028
	// StatisticLongTermLogicalBytesName is the name for long-term logical bytes statistic.
	StatisticLongTermLogicalBytesName = "bigquery.statistic.long_term_logical_bytes"

	// StatisticTimeTravelPhysicalBytesKey is the key for time travel physical bytes.
	// Time travel window storage from TABLE_STORAGE (in bytes).
	StatisticTimeTravelPhysicalBytesKey = 1029
	// StatisticTimeTravelPhysicalBytesName is the name for time travel physical bytes statistic.
	StatisticTimeTravelPhysicalBytesName = "bigquery.statistic.time_travel_physical_bytes"

	// StatisticPartitionCountKey is the key for partition count.
	// Number of partitions from PARTITIONS aggregate.
	StatisticPartitionCountKey = 1030
	// StatisticPartitionCountName is the name for partition count statistic.
	StatisticPartitionCountName = "bigquery.statistic.partition_count"

	// StatisticLastModifiedTimeKey is the key for last modified time.
	// Most recent data change from PARTITIONS aggregate (RFC3339 timestamp).
	StatisticLastModifiedTimeKey = 1031
	// StatisticLastModifiedTimeName is the name for last modified time statistic.
	StatisticLastModifiedTimeName = "bigquery.statistic.last_modified_time"
)

const (
	unionTypeInt64   arrow.UnionTypeCode = 0
	unionTypeUint64  arrow.UnionTypeCode = 1
	unionTypeFloat64 arrow.UnionTypeCode = 2
	unionTypeBinary  arrow.UnionTypeCode = 3
)

type bigqueryStatistic struct {
	tableName  string
	columnName *string
	key        int16
	valueKind  arrow.UnionTypeCode
	valueI64   int64
	valueU64   uint64
	valueF64   float64
	valueBin   []byte
	approx     bool
}

func newInt64Stat(table string, column *string, key int16, value int64, approx bool) bigqueryStatistic {
	return bigqueryStatistic{
		tableName:  table,
		columnName: column,
		key:        key,
		valueKind:  unionTypeInt64,
		valueI64:   value,
		approx:     approx,
	}
}

func newBinaryStat(table string, column *string, key int16, value []byte, approx bool) bigqueryStatistic {
	return bigqueryStatistic{
		tableName:  table,
		columnName: column,
		key:        key,
		valueKind:  unionTypeBinary,
		valueBin:   value,
		approx:     approx,
	}
}

func (c *connectionImpl) GetStatistics(ctx context.Context, catalog, dbSchema, tableName *string, approximate bool) (array.RecordReader, error) {
	catalogPattern, err := driverbase.PatternToRegexp(catalog)
	if err != nil {
		return nil, err
	}
	if catalogPattern == nil {
		catalogPattern = driverbase.AcceptAll
	}

	schemaPattern, err := driverbase.PatternToRegexp(dbSchema)
	if err != nil {
		return nil, err
	}
	if schemaPattern == nil {
		schemaPattern = driverbase.AcceptAll
	}

	tablePattern, err := driverbase.PatternToRegexp(tableName)
	if err != nil {
		return nil, err
	}
	if tablePattern == nil {
		tablePattern = driverbase.AcceptAll
	}

	project := c.client.Project()
	if !catalogPattern.MatchString(project) {
		// No matching catalog, return empty result
		return c.emptyGetStatisticsReader()
	}

	// Get datasets matching the schema pattern
	datasets, err := c.GetDBSchemasForCatalog(ctx, project, dbSchema)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err, "list datasets")
	}

	statsByDataset := map[string][]bigqueryStatistic{}
	var datasetOrder []string
	seenDataset := map[string]bool{}

	for _, dataset := range datasets {
		if !schemaPattern.MatchString(dataset) {
			continue
		}

		if !seenDataset[dataset] {
			seenDataset[dataset] = true
			datasetOrder = append(datasetOrder, dataset)
		}

		stats, err := c.getTableStatistics(ctx, project, dataset, tablePattern, tableName, approximate)
		if err != nil {
			return nil, err
		}

		if len(stats) > 0 {
			statsByDataset[dataset] = append(statsByDataset[dataset], stats...)
		}
	}

	return c.buildGetStatisticsReader(project, datasetOrder, statsByDataset)
}

// getTableStatistics retrieves statistics for tables in a dataset
func (c *connectionImpl) getTableStatistics(ctx context.Context, project, dataset string, tablePattern *regexp.Regexp, tableName *string, approximate bool) ([]bigqueryStatistic, error) {
	tableIt := c.client.DatasetInProject(project, dataset).Tables(ctx)
	var tableNames []string
	for {
		table, err := tableIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusInternal, err, "enumerate tables for statistics")
		}
		if tablePattern.MatchString(table.TableID) {
			tableNames = append(tableNames, table.TableID)
		}
	}

	if len(tableNames) == 0 {
		return nil, nil
	}

	// Query INFORMATION_SCHEMA in batches to avoid query size limits
	batchSize := 100
	var allStats []bigqueryStatistic

	for i := 0; i < len(tableNames); i += batchSize {
		end := min(i+batchSize, len(tableNames))
		batch := tableNames[i:end]

		stats, err := c.getTableStatisticsBatch(ctx, project, dataset, batch, approximate)
		if err != nil {
			return nil, err
		}
		allStats = append(allStats, stats...)
	}

	return allStats, nil
}

// getTableStatisticsBatch retrieves statistics for a specific batch of tables
func (c *connectionImpl) getTableStatisticsBatch(ctx context.Context, project, dataset string, tableNames []string, approximate bool) ([]bigqueryStatistic, error) {
	var inClause strings.Builder
	inClause.WriteString("(")
	for i, name := range tableNames {
		if i > 0 {
			inClause.WriteString(", ")
		}
		fmt.Fprintf(&inClause, "'%s'", strings.ReplaceAll(name, "'", "''"))
	}
	inClause.WriteString(")")

	// Query INFORMATION_SCHEMA.PARTITIONS for aggregated statistics
	// Use nested query to handle:
	// 1. NULL partition_id for unpartitioned tables (coalesce to __UNPARTITIONED__)
	// 2. Multiple rows per partition when data spans storage tiers (active vs long-term)
	//    Per BigQuery docs, PARTITIONS can return separate rows per storage tier:
	//    - total_rows: Represents the partition's total rows (same across tiers), use MAX to deduplicate
	//    - total_logical_bytes, total_billable_bytes: Tier-specific byte counts, use SUM to aggregate
	//    Inner query: GROUP BY partition to aggregate across storage tiers within each partition
	//    Outer query: SUM across all partitions to get table-level totals
	partitionsQuery := fmt.Sprintf(`
		SELECT
			table_name,
			COUNT(DISTINCT partition_id_coalesced) AS partition_count,
			SUM(partition_rows) AS total_rows,
			SUM(partition_logical_bytes) AS total_logical_bytes,
			SUM(partition_billable_bytes) AS total_billable_bytes,
			MAX(partition_last_modified) AS last_modified_time
		FROM (
			SELECT
				table_name,
				COALESCE(partition_id, '__UNPARTITIONED__') AS partition_id_coalesced,
				MAX(total_rows) AS partition_rows,
				SUM(total_logical_bytes) AS partition_logical_bytes,
				SUM(total_billable_bytes) AS partition_billable_bytes,
				MAX(last_modified_time) AS partition_last_modified
			FROM %s.%s.INFORMATION_SCHEMA.PARTITIONS
			WHERE table_name IN %s
			GROUP BY table_name, partition_id_coalesced
		)
		GROUP BY table_name
	`, quoteIdentifier(project), quoteIdentifier(dataset), inClause.String())

	partitionsJob := c.client.Query(partitionsQuery)
	partitionsJob.DefaultProjectID = project
	partitionsJob.DefaultDatasetID = dataset

	partitionsIt, err := partitionsJob.Read(ctx)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err, "query PARTITIONS")
	}

	// Store partition stats by table name
	type PartitionStats struct {
		PartitionCount     int64
		TotalRows          int64
		TotalLogicalBytes  int64
		TotalBillableBytes int64
		LastModifiedTime   time.Time
	}
	partitionStats := make(map[string]PartitionStats)

	for {
		var row struct {
			TableName          string    `bigquery:"table_name"`
			PartitionCount     int64     `bigquery:"partition_count"`
			TotalRows          int64     `bigquery:"total_rows"`
			TotalLogicalBytes  int64     `bigquery:"total_logical_bytes"`
			TotalBillableBytes int64     `bigquery:"total_billable_bytes"`
			LastModifiedTime   time.Time `bigquery:"last_modified_time"`
		}
		err := partitionsIt.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusInternal, err, "read PARTITIONS results")
		}

		// Validate table name
		if row.TableName == "" {
			continue
		}

		stats := PartitionStats{
			PartitionCount:     row.PartitionCount,
			TotalRows:          row.TotalRows,
			TotalLogicalBytes:  row.TotalLogicalBytes,
			TotalBillableBytes: row.TotalBillableBytes,
			LastModifiedTime:   row.LastModifiedTime,
		}
		partitionStats[row.TableName] = stats
	}

	// Query TABLE_STORAGE for physical storage statistics
	storageQuery := fmt.Sprintf(`
		SELECT
			table_name,
			total_logical_bytes,
			active_logical_bytes,
			long_term_logical_bytes,
			total_physical_bytes,
			active_physical_bytes,
			long_term_physical_bytes,
			time_travel_physical_bytes
		FROM %s.%s.INFORMATION_SCHEMA.TABLE_STORAGE
		WHERE table_name IN %s
	`, quoteIdentifier(project), quoteIdentifier(dataset), inClause.String())

	storageJob := c.client.Query(storageQuery)
	storageJob.DefaultProjectID = project
	storageJob.DefaultDatasetID = dataset

	storageIt, err := storageJob.Read(ctx)
	if err != nil {
		// TABLE_STORAGE may not be available in all regions/configurations
		// (requires specific BigQuery editions or project settings)
		c.Logger.Debug("TABLE_STORAGE query failed, skipping physical storage stats", "error", err)
		storageIt = nil
	}

	// Store storage stats by table name
	type StorageStats struct {
		TotalLogicalBytes       int64
		ActiveLogicalBytes      int64
		LongTermLogicalBytes    int64
		TotalPhysicalBytes      int64
		ActivePhysicalBytes     int64
		LongTermPhysicalBytes   int64
		TimeTravelPhysicalBytes int64
	}
	storageStats := make(map[string]StorageStats)

	if storageIt != nil {
		for {
			var row struct {
				TableName               string `bigquery:"table_name"`
				TotalLogicalBytes       int64  `bigquery:"total_logical_bytes"`
				ActiveLogicalBytes      int64  `bigquery:"active_logical_bytes"`
				LongTermLogicalBytes    int64  `bigquery:"long_term_logical_bytes"`
				TotalPhysicalBytes      int64  `bigquery:"total_physical_bytes"`
				ActivePhysicalBytes     int64  `bigquery:"active_physical_bytes"`
				LongTermPhysicalBytes   int64  `bigquery:"long_term_physical_bytes"`
				TimeTravelPhysicalBytes int64  `bigquery:"time_travel_physical_bytes"`
			}
			err := storageIt.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, errToAdbcErr(adbc.StatusInternal, err, "read TABLE_STORAGE results")
			}

			// Validate table name
			if row.TableName == "" {
				continue
			}

			stats := StorageStats{
				TotalLogicalBytes:       row.TotalLogicalBytes,
				ActiveLogicalBytes:      row.ActiveLogicalBytes,
				LongTermLogicalBytes:    row.LongTermLogicalBytes,
				TotalPhysicalBytes:      row.TotalPhysicalBytes,
				ActivePhysicalBytes:     row.ActivePhysicalBytes,
				LongTermPhysicalBytes:   row.LongTermPhysicalBytes,
				TimeTravelPhysicalBytes: row.TimeTravelPhysicalBytes,
			}
			storageStats[row.TableName] = stats
		}
	}

	// Combine statistics for each table
	var allStats []bigqueryStatistic
	for tableName, pstats := range partitionStats {
		allStats = append(allStats,
			newInt64Stat(tableName, nil, int16(adbc.StatisticRowCountKey), pstats.TotalRows, approximate),
			newInt64Stat(tableName, nil, StatisticTotalLogicalBytesKey, pstats.TotalLogicalBytes, approximate),
			newInt64Stat(tableName, nil, StatisticTotalBillableBytesKey, pstats.TotalBillableBytes, approximate),
			newInt64Stat(tableName, nil, StatisticPartitionCountKey, pstats.PartitionCount, false),
			newBinaryStat(tableName, nil, StatisticLastModifiedTimeKey, []byte(pstats.LastModifiedTime.Format(time.RFC3339Nano)), false),
		)

		// Add storage statistics if available
		// Note: TABLE_STORAGE data is not real-time and is typically delayed by
		// seconds to minutes, so these statistics are always marked as approximate
		// regardless of the caller's preference.
		if sstats, ok := storageStats[tableName]; ok {
			allStats = append(allStats,
				newInt64Stat(tableName, nil, StatisticTotalPhysicalBytesKey, sstats.TotalPhysicalBytes, true),
				newInt64Stat(tableName, nil, StatisticActiveLogicalBytesKey, sstats.ActiveLogicalBytes, true),
				newInt64Stat(tableName, nil, StatisticLongTermLogicalBytesKey, sstats.LongTermLogicalBytes, true),
				newInt64Stat(tableName, nil, StatisticTimeTravelPhysicalBytesKey, sstats.TimeTravelPhysicalBytes, true),
			)
		}
	}

	return allStats, nil
}

func (c *connectionImpl) GetStatisticNames(ctx context.Context) (array.RecordReader, error) {
	statistics := []struct {
		Name string
		Key  int16
	}{
		// BigQuery-specific statistics only (keys >= 1024)
		{StatisticTotalLogicalBytesName, StatisticTotalLogicalBytesKey},
		{StatisticTotalBillableBytesName, StatisticTotalBillableBytesKey},
		{StatisticTotalPhysicalBytesName, StatisticTotalPhysicalBytesKey},
		{StatisticActiveLogicalBytesName, StatisticActiveLogicalBytesKey},
		{StatisticLongTermLogicalBytesName, StatisticLongTermLogicalBytesKey},
		{StatisticTimeTravelPhysicalBytesName, StatisticTimeTravelPhysicalBytesKey},
		{StatisticPartitionCountName, StatisticPartitionCountKey},
		{StatisticLastModifiedTimeName, StatisticLastModifiedTimeKey},
	}

	bldr := array.NewRecordBuilder(c.Alloc, adbc.GetStatisticNamesSchema)
	defer bldr.Release()

	nameBldr := bldr.Field(0).(*array.StringBuilder)
	keyBldr := bldr.Field(1).(*array.Int16Builder)

	for _, stat := range statistics {
		nameBldr.Append(stat.Name)
		keyBldr.Append(stat.Key)
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetStatisticNamesSchema, []arrow.RecordBatch{rec})
}

// emptyGetStatisticsReader returns an empty GetStatistics result.
func (c *connectionImpl) emptyGetStatisticsReader() (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(c.Alloc, adbc.GetStatisticsSchema)
	defer bldr.Release()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetStatisticsSchema, []arrow.RecordBatch{rec})
}

// buildGetStatisticsReader constructs the Arrow RecordReader for GetStatistics.
func (c *connectionImpl) buildGetStatisticsReader(
	project string,
	datasetOrder []string,
	statsByDataset map[string][]bigqueryStatistic,
) (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(c.Alloc, adbc.GetStatisticsSchema)
	defer bldr.Release()

	catalogNameBldr := bldr.Field(0).(*array.StringBuilder)
	catalogSchemasBldr := bldr.Field(1).(*array.ListBuilder)
	dbSchemaStructBldr := catalogSchemasBldr.ValueBuilder().(*array.StructBuilder)
	dbSchemaNameBldr := dbSchemaStructBldr.FieldBuilder(0).(*array.StringBuilder)
	dbSchemaStatsListBldr := dbSchemaStructBldr.FieldBuilder(1).(*array.ListBuilder)

	statsStructBldr := dbSchemaStatsListBldr.ValueBuilder().(*array.StructBuilder)
	tableNameBldr := statsStructBldr.FieldBuilder(0).(*array.StringBuilder)
	columnNameBldr := statsStructBldr.FieldBuilder(1).(*array.StringBuilder)
	statKeyBldr := statsStructBldr.FieldBuilder(2).(*array.Int16Builder)
	statValueBldr := statsStructBldr.FieldBuilder(3).(*array.DenseUnionBuilder)
	statApproxBldr := statsStructBldr.FieldBuilder(4).(*array.BooleanBuilder)

	statI64Bldr := statValueBldr.Child(0).(*array.Int64Builder)
	statU64Bldr := statValueBldr.Child(1).(*array.Uint64Builder)
	statF64Bldr := statValueBldr.Child(2).(*array.Float64Builder)
	statBinBldr := statValueBldr.Child(3).(*array.BinaryBuilder)

	catalogNameBldr.Append(project)
	catalogSchemasBldr.Append(true)

	for _, dataset := range datasetOrder {
		dbSchemaStructBldr.Append(true)
		dbSchemaNameBldr.Append(dataset)
		dbSchemaStatsListBldr.Append(true)

		for _, st := range statsByDataset[dataset] {
			statsStructBldr.Append(true)
			tableNameBldr.Append(st.tableName)

			if st.columnName == nil {
				columnNameBldr.AppendNull()
			} else {
				columnNameBldr.Append(*st.columnName)
			}

			statKeyBldr.Append(st.key)
			statApproxBldr.Append(st.approx)

			statValueBldr.Append(st.valueKind)
			switch st.valueKind {
			case unionTypeInt64:
				statI64Bldr.Append(st.valueI64)
			case unionTypeUint64:
				statU64Bldr.Append(st.valueU64)
			case unionTypeFloat64:
				statF64Bldr.Append(st.valueF64)
			case unionTypeBinary:
				statBinBldr.Append(st.valueBin)
			default:
				return nil, fmt.Errorf("unknown statistic value kind: %d", st.valueKind)
			}
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetStatisticsSchema, []arrow.RecordBatch{rec})
}
