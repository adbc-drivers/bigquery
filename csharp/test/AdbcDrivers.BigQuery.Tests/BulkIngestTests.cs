/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Arrays;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Tests for bulk ingestion using the BigQuery Storage Write API.
    /// Requires a live BigQuery connection configured via BIGQUERY_TEST_CONFIG_FILE.
    /// Each test creates and cleans up its own table using catalog/dataset from the config.
    /// </summary>
    public class BulkIngestTests : IDisposable
    {
        private readonly BigQueryTestConfiguration _testConfiguration;
        private readonly List<BigQueryTestEnvironment> _environments;
        private readonly ITestOutputHelper? _outputHelper;
        private readonly List<(BigQueryTestEnvironment environment, string catalog, string dataset, string table)> _tablesToCleanup = new();

        public BulkIngestTests(ITestOutputHelper? outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            _testConfiguration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(_testConfiguration);
            _outputHelper = outputHelper;
        }

        public void Dispose()
        {
            foreach (var (environment, catalog, dataset, table) in _tablesToCleanup)
            {
                try
                {
                    using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                    using AdbcStatement stmt = connection.CreateStatement();
                    stmt.SqlQuery = $"DROP TABLE IF EXISTS `{catalog}.{dataset}.{table}`";
                    stmt.ExecuteUpdate();
                    _outputHelper?.WriteLine($"Cleaned up table {catalog}.{dataset}.{table}");
                }
                catch (Exception ex)
                {
                    _outputHelper?.WriteLine($"Warning: failed to clean up table {catalog}.{dataset}.{table}: {ex.Message}");
                }
            }
        }

        private string UniqueTableName(string testName)
        {
            return $"adbc_test_{testName}_{DateTime.UtcNow:yyyyMMdd_HHmmss}_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
        }

        private void EnsureTableDropped(AdbcConnection connection, string catalog, string dataset, string tableName)
        {
            using AdbcStatement stmt = connection.CreateStatement();
            stmt.SqlQuery = $"DROP TABLE IF EXISTS `{catalog}.{dataset}.{tableName}`";
            stmt.ExecuteUpdate();
        }

        private void RegisterCleanup(BigQueryTestEnvironment environment, string catalog, string dataset, string tableName)
        {
            _tablesToCleanup.Add((environment, catalog, dataset, tableName));
        }

        private static RecordBatch CreateSimpleBatch(int rowCount = 3)
        {
            var schema = new Schema(new[]
            {
                new Field("id", Int64Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: true),
                new Field("value", DoubleType.Default, nullable: true),
            }, null);

            var idBuilder = new Int64Array.Builder();
            var nameBuilder = new StringArray.Builder();
            var valueBuilder = new DoubleArray.Builder();

            for (int i = 0; i < rowCount; i++)
            {
                idBuilder.Append(i + 1);
                nameBuilder.Append($"row_{i + 1}");
                valueBuilder.Append((i + 1) * 1.5);
            }

            return new RecordBatch(schema, new IArrowArray[]
            {
                idBuilder.Build(),
                nameBuilder.Build(),
                valueBuilder.Build(),
            }, rowCount);
        }

        private long CountRows(AdbcConnection connection, string catalog, string dataset, string tableName)
        {
            using AdbcStatement stmt = connection.CreateStatement();
            stmt.SqlQuery = $"SELECT COUNT(*) AS cnt FROM `{catalog}.{dataset}.{tableName}`";
            QueryResult result = stmt.ExecuteQuery();
            using (result.Stream!)
            {
                RecordBatch? batch = result.Stream!.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
                if (batch != null)
                {
                    var countCol = (Int64Array)batch.Column(0);
                    return countCol.GetValue(0)!.Value;
                }
            }
            return -1;
        }

        [SkippableTheory]
        [InlineData("protobuf")]
        [InlineData("arrow")]
        public void CanBulkIngestCreateMode(string format)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("create");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);

                RecordBatch batch = CreateSimpleBatch(3);

                using AdbcStatement statement = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false);
                statement.SetOption(BigQueryParameters.IngestFormat, format);
                statement.Bind(batch, batch.Schema);
                UpdateResult result = statement.ExecuteUpdate();

                Assert.Equal(3, result.AffectedRows);

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(3, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestCreateMode: inserted {result.AffectedRows} rows into {catalog}.{dataset}.{tableName}");
            }
        }

        [SkippableTheory]
        [InlineData("protobuf")]
        [InlineData("arrow")]
        public void CanBulkIngestAppendMode(string format)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("append");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);

                // First, create the table with initial data
                RecordBatch batch1 = CreateSimpleBatch(2);
                using (AdbcStatement stmt1 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false))
                {
                    stmt1.SetOption(BigQueryParameters.IngestFormat, format);
                    stmt1.Bind(batch1, batch1.Schema);
                    stmt1.ExecuteUpdate();
                }

                // Then append more data
                RecordBatch batch2 = CreateSimpleBatch(3);
                using (AdbcStatement stmt2 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Append, false))
                {
                    stmt2.SetOption(BigQueryParameters.IngestFormat, format);
                    stmt2.Bind(batch2, batch2.Schema);
                    UpdateResult result = stmt2.ExecuteUpdate();
                    Assert.Equal(3, result.AffectedRows);
                }

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(5, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestAppendMode: total {rowCount} rows in {catalog}.{dataset}.{tableName}");
            }
        }

        [SkippableTheory]
        [InlineData("protobuf")]
        [InlineData("arrow")]
        public void CanBulkIngestCreateAppendMode(string format)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("createappend");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);

                // CreateAppend should create the table since it doesn't exist
                RecordBatch batch1 = CreateSimpleBatch(2);
                using (AdbcStatement stmt1 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.CreateAppend, false))
                {
                    stmt1.SetOption(BigQueryParameters.IngestFormat, format);
                    stmt1.Bind(batch1, batch1.Schema);
                    stmt1.ExecuteUpdate();
                }

                // CreateAppend again should append since table now exists
                RecordBatch batch2 = CreateSimpleBatch(3);
                using (AdbcStatement stmt2 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.CreateAppend, false))
                {
                    stmt2.SetOption(BigQueryParameters.IngestFormat, format);
                    stmt2.Bind(batch2, batch2.Schema);
                    stmt2.ExecuteUpdate();
                }

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(5, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestCreateAppendMode: total {rowCount} rows in {catalog}.{dataset}.{tableName}");
            }
        }

        [SkippableTheory]
        [InlineData("protobuf")]
        [InlineData("arrow")]
        public void BulkIngestCreateFailsIfTableExists(string format)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("createfail");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);

                // Create the table first
                RecordBatch batch = CreateSimpleBatch(1);
                using (AdbcStatement stmt1 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false))
                {
                    stmt1.SetOption(BigQueryParameters.IngestFormat, format);
                    stmt1.Bind(batch, batch.Schema);
                    stmt1.ExecuteUpdate();
                }

                // Create again should fail
                using AdbcStatement stmt2 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false);
                stmt2.SetOption(BigQueryParameters.IngestFormat, format);
                stmt2.Bind(batch, batch.Schema);
                AdbcException ex = Assert.Throws<AdbcException>(() => stmt2.ExecuteUpdate());
                Assert.Equal(AdbcStatusCode.AlreadyExists, ex.Status);

                _outputHelper?.WriteLine($"BulkIngestCreateFailsIfTableExists: correctly threw {ex.Status}");
            }
        }

        [SkippableTheory]
        [InlineData("protobuf")]
        [InlineData("arrow")]
        public void BulkIngestAppendFailsIfTableMissing(string format)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("appendfail");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);

                RecordBatch batch = CreateSimpleBatch(1);
                using AdbcStatement stmt = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Append, false);
                stmt.SetOption(BigQueryParameters.IngestFormat, format);
                stmt.Bind(batch, batch.Schema);
                AdbcException ex = Assert.Throws<AdbcException>(() => stmt.ExecuteUpdate());
                Assert.Equal(AdbcStatusCode.NotFound, ex.Status);

                _outputHelper?.WriteLine($"BulkIngestAppendFailsIfTableMissing: correctly threw {ex.Status}");
            }
        }

        [SkippableTheory]
        [InlineData("protobuf")]
        [InlineData("arrow")]
        public void CanBulkIngestReplaceMode(string format)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("replace");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);

                // Create with 5 rows
                RecordBatch batch1 = CreateSimpleBatch(5);
                using (AdbcStatement stmt1 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false))
                {
                    stmt1.SetOption(BigQueryParameters.IngestFormat, format);
                    stmt1.Bind(batch1, batch1.Schema);
                    stmt1.ExecuteUpdate();
                }

                // Replace with 2 rows
                RecordBatch batch2 = CreateSimpleBatch(2);
                using (AdbcStatement stmt2 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Replace, false))
                {
                    stmt2.SetOption(BigQueryParameters.IngestFormat, format);
                    stmt2.Bind(batch2, batch2.Schema);
                    UpdateResult result = stmt2.ExecuteUpdate();
                    Assert.Equal(2, result.AffectedRows);
                }

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(2, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestReplaceMode: replaced with {rowCount} rows in {catalog}.{dataset}.{tableName}");
            }
        }

        [SkippableTheory]
        [InlineData("protobuf")]
        [InlineData("arrow")]
        public void CanBulkIngestWithBindStream(string format)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("bindstream");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);

                RecordBatch batch1 = CreateSimpleBatch(2);
                RecordBatch batch2 = CreateSimpleBatch(3);

                // Use BindStream with multiple batches
                var stream = new TestArrowArrayStream(batch1.Schema, new[] { batch1, batch2 });

                using AdbcStatement stmt = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false);
                stmt.SetOption(BigQueryParameters.IngestFormat, format);
                stmt.BindStream(stream);
                UpdateResult result = stmt.ExecuteUpdate();

                Assert.Equal(5, result.AffectedRows);

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(5, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestWithBindStream: inserted {result.AffectedRows} rows via stream");
            }
        }

        [SkippableFact]
        public void BulkIngestRejectsTemporaryTable()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);

                Assert.Throws<AdbcException>(() =>
                    connection.BulkIngest(
                        environment.Metadata.Catalog,
                        environment.Metadata.Schema,
                        "temp_table",
                        BulkIngestMode.Create,
                        isTemporary: true));
            }
        }

        [SkippableTheory]
        [InlineData("protobuf")]
        [InlineData("arrow")]
        public void CanBulkIngestTemporalTypes(string format)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("temporal");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);

                // Create a batch with temporal types that require normalization.
                // Time32 (second) and Timestamp (nanosecond) both need conversion
                // to microsecond precision for BigQuery.
                var time32Builder = new Time32Array.Builder(TimeUnit.Second);
                time32Builder.Append(3661); // 1h 1m 1s
                time32Builder.Append(7200); // 2h
                time32Builder.AppendNull();

                var timestampBuilder = new TimestampArray.Builder(new TimestampType(TimeUnit.Nanosecond, "UTC"));
                timestampBuilder.Append(new DateTimeOffset(2024, 6, 15, 12, 30, 45, TimeSpan.Zero));
                timestampBuilder.Append(new DateTimeOffset(2024, 12, 31, 23, 59, 59, TimeSpan.Zero));
                timestampBuilder.AppendNull();

                var idBuilder = new Int64Array.Builder();
                idBuilder.Append(1);
                idBuilder.Append(2);
                idBuilder.Append(3);

                var schema = new Schema(new[]
                {
                    new Field("id", Int64Type.Default, nullable: false),
                    new Field("time_val", new Time32Type(TimeUnit.Second), nullable: true),
                    new Field("ts_val", new TimestampType(TimeUnit.Nanosecond, "UTC"), nullable: true),
                }, null);

                var batch = new RecordBatch(schema, new IArrowArray[]
                {
                    idBuilder.Build(),
                    time32Builder.Build(),
                    timestampBuilder.Build(),
                }, 3);

                using AdbcStatement statement = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false);
                statement.SetOption(BigQueryParameters.IngestFormat, format);
                statement.Bind(batch, batch.Schema);
                UpdateResult result = statement.ExecuteUpdate();

                Assert.Equal(3, result.AffectedRows);

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(3, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestTemporalTypes: inserted {result.AffectedRows} rows with temporal types into {catalog}.{dataset}.{tableName}");
            }
        }

        [SkippableTheory]
        [InlineData("protobuf")]
        [InlineData("arrow")]
        public void CanRoundTripAllSupportedTypes(string format)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string table = UniqueTableName("types_" + format);
                RegisterCleanup(environment, catalog, dataset, table);
                var cases = CreateTypeCases(includeProtobufOnlyTypes: format == "protobuf");
                var fields = new[] { new Field("id", Int64Type.Default, false) }
                    .Concat(cases.Select(c => new Field(c.name, c.array.Data.DataType, true)));
                var schema = new Schema(fields, null);
                using var original = new RecordBatch(schema,
                    new IArrowArray[] { new Int64Array.Builder().Append(0).Append(1).Append(2).Append(3).Build() }
                        .Concat(cases.Select(c => c.array)), 4);
                using RecordBatch batch = original.Slice(1, 3);
                using (AdbcStatement statement = connection.BulkIngest(catalog, dataset, table, BulkIngestMode.Create, false))
                {
                    statement.SetOption(BigQueryParameters.IngestFormat, format);
                    statement.Bind(batch, batch.Schema);
                    Assert.Equal(3, statement.ExecuteUpdate().AffectedRows);
                }

                using AdbcStatement query = connection.CreateStatement();
                query.SqlQuery = $"SELECT {string.Join(", ", cases.Select(c => c.projection))} FROM `{catalog}.{dataset}.{table}` ORDER BY id";
                using IArrowArrayStream stream = query.ExecuteQuery().Stream!;
                int row = 0;
                RecordBatch? result;
                while ((result = stream.ReadNextRecordBatchAsync().GetAwaiter().GetResult()) != null)
                {
                    using (result)
                    {
                        for (int i = 0; i < result.Length; i++, row++)
                        {
                            Assert.InRange(row, 0, 2);
                            for (int column = 0; column < cases.Count; column++)
                            {
                                string? actual = ((StringArray)result.Column(column)).GetString(i);
                                Assert.True(cases[column].expected[row] == actual,
                                    $"{format} {cases[column].name}, row {row}: expected '{cases[column].expected[row] ?? "NULL"}', got '{actual ?? "NULL"}'");
                            }
                        }
                    }
                }
                Assert.Equal(3, row);
            }
        }

        private static List<(string name, IArrowArray array, string projection, string?[] expected)> CreateTypeCases(bool includeProtobufOnlyTypes)
        {
            var cases = new List<(string name, IArrowArray array, string projection, string?[] expected)>();
            void Add(string name, IArrowArray array, string value, string zero, string? projection = null, string? nullValue = null)
                => cases.Add((name, array, (projection ?? $"CAST(`{name}` AS STRING)") + $" AS `{name}`",
                    new[] { value, zero, nullValue }));

            Add("i8", new Int8Array.Builder().Append(99).Append(sbyte.MinValue).Append(0).AppendNull().Build(), "-128", "0");
            Add("i16", new Int16Array.Builder().Append(99).Append(short.MinValue).Append(0).AppendNull().Build(), "-32768", "0");
            Add("i32", new Int32Array.Builder().Append(99).Append(int.MinValue).Append(0).AppendNull().Build(), "-2147483648", "0");
            Add("i64", new Int64Array.Builder().Append(99).Append(long.MinValue).Append(0).AppendNull().Build(), "-9223372036854775808", "0");
            // These Arrow representations are not supported by BigQuery's Arrow write path.
            if (includeProtobufOnlyTypes)
            {
                Add("u8", new UInt8Array.Builder().Append(99).Append(byte.MaxValue).Append(0).AppendNull().Build(), "255", "0");
                Add("u16", new UInt16Array.Builder().Append(99).Append(ushort.MaxValue).Append(0).AppendNull().Build(), "65535", "0");
                Add("u32", new UInt32Array.Builder().Append(99).Append(uint.MaxValue).Append(0).AppendNull().Build(), "4294967295", "0");
                Add("u64", new UInt64Array.Builder().Append(99).Append(long.MaxValue).Append(0).AppendNull().Build(), "9223372036854775807", "0");
                Add("fixed_binary", new FixedSizeBinaryArray(new ArrayData(new FixedSizeBinaryType(2), 4, 1, 0, new[]
                    {
                        new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Append(true).Append(false).Build(),
                        new ArrowBuffer.Builder<byte>().Append(new byte[] { 99, 99, 0, 255, 0, 0, 0, 0 }).Build(),
                    })),
                    "00ff", "0000", "LOWER(TO_HEX(fixed_binary))");
                Add("date64", new Date64Array.Builder().Append(new DateTime(2000, 1, 1)).Append(new DateTime(1969, 12, 31)).Append(new DateTime(1970, 1, 1)).AppendNull().Build(), "1969-12-31", "1970-01-01");
                var nullLists = new ListArray.Builder(Int64Type.Default);
                nullLists.AppendNull().AppendNull().AppendNull().AppendNull();
                Add("null_lists", nullLists.Build(), "[]", "[]", "TO_JSON_STRING(null_lists)", "[]");
            }
            Add("boolean", new BooleanArray.Builder().Append(false).Append(true).Append(false).AppendNull().Build(), "true", "false");
            Add("single", new FloatArray.Builder().Append(99).Append(-1.5f).Append(0).AppendNull().Build(), "-1.5", "0");
            Add("double", new DoubleArray.Builder().Append(99).Append(-1.25).Append(0).AppendNull().Build(), "-1.25", "0");
            Add("text", new StringArray.Builder().Append("excluded").Append("caf\u00e9 \u65e5\u672c").Append("").AppendNull().Build(), "caf\u00e9 \u65e5\u672c", "");
            Add("binary", new BinaryArray.Builder().Append(new byte[] { 99 }).Append(new byte[] { 0, 255 }).Append(System.Array.Empty<byte>()).AppendNull().Build(),
                "00ff", "", "LOWER(TO_HEX(binary))");
            const string numeric = "-99999999999999999999999999999.999999999";
            const string bigNumeric = "-99999999999999999999999999999999999999.99999999999999999999999999999999999999";
            Add("numeric", new Decimal128Array.Builder(new Decimal128Type(38, 9)).Append("99").Append(numeric).Append("0").AppendNull().Build(), numeric, "0");
            Add("big_numeric", new Decimal256Array.Builder(new Decimal256Type(76, 38)).Append("99").Append(bigNumeric).Append("0").AppendNull().Build(), bigNumeric, "0");
            Add("date32", new Date32Array.Builder().Append(new DateTime(2000, 1, 1)).Append(new DateTime(1969, 12, 31)).Append(new DateTime(1970, 1, 1)).AppendNull().Build(), "1969-12-31", "1970-01-01");
            foreach (TimeUnit unit in new[] { TimeUnit.Second, TimeUnit.Millisecond })
            {
                int value = unit == TimeUnit.Second ? 3661 : 3661123;
                Add("time32_" + unit, new Time32Array.Builder(unit).Append(99).Append(value).Append(0).AppendNull().Build(),
                    unit == TimeUnit.Second ? "01:01:01" : "01:01:01.123", "00:00:00");
            }
            foreach (TimeUnit unit in new[] { TimeUnit.Microsecond, TimeUnit.Nanosecond })
            {
                long value = unit == TimeUnit.Microsecond ? 3661123456L : 3661123456789L;
                Add("time64_" + unit, new Time64Array.Builder(unit).Append(99).Append(value).Append(0).AppendNull().Build(),
                    "01:01:01.123456", "00:00:00");
            }
            foreach (TimeUnit unit in new[] { TimeUnit.Second, TimeUnit.Millisecond, TimeUnit.Microsecond, TimeUnit.Nanosecond })
            {
                foreach (string? timezone in new string?[] { null, "UTC" })
                {
                    var type = new TimestampType(unit, timezone);
                    var timestamp = new DateTimeOffset(1969, 12, 31, 23, 59, 59, TimeSpan.Zero);
                    string suffix = "";
                    if (unit != TimeUnit.Second)
                    {
                        timestamp = timestamp.AddTicks(unit == TimeUnit.Millisecond ? 1230000 : 1234560);
                        suffix = unit == TimeUnit.Millisecond ? ".123" : ".123456";
                    }
                    Add((timezone == null ? "datetime_" : "timestamp_") + unit,
                        new TimestampArray.Builder(type).Append(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero))
                            .Append(timestamp).Append(new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero)).AppendNull().Build(),
                        "1969-12-31 23:59:59" + suffix + (timezone == null ? "" : "+00"),
                        "1970-01-01 00:00:00" + (timezone == null ? "" : "+00"));
                }
            }

            var list = new ListArray.Builder(Int64Type.Default);
            var values = (Int64Array.Builder)list.ValueBuilder;
            list.Append(); values.Append(99);
            list.Append(); values.Append(7).Append(-2);
            list.Append();
            list.Append();
            Add("numbers", list.Build(), "[7,-2]", "[]", "TO_JSON_STRING(numbers)", "[]");

            var recordType = new StructType(new[]
            {
                new Field("name", StringType.Default, true),
                new Field("time", new Time64Type(TimeUnit.Microsecond), true),
            });
            var record = new StructArray(recordType, 4, new IArrowArray[]
            {
                new StringArray.Builder().Append("excluded").Append("nested").Append("").AppendNull().Build(),
                new Time64Array.Builder(TimeUnit.Microsecond).Append(0).Append(3661123456L).Append(0).AppendNull().Build(),
            }, new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Append(true).Append(false).Build(), 1);
            Add("record", record, "{\"name\":\"nested\",\"time\":\"01:01:01.123456\"}", "{\"name\":\"\",\"time\":\"00:00:00\"}", "TO_JSON_STRING(record)", "null");

            var repeatedRecordType = new StructType(new[] { new Field("value", StringType.Default, true) });
            var repeatedRecords = new StructArray(repeatedRecordType, 3, new IArrowArray[]
            {
                new StringArray.Builder().Append("excluded").Append("nested").AppendNull().Build(),
            }, ArrowBuffer.Empty);
            var records = new ListArray(new ListType(repeatedRecordType), 4,
                new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(3).Append(3).Append(3).Build(),
                repeatedRecords, ArrowBuffer.Empty);
            Add("records", records, "[{\"value\":\"nested\"},{\"value\":null}]", "[]", "TO_JSON_STRING(records)", "[]");
            return cases;
        }

        [SkippableFact]
        public void DefaultProtobufPreservesFlexibleColumnNames()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string table = UniqueTableName("flexible_names");
                RegisterCleanup(environment, catalog, dataset, table);
                var recordType = new StructType(new[] { new Field("first name", StringType.Default, true) });
                var schema = new Schema(new[]
                {
                    new Field("date accessed", Int64Type.Default, true),
                    new Field("user details", recordType, true),
                    new Field("repeated details", new ListType(recordType), true),
                }, null);
                using var batch = new RecordBatch(schema, new IArrowArray[]
                {
                    new Int64Array.Builder().Append(42).AppendNull().Build(),
                    new StructArray(recordType, 2, new IArrowArray[]
                    {
                        new StringArray.Builder().Append("Ada").AppendNull().Build(),
                    }, new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build(), 1),
                    new ListArray(new ListType(recordType), 2,
                        new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(1).Build(),
                        new StructArray(recordType, 1, new IArrowArray[]
                        {
                            new StringArray.Builder().Append("Grace").Build(),
                        }, ArrowBuffer.Empty), ArrowBuffer.Empty),
                }, 2);
                using (AdbcStatement statement = connection.BulkIngest(catalog, dataset, table, BulkIngestMode.Create, false))
                {
                    statement.Bind(batch, schema);
                    Assert.Equal(2, statement.ExecuteUpdate().AffectedRows);
                }
                using AdbcStatement query = connection.CreateStatement();
                query.SqlQuery = $"SELECT `date accessed`, `user details`.`first name`, `repeated details`[SAFE_OFFSET(0)].`first name` FROM `{catalog}.{dataset}.{table}` ORDER BY `date accessed` DESC";
                using IArrowArrayStream stream = query.ExecuteQuery().Stream!;
                using RecordBatch? result = stream.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
                Assert.NotNull(result);
                Assert.Equal(2, result!.Length);
                Assert.Equal(42, ((Int64Array)result.Column(0)).GetValue(0));
                Assert.Equal("Ada", ((StringArray)result.Column(1)).GetString(0));
                Assert.Equal("Grace", ((StringArray)result.Column(2)).GetString(0));
                for (int i = 0; i < 3; i++)
                    Assert.True(result.Column(i).IsNull(1));
            }
        }

        /// <summary>
        /// A simple IArrowArrayStream that yields a fixed set of record batches.
        /// </summary>
        private class TestArrowArrayStream : IArrowArrayStream
        {
            private readonly Queue<RecordBatch> _batches;

            public TestArrowArrayStream(Schema schema, IEnumerable<RecordBatch> batches)
            {
                Schema = schema;
                _batches = new Queue<RecordBatch>(batches);
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(System.Threading.CancellationToken cancellationToken = default)
            {
                if (_batches.Count > 0)
                    return new ValueTask<RecordBatch?>(_batches.Dequeue());
                return new ValueTask<RecordBatch?>((RecordBatch?)null);
            }

            public void Dispose() { }
        }
    }
}
