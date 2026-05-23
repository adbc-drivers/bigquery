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
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
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
        private readonly List<(AdbcConnection connection, string catalog, string dataset, string table)> _tablesToCleanup = new();

        public BulkIngestTests(ITestOutputHelper? outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            _testConfiguration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(_testConfiguration);
            _outputHelper = outputHelper;
        }

        public void Dispose()
        {
            foreach (var (connection, catalog, dataset, table) in _tablesToCleanup)
            {
                try
                {
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

        private void RegisterCleanup(AdbcConnection connection, string catalog, string dataset, string tableName)
        {
            _tablesToCleanup.Add((connection, catalog, dataset, tableName));
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

        [SkippableFact]
        public void CanBulkIngestCreateMode()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("create");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(connection, catalog, dataset, tableName);

                RecordBatch batch = CreateSimpleBatch(3);

                using AdbcStatement statement = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false);
                statement.Bind(batch, batch.Schema);
                UpdateResult result = statement.ExecuteUpdate();

                Assert.Equal(3, result.AffectedRows);

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(3, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestCreateMode: inserted {result.AffectedRows} rows into {catalog}.{dataset}.{tableName}");
            }
        }

        [SkippableFact]
        public void CanBulkIngestAppendMode()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("append");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(connection, catalog, dataset, tableName);

                // First, create the table with initial data
                RecordBatch batch1 = CreateSimpleBatch(2);
                using (AdbcStatement stmt1 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false))
                {
                    stmt1.Bind(batch1, batch1.Schema);
                    stmt1.ExecuteUpdate();
                }

                // Then append more data
                RecordBatch batch2 = CreateSimpleBatch(3);
                using (AdbcStatement stmt2 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Append, false))
                {
                    stmt2.Bind(batch2, batch2.Schema);
                    UpdateResult result = stmt2.ExecuteUpdate();
                    Assert.Equal(3, result.AffectedRows);
                }

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(5, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestAppendMode: total {rowCount} rows in {catalog}.{dataset}.{tableName}");
            }
        }

        [SkippableFact]
        public void CanBulkIngestCreateAppendMode()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("createappend");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(connection, catalog, dataset, tableName);

                // CreateAppend should create the table since it doesn't exist
                RecordBatch batch1 = CreateSimpleBatch(2);
                using (AdbcStatement stmt1 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.CreateAppend, false))
                {
                    stmt1.Bind(batch1, batch1.Schema);
                    stmt1.ExecuteUpdate();
                }

                // CreateAppend again should append since table now exists
                RecordBatch batch2 = CreateSimpleBatch(3);
                using (AdbcStatement stmt2 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.CreateAppend, false))
                {
                    stmt2.Bind(batch2, batch2.Schema);
                    stmt2.ExecuteUpdate();
                }

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(5, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestCreateAppendMode: total {rowCount} rows in {catalog}.{dataset}.{tableName}");
            }
        }

        [SkippableFact]
        public void BulkIngestCreateFailsIfTableExists()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("createfail");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(connection, catalog, dataset, tableName);

                // Create the table first
                RecordBatch batch = CreateSimpleBatch(1);
                using (AdbcStatement stmt1 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false))
                {
                    stmt1.Bind(batch, batch.Schema);
                    stmt1.ExecuteUpdate();
                }

                // Create again should fail
                using AdbcStatement stmt2 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false);
                stmt2.Bind(batch, batch.Schema);
                AdbcException ex = Assert.Throws<AdbcException>(() => stmt2.ExecuteUpdate());
                Assert.Equal(AdbcStatusCode.AlreadyExists, ex.Status);

                _outputHelper?.WriteLine($"BulkIngestCreateFailsIfTableExists: correctly threw {ex.Status}");
            }
        }

        [SkippableFact]
        public void BulkIngestAppendFailsIfTableMissing()
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
                stmt.Bind(batch, batch.Schema);
                AdbcException ex = Assert.Throws<AdbcException>(() => stmt.ExecuteUpdate());
                Assert.Equal(AdbcStatusCode.NotFound, ex.Status);

                _outputHelper?.WriteLine($"BulkIngestAppendFailsIfTableMissing: correctly threw {ex.Status}");
            }
        }

        [SkippableFact]
        public void CanBulkIngestReplaceMode()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("replace");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(connection, catalog, dataset, tableName);

                // Create with 5 rows
                RecordBatch batch1 = CreateSimpleBatch(5);
                using (AdbcStatement stmt1 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false))
                {
                    stmt1.Bind(batch1, batch1.Schema);
                    stmt1.ExecuteUpdate();
                }

                // Replace with 2 rows
                RecordBatch batch2 = CreateSimpleBatch(2);
                using (AdbcStatement stmt2 = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Replace, false))
                {
                    stmt2.Bind(batch2, batch2.Schema);
                    UpdateResult result = stmt2.ExecuteUpdate();
                    Assert.Equal(2, result.AffectedRows);
                }

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(2, rowCount);

                _outputHelper?.WriteLine($"CanBulkIngestReplaceMode: replaced with {rowCount} rows in {catalog}.{dataset}.{tableName}");
            }
        }

        [SkippableFact]
        public void CanBulkIngestWithBindStream()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("bindstream");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(connection, catalog, dataset, tableName);

                RecordBatch batch1 = CreateSimpleBatch(2);
                RecordBatch batch2 = CreateSimpleBatch(3);

                // Use BindStream with multiple batches
                var stream = new TestArrowArrayStream(batch1.Schema, new[] { batch1, batch2 });

                using AdbcStatement stmt = connection.BulkIngest(catalog, dataset, tableName, BulkIngestMode.Create, false);
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
