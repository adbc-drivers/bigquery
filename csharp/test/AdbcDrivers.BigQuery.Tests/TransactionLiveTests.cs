/*
 * Copyright (c) 2026 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Live integration tests for BigQuery transaction support.
    /// Requires valid BigQuery credentials configured via BIGQUERY_TEST_CONFIG_FILE.
    /// Each test creates and cleans up its own table.
    /// </summary>
    public class TransactionLiveTests : IDisposable
    {
        private readonly BigQueryTestConfiguration _testConfiguration;
        private readonly List<BigQueryTestEnvironment> _environments;
        private readonly ITestOutputHelper? _outputHelper;
        private readonly List<(BigQueryTestEnvironment environment, string catalog, string dataset, string table)> _tablesToCleanup = new();

        public TransactionLiveTests(ITestOutputHelper? outputHelper)
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
            return $"adbc_txn_test_{testName}_{DateTime.UtcNow:yyyyMMdd_HHmmss}_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
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

        private void CreateTestTable(AdbcConnection connection, string catalog, string dataset, string tableName)
        {
            using AdbcStatement stmt = connection.CreateStatement();
            stmt.SqlQuery = $"CREATE TABLE `{catalog}.{dataset}.{tableName}` (id INT64, name STRING)";
            stmt.ExecuteUpdate();
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
                    using (batch)
                    {
                        var countCol = (Int64Array)batch.Column(0);
                        return countCol.GetValue(0)!.Value;
                    }
                }
            }
            return -1;
        }

        /// <summary>
        /// Verifies that autocommit can be disabled and re-enabled,
        /// which creates and closes a BigQuery session.
        /// </summary>
        [SkippableFact]
        public void CanDisableAndEnableAutoCommit()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);

                Assert.True(connection.AutoCommit);

                connection.AutoCommit = false;
                Assert.False(connection.AutoCommit);

                connection.AutoCommit = true;
                Assert.True(connection.AutoCommit);

                _outputHelper?.WriteLine($"[{environment.Name}] AutoCommit toggle succeeded");
            }
        }

        /// <summary>
        /// Verifies that a committed transaction persists data.
        /// Creates a table, disables autocommit, inserts a row, commits,
        /// re-enables autocommit, and verifies the row is visible.
        /// </summary>
        [SkippableFact]
        public void CommitPersistsData()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("commit");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);
                CreateTestTable(connection, catalog, dataset, tableName);

                // Disable autocommit to start a transaction
                connection.AutoCommit = false;

                using (AdbcStatement stmt = connection.CreateStatement())
                {
                    stmt.SqlQuery = $"INSERT INTO `{catalog}.{dataset}.{tableName}` (id, name) VALUES (1, 'committed')";
                    stmt.ExecuteUpdate();
                }

                connection.Commit();

                // Re-enable autocommit to close the session
                connection.AutoCommit = true;

                // Verify the row was persisted
                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(1, rowCount);

                _outputHelper?.WriteLine($"[{environment.Name}] Commit persisted {rowCount} row(s)");
            }
        }

        /// <summary>
        /// Verifies that a rolled-back transaction does not persist data.
        /// Creates a table, disables autocommit, inserts a row, rolls back,
        /// re-enables autocommit, and verifies the row is not visible.
        /// </summary>
        [SkippableFact]
        public void RollbackDiscardsData()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("rollback");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);
                CreateTestTable(connection, catalog, dataset, tableName);

                // Disable autocommit to start a transaction
                connection.AutoCommit = false;

                using (AdbcStatement stmt = connection.CreateStatement())
                {
                    stmt.SqlQuery = $"INSERT INTO `{catalog}.{dataset}.{tableName}` (id, name) VALUES (1, 'rolled_back')";
                    stmt.ExecuteUpdate();
                }

                connection.Rollback();

                // Re-enable autocommit to close the session
                connection.AutoCommit = true;

                // Verify the row was NOT persisted
                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(0, rowCount);

                _outputHelper?.WriteLine($"[{environment.Name}] Rollback discarded data as expected");
            }
        }

        /// <summary>
        /// Verifies that multiple commits within a single session work correctly.
        /// </summary>
        [SkippableFact]
        public void MultipleCommitsInOneSession()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string catalog = environment.Metadata.Catalog;
                string dataset = environment.Metadata.Schema;
                string tableName = UniqueTableName("multi");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                EnsureTableDropped(connection, catalog, dataset, tableName);
                RegisterCleanup(environment, catalog, dataset, tableName);
                CreateTestTable(connection, catalog, dataset, tableName);

                connection.AutoCommit = false;

                // First transaction: insert row 1
                using (AdbcStatement stmt = connection.CreateStatement())
                {
                    stmt.SqlQuery = $"INSERT INTO `{catalog}.{dataset}.{tableName}` (id, name) VALUES (1, 'first')";
                    stmt.ExecuteUpdate();
                }
                connection.Commit();

                // Second transaction: insert row 2
                using (AdbcStatement stmt = connection.CreateStatement())
                {
                    stmt.SqlQuery = $"INSERT INTO `{catalog}.{dataset}.{tableName}` (id, name) VALUES (2, 'second')";
                    stmt.ExecuteUpdate();
                }
                connection.Commit();

                connection.AutoCommit = true;

                long rowCount = CountRows(connection, catalog, dataset, tableName);
                Assert.Equal(2, rowCount);

                _outputHelper?.WriteLine($"[{environment.Name}] Multiple commits persisted {rowCount} row(s)");
            }
        }

        /// <summary>
        /// Verifies that Commit and Rollback throw when autocommit is enabled.
        /// </summary>
        [SkippableFact]
        public void CommitAndRollbackThrowWhenAutocommitEnabled()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);

                Assert.Throws<AdbcException>(() => connection.Commit());
                Assert.Throws<AdbcException>(() => connection.Rollback());

                _outputHelper?.WriteLine($"[{environment.Name}] Commit/Rollback correctly rejected with autocommit on");
            }
        }
    }
}
