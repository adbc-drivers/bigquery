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

#if NET8_0_OR_GREATER

using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using AdbcDrivers.BigQuery.MockServer;
using Google.Cloud.BigQuery.Storage.V1;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests.MockServer
{
    [Trait("Category", "MockServer")]
    public class MockServerTests
    {
        [Fact]
        public async System.Threading.Tasks.Task CanExecuteSelectAgainstMockServer()
        {
            using var mockServer = new BigQueryMockServer();

            // Build Arrow schema and record batch for the mock Storage Read API response
            var schema = new Schema(new[]
            {
                new Field("value", Int64Type.Default, nullable: true)
            }, null);

            // Build a single record batch with value = 42
            var int64Builder = new Int64Array.Builder();
            int64Builder.Append(42);
            var batch = new RecordBatch(schema, new IArrowArray[] { int64Builder.Build() }, 1);

            // Serialize using the same format the BigQuery Storage API uses
            byte[] schemaBytes = ArrowSerializationHelpers.SerializeSchema(schema);
            byte[] batchBytes = ArrowSerializationHelpers.SerializeRecordBatch(batch);

            // Configure the mock gRPC service with default data for any table
            mockServer.ReadService.DefaultArrowSchema = schemaBytes;
            mockServer.ReadService.DefaultArrowBatch = batchBytes;
            mockServer.ReadService.DefaultRowCount = 1;

            string projectId = "mock-project";
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, projectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(parameters);
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            // ExecuteQuery drives the full pipeline:
            // 1. REST: POST jobs (create query job)
            // 2. REST: GET jobs/{id} (poll job status)
            // 3. REST: GET queries/{id} (get query results metadata)
            // 4. gRPC: CreateReadSession (create storage read session)
            // 5. gRPC: ReadRows (stream Arrow data)
            QueryResult result = statement.ExecuteQuery();
            Assert.NotNull(result);
            Assert.NotNull(result.Stream);

            // Read the first batch
            using (result.Stream)
            {
                using RecordBatch? resultBatch = await result.Stream.ReadNextRecordBatchAsync();
                Assert.NotNull(resultBatch);
                Assert.Equal(1, resultBatch.Length);

                var column = Assert.IsType<Int64Array>(resultBatch.Column(0));
                Assert.Equal(42L, column.GetValue(0));
            }
        }

        [Fact]
        public async System.Threading.Tasks.Task CanBulkIngestAppendToTable()
        {
            using var mockServer = new BigQueryMockServer();

            string projectId = "mock-project";
            string datasetId = "test_dataset";
            string tableId = "test_table";

            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, projectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(parameters);
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());

            // Create test data
            var schema = new Schema(new[]
            {
                new Field("id", Int64Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: true),
            }, null);

            var idBuilder = new Int64Array.Builder();
            idBuilder.Append(1);
            idBuilder.Append(2);
            idBuilder.Append(3);

            var nameBuilder = new StringArray.Builder();
            nameBuilder.Append("Alice");
            nameBuilder.Append("Bob");
            nameBuilder.Append("Charlie");

            var batch = new RecordBatch(schema, new IArrowArray[]
            {
                idBuilder.Build(),
                nameBuilder.Build()
            }, 3);

            // Use BulkIngest with CreateAppend mode (creates table if missing)
            using AdbcStatement statement = connection.BulkIngest(projectId, datasetId, tableId, BulkIngestMode.CreateAppend, false);
            statement.Bind(batch, schema);

            UpdateResult result = statement.ExecuteUpdate();

            // Verify rows were reported
            Assert.Equal(3, result.AffectedRows);

            // Verify the mock server received the data
            Assert.Single(mockServer.WriteService.Streams);
            var writeStream = Assert.Single(mockServer.WriteService.Streams.Values);
            Assert.Equal(WriteStream.Types.Type.Pending, writeStream.Type);
            Assert.True(writeStream.Finalized, "Write stream should have been finalized");
            Assert.NotNull(writeStream.SchemaBytes);
            Assert.Single(writeStream.RecordBatches);

            // Verify the stream was committed
            Assert.Single(mockServer.WriteService.CommittedStreams);

            // Verify the table was created in the REST API
            // (CreateAppend mode should create it since it didn't exist)
        }

        [Theory]
        [InlineData(AdbcOptions.IngestMode.Create, false)]
        [InlineData(AdbcOptions.IngestMode.Append, true)]
        [InlineData(AdbcOptions.IngestMode.Replace, true)]
        [InlineData(AdbcOptions.IngestMode.CreateAppend, false)]
        public void CanBulkIngestThroughStatementOptions(string mode, bool createTableFirst)
        {
            using var mockServer = new BigQueryMockServer();

            const string projectId = "mock-project";
            const string datasetId = "test_dataset";
            string tableId = $"option_ingest_{mode.Substring(mode.LastIndexOf('.') + 1)}";
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, projectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(parameters);
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using RecordBatch batch = CreateBatch();
            var existingStreamNames = mockServer.WriteService.Streams.Keys.ToHashSet();

            if (createTableFirst)
            {
                using AdbcStatement create = connection.BulkIngest(projectId, datasetId, tableId, BulkIngestMode.Create, false);
                create.Bind(batch, batch.Schema);
                create.ExecuteUpdate();
                existingStreamNames = mockServer.WriteService.Streams.Keys.ToHashSet();
            }

            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(AdbcOptions.Ingest.TargetCatalog, projectId);
            statement.SetOption(AdbcOptions.Ingest.TargetDbSchema, datasetId);
            statement.SetOption(AdbcOptions.Ingest.TargetTable, tableId);
            statement.SetOption(AdbcOptions.Ingest.Temporary, AdbcOptions.Disabled);
            statement.SetOption(AdbcOptions.Ingest.Mode, mode);
            statement.Bind(batch, batch.Schema);

            UpdateResult result = statement.ExecuteUpdate();

            Assert.Equal(3, result.AffectedRows);
            Assert.Empty(mockServer.ExecutedQueries);
            string writeStreamName = Assert.Single(mockServer.WriteService.Streams.Keys.Except(existingStreamNames));
            var writeStream = mockServer.WriteService.Streams[writeStreamName];
            Assert.True(writeStream.Finalized);
            Assert.Single(writeStream.RecordBatches);
        }

        [Fact]
        public void BulkIngestThroughStatementOptionsRejectsTemporaryTable()
        {
            using var mockServer = new BigQueryMockServer();
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, "mock-project" },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(parameters);
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();

            AdbcException exception = Assert.Throws<AdbcException>(
                () => statement.SetOption(AdbcOptions.Ingest.Temporary, AdbcOptions.Enabled));

            Assert.Equal(AdbcStatusCode.NotImplemented, exception.Status);
        }

        [Fact]
        public void DisabledTemporaryOptionDoesNotChangeExecutionMode()
        {
            using var mockServer = new BigQueryMockServer();
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, "mock-project" },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(parameters);
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(AdbcOptions.Ingest.Temporary, AdbcOptions.Disabled);
            statement.SqlQuery = "UPDATE test_table SET value = 1";

            UpdateResult result = statement.ExecuteUpdate();

            Assert.Equal(2, result.AffectedRows);
        }

        [Fact]
        public void InvalidIngestModeDoesNotChangeExecutionMode()
        {
            using var mockServer = new BigQueryMockServer();
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, "mock-project" },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(parameters);
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            Assert.Throws<AdbcException>(() => statement.SetOption(AdbcOptions.Ingest.Mode, "invalid"));
            statement.SqlQuery = "UPDATE test_table SET value = 1";

            UpdateResult result = statement.ExecuteUpdate();

            Assert.Equal(2, result.AffectedRows);
        }

        [Fact]
        public void DropTableExecuteUpdateDoesNotRequestQueryResults()
        {
            using var mockServer = new BigQueryMockServer();
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, "mock-project" },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(parameters);
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "DROP TABLE IF EXISTS `mock-project.test_dataset.test_table`";

            UpdateResult result = statement.ExecuteUpdate();

            Assert.Equal(-1, result.AffectedRows);
            Assert.Equal(0, mockServer.QueryResultsRequestCount);
            Assert.Single(mockServer.ExecutedQueries);
        }

        [Fact]
        public void DmlExecuteUpdateReturnsAffectedRowsWithoutRequestingQueryResults()
        {
            using var mockServer = new BigQueryMockServer();
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, "mock-project" },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(parameters);
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "UPDATE test_table SET value = 1";

            UpdateResult result = statement.ExecuteUpdate();

            Assert.Equal(2, result.AffectedRows);
            Assert.Equal(0, mockServer.QueryResultsRequestCount);
        }

        private static RecordBatch CreateBatch()
        {
            var schema = new Schema(new[]
            {
                new Field("id", Int64Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: true),
            }, null);

            return new RecordBatch(
                schema,
                new IArrowArray[]
                {
                    new Int64Array.Builder().Append(1).Append(2).Append(3).Build(),
                    new StringArray.Builder().Append("Alice").Append("Bob").Append("Charlie").Build(),
                },
                3);
        }
    }
}

#endif
