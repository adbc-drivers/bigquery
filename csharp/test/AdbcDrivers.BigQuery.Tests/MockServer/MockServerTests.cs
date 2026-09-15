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

using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Types;
using AdbcDrivers.BigQuery.MockServer;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.Storage.V1;
using Xunit;
using RestTableFieldSchema = Google.Apis.Bigquery.v2.Data.TableFieldSchema;
using RestTableSchema = Google.Apis.Bigquery.v2.Data.TableSchema;

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
        public void UseJobCreationModeExecutesThroughQueriesEndpoint()
        {
            using var mockServer = new BigQueryMockServer();
            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(BigQueryParameters.UseJobCreationMode, "true");
            statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = statement.ExecuteQuery();
            Assert.NotNull(result.Stream);
            result.Stream.Dispose();

            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobQuery));
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            Assert.NotNull(mockServer.LastQueryRequest);
            Assert.Equal("JOB_CREATION_REQUIRED", mockServer.LastQueryRequest!.JobCreationMode);
        }

        [Fact]
        public void UseJobCreationModeCreatesQueryExecutionActivity()
        {
            const string queryActivityName = "BigQueryStatement.ExecuteQueryInternalAsync.Jobs.Query";
            Activity? queryActivity = null;
            using var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = activity =>
                {
                    if (activity.OperationName == queryActivityName)
                    {
                        queryActivity = activity;
                    }
                },
            };
            ActivitySource.AddActivityListener(listener);

            using var mockServer = new BigQueryMockServer();
            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(BigQueryParameters.UseJobCreationMode, "true");
            statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = statement.ExecuteQuery();
            result.Stream?.Dispose();

            Assert.NotNull(queryActivity);
            Assert.Equal(ActivityStatusCode.Ok, queryActivity!.Status);
            Assert.NotNull(queryActivity.Parent);
            Assert.True(queryActivity.Duration > TimeSpan.Zero);
        }

        [Fact]
        public void UseJobCreationModeWithLargeResultsExecutesThroughJobInsertEndpoint()
        {
            using var mockServer = new BigQueryMockServer();
            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(BigQueryParameters.UseJobCreationMode, "true");
            statement.SetOption(BigQueryParameters.AllowLargeResults, "true");
            statement.SetOption(BigQueryParameters.LargeResultsDestinationTable, "mock-project.test_dataset.test_table");
            statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = statement.ExecuteQuery();
            Assert.NotNull(result.Stream);
            result.Stream.Dispose();

            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.JobQuery));
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            Assert.NotNull(mockServer.LastInsertedJob);
            Assert.True(mockServer.LastInsertedJob!.Configuration.Query.AllowLargeResults);
            Assert.Equal("mock-project", mockServer.LastInsertedJob.Configuration.Query.DestinationTable.ProjectId);
            Assert.Equal("test_dataset", mockServer.LastInsertedJob.Configuration.Query.DestinationTable.DatasetId);
            Assert.Equal("test_table", mockServer.LastInsertedJob.Configuration.Query.DestinationTable.TableId);
        }

        [Fact]
        public async Task UseJobCreationModeReturnsEligibleInlineResultsWithoutStorageRead()
        {
            using var mockServer = new BigQueryMockServer();
            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(BigQueryParameters.UseJobCreationMode, "true");
            statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = statement.ExecuteQuery();

            Assert.Equal(0, mockServer.ReadService.CreateReadSessionCallCount);
            Assert.Equal(0, mockServer.QueryResultsRequestCount);
            await AssertSingleRowAsync(result);
        }

        public static IEnumerable<object[]> SupportedInlineRestValues()
        {
            yield return new object[] { "integer", "INTEGER", "NULLABLE", "42" };
            yield return new object[] { "float", "FLOAT", "NULLABLE", "1.25" };
            yield return new object[] { "boolean", "BOOLEAN", "NULLABLE", "true" };
            yield return new object[] { "string", "STRING", "NULLABLE", "hello" };
            yield return new object[] { "bytes", "BYTES", "NULLABLE", Convert.ToBase64String(Encoding.UTF8.GetBytes("abc123")) };
            yield return new object[] { "date", "DATE", "NULLABLE", "2023-09-08" };
            yield return new object[] { "datetime", "DATETIME", "NULLABLE", "2023-09-08T12:34:56" };
            yield return new object[] { "timestamp", "TIMESTAMP", "NULLABLE", "1694176496000000" };
            yield return new object[] { "time", "TIME", "NULLABLE", "12:34:56" };
            yield return new object[] { "numeric", "NUMERIC", "NULLABLE", "4.56" };
            yield return new object[] { "bignumeric", "BIGNUMERIC", "NULLABLE", "7.89000000000000000000000000000000000001" };
            yield return new object[]
            {
                "repeated",
                "INTEGER",
                "REPEATED",
                new object[]
                {
                    new Dictionary<string, object?> { ["v"] = "1" },
                    new Dictionary<string, object?> { ["v"] = "2" },
                    new Dictionary<string, object?> { ["v"] = "3" },
                },
            };
        }

        [Theory]
        [MemberData(nameof(SupportedInlineRestValues))]
        public async Task UseJobCreationModeConvertsSupportedInlineRestValuesWithoutStorageRead(
            string testCase,
            string fieldType,
            string fieldMode,
            object restValue)
        {
            using var mockServer = new BigQueryMockServer
            {
                InlineQueryResponse = new QueryResponse
                {
                    JobComplete = true,
                    QueryId = $"mock-query-{testCase}",
                    TotalRows = 1,
                    Schema = new RestTableSchema
                    {
                        Fields = new[]
                        {
                            new RestTableFieldSchema { Name = "value", Type = fieldType, Mode = fieldMode },
                        },
                    },
                    Rows = new[]
                    {
                        new TableRow { F = new[] { new TableCell { V = restValue } } },
                    },
                },
            };
            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(BigQueryParameters.UseJobCreationMode, "true");
            statement.SqlQuery = "SELECT value";

            QueryResult result = statement.ExecuteQuery();

            Assert.Equal(0, mockServer.ReadService.CreateReadSessionCallCount);
            Assert.Equal(0, mockServer.QueryResultsRequestCount);
            Assert.NotNull(result.Stream);
            using (result.Stream)
            {
                using RecordBatch? batch = await result.Stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);
                AssertInlineRestValue(testCase, batch.Schema.GetFieldByIndex(0), batch.Column(0));
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task UseJobCreationModeFallsBackForIncompleteOrPagedResults(bool includesPagedResults)
        {
            using var mockServer = new BigQueryMockServer
            {
                OptionalQueryCreatesJob = true,
                OptionalQueryIncludesPagedResults = includesPagedResults,
            };
            ConfigureSingleRowStorageResults(mockServer);

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(BigQueryParameters.UseJobCreationMode, "true");
            statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = statement.ExecuteQuery();

            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobQuery));
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            Assert.True(mockServer.QueryResultsRequestCount > 0);
            Assert.Equal(1, mockServer.ReadService.CreateReadSessionCallCount);
            await AssertSingleRowAsync(result);
        }

        [Fact]
        public void CanBulkIngestAppendToTable()
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

        private static Dictionary<string, string> CreateParameters(BigQueryMockServer mockServer) => new()
        {
            { BigQueryParameters.ProjectId, "mock-project" },
            { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
            { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
            { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
        };

        private static void ConfigureSingleRowStorageResults(BigQueryMockServer mockServer)
        {
            var schema = new Schema(new[] { new Field("value", Int64Type.Default, nullable: true) }, null);
            using var batch = new RecordBatch(
                schema,
                new IArrowArray[] { new Int64Array.Builder().Append(42).Build() },
                1);

            mockServer.ReadService.DefaultArrowSchema = ArrowSerializationHelpers.SerializeSchema(schema);
            mockServer.ReadService.DefaultArrowBatch = ArrowSerializationHelpers.SerializeRecordBatch(batch);
            mockServer.ReadService.DefaultRowCount = 1;
        }

        private static async Task AssertSingleRowAsync(QueryResult result)
        {
            Assert.NotNull(result.Stream);
            using (result.Stream)
            {
                using RecordBatch? batch = await result.Stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch!.Length);
                Assert.Equal(42L, Assert.IsType<Int64Array>(batch.Column(0)).GetValue(0));
            }
        }

        private static void AssertInlineRestValue(string testCase, Field field, IArrowArray array)
        {
            switch (testCase)
            {
                case "integer":
                    Assert.IsType<Int64Type>(field.DataType);
                    Assert.Equal(42L, Assert.IsType<Int64Array>(array).GetValue(0));
                    break;
                case "float":
                    Assert.IsType<DoubleType>(field.DataType);
                    Assert.Equal(1.25d, Assert.IsType<DoubleArray>(array).GetValue(0));
                    break;
                case "boolean":
                    Assert.IsType<BooleanType>(field.DataType);
                    Assert.True(Assert.IsType<BooleanArray>(array).GetValue(0));
                    break;
                case "string":
                    Assert.IsType<StringType>(field.DataType);
                    Assert.Equal("hello", Assert.IsType<StringArray>(array).GetString(0));
                    break;
                case "bytes":
                    Assert.IsType<BinaryType>(field.DataType);
                    Assert.Equal(Encoding.UTF8.GetBytes("abc123"), Assert.IsType<BinaryArray>(array).GetBytes(0).ToArray());
                    break;
                case "date":
                    Assert.IsType<Date32Type>(field.DataType);
                    Assert.Equal(new DateTime(2023, 9, 8), Assert.IsType<Date32Array>(array).GetDateTime(0));
                    break;
                case "datetime":
                    var dateTimeType = Assert.IsType<TimestampType>(field.DataType);
                    Assert.Null(dateTimeType.Timezone);
                    Assert.Equal(
                        new DateTimeOffset(2023, 9, 8, 12, 34, 56, TimeSpan.Zero),
                        Assert.IsType<TimestampArray>(array).GetTimestamp(0));
                    break;
                case "timestamp":
                    var timestampType = Assert.IsType<TimestampType>(field.DataType);
                    Assert.Equal("UTC", timestampType.Timezone);
                    Assert.Equal(
                        new DateTimeOffset(2023, 9, 8, 12, 34, 56, TimeSpan.Zero),
                        Assert.IsType<TimestampArray>(array).GetTimestamp(0));
                    break;
                case "time":
                    Assert.IsType<Time64Type>(field.DataType);
                    Assert.Equal(new TimeOnly(12, 34, 56), Assert.IsType<Time64Array>(array).GetTime(0));
                    break;
                case "numeric":
                    Assert.IsType<Decimal128Type>(field.DataType);
                    Assert.Equal(SqlDecimal.Parse("4.56"), Assert.IsType<Decimal128Array>(array).GetSqlDecimal(0));
                    break;
                case "bignumeric":
                    Assert.IsType<StringType>(field.DataType);
                    Assert.Equal("7.89000000000000000000000000000000000001", Assert.IsType<StringArray>(array).GetString(0));
                    break;
                case "repeated":
                    var listType = Assert.IsType<ListType>(field.DataType);
                    Assert.IsType<Int64Type>(listType.ValueDataType);
                    ListArray list = Assert.IsType<ListArray>(array);
                    Int64Array values = Assert.IsType<Int64Array>(list.GetSlicedValues(0));
                    Assert.Equal(new long?[] { 1, 2, 3 }, Enumerable.Range(0, values.Length).Select(values.GetValue));
                    break;
                default:
                    throw new InvalidOperationException($"Unknown inline REST test case '{testCase}'.");
            }
        }

    }
}

#endif
