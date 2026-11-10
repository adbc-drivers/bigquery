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
using System.Net;
using System.Threading.Tasks;
using AdbcDrivers.BigQuery.MockServer;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests.Metadata;
using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests.MockServer
{
    /// <summary>
    /// The metadata calls build their SQL against INFORMATION_SCHEMA. Everything a caller supplies
    /// except the catalog and dataset is bound as a query parameter, because BigQuery parameters
    /// cannot stand in for the identifiers that name the view. These tests drive the real driver
    /// against <see cref="BigQueryMockServer"/> and inspect the SQL and parameters it sent, so they
    /// need no credentials.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MetadataInjectionTests
    {
        private const string ProjectId = "mock-project";
        private const string DbSchema = "mock_dataset";

        /// <summary>
        /// A payload whose first character is in the identifier allowlist. The allowlist regex
        /// used to be unanchored, so a match on that first character was enough to pass the whole
        /// string through into the query text.
        /// </summary>
        private const string Payload = "t'; DROP TABLE secrets; SELECT '";

        [Fact]
        public void GetTableSchemaBindsTableNameInsteadOfInterpolatingIt()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            connection.GetTableSchema(ProjectId, DbSchema, Payload);

            string query = Assert.Single(mockServer.ExecutedQueries);
            Assert.DoesNotContain("DROP TABLE secrets", query);
            Assert.Contains("table_name = @tableName", query);

            QueryParameter parameter = Assert.Single(Assert.Single(mockServer.ExecutedQueryParameters));
            Assert.Equal("tableName", parameter.Name);
            Assert.Equal(Payload, parameter.ParameterValue.Value);
        }

        [Fact]
        public void GetObjectsUsesRestApiAndMatchesTableAndColumnPatternsClientSide()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "my_table",
                },
                Type = "TABLE",
                Schema = new TableSchema
                {
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
                    },
                },
            });

            using AdbcConnection connection = Connect(mockServer);

            connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: Payload,
                tableTypes: new List<string> { "BASE TABLE" },
                columnNamePattern: Payload);

            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.TablesList));
        }

        [Fact]
        public async Task GetObjectsSupportsPcntDatasets()
        {
            const string pcntDataset = "my_catalog.my_namespace";
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = pcntDataset,
                    TableId = "pcnt_table",
                },
                Type = "TABLE",
                Schema = new TableSchema
                {
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
                        new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
                    },
                },
                TableConstraints = new TableConstraints
                {
                    PrimaryKey = new TableConstraints.PrimaryKeyData
                    {
                        Columns = new[] { "id" },
                    },
                },
            });

            using AdbcConnection connection = Connect(mockServer);

            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                catalogPattern: ProjectId,
                dbSchemaPattern: pcntDataset,
                tableNamePattern: "pcnt_table",
                tableTypes: new List<string> { "BASE TABLE" },
                columnNamePattern: null);

            var batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.TablesList));
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.TableGet));
        }

        [Fact]
        public async Task GetObjectsRetriesTransientTablesListError()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "retry_table",
                },
                Type = "TABLE",
            });

            // Pair 500 with a reason Google.Apis ignores so RetryManager (not the HTTP handler) retries.
            mockServer.QueueError(MockRequestKind.TablesList, HttpStatusCode.InternalServerError, "invalidQuery");

            using AdbcConnection connection = Connect(mockServer, new Dictionary<string, string>
            {
                { BigQueryParameters.RetryDelayMs, "1" },
            });

            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Tables,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: "retry_table",
                tableTypes: new List<string> { "BASE TABLE" },
                columnNamePattern: null);

            var batch = await stream.ReadNextRecordBatchAsync();
            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(batch, DbSchema);
            AdbcTable table = Assert.Single(Assert.Single(Assert.Single(catalogs).DbSchemas!).Tables!);

            Assert.Equal("retry_table", table.Name);
            Assert.Equal("BASE TABLE", table.Type);
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.TablesList));
        }

        [Fact]
        public async Task GetObjectsRefreshesTokenWhenTablesListReturnsUnauthorized()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "auth_table",
                },
                Type = "TABLE",
            });

            mockServer.QueueError(MockRequestKind.TablesList, HttpStatusCode.Unauthorized, "authError");

            using AdbcConnection connection = Connect(mockServer, new Dictionary<string, string>
            {
                { BigQueryParameters.RetryDelayMs, "1" },
            });

            bool tokenRefreshed = false;
            ((BigQueryConnection)connection).UpdateToken = () =>
            {
                tokenRefreshed = true;
                ((BigQueryConnection)connection).UpdateClientToken();
                return Task.CompletedTask;
            };

            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Tables,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: "auth_table",
                tableTypes: null,
                columnNamePattern: null);

            var batch = await stream.ReadNextRecordBatchAsync();
            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(batch, DbSchema);
            AdbcTable table = Assert.Single(Assert.Single(Assert.Single(catalogs).DbSchemas!).Tables!);

            Assert.True(tokenRefreshed);
            Assert.Equal("auth_table", table.Name);
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.TablesList));
        }

        [Theory]
        [InlineData("MATERIALIZED VIEW")]
        [InlineData("MATERIALIZED_VIEW")]
        public async Task GetObjectsNormalizesMaterializedViewTableType(string filterType)
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "my_mv",
                },
                Type = "MATERIALIZED_VIEW",
            });

            using AdbcConnection connection = Connect(mockServer);

            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.Tables,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: "my_mv",
                tableTypes: new List<string> { filterType },
                columnNamePattern: null);

            var batch = await stream.ReadNextRecordBatchAsync();
            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(batch, DbSchema);
            AdbcTable table = Assert.Single(Assert.Single(Assert.Single(catalogs).DbSchemas!).Tables!);

            Assert.Equal("my_mv", table.Name);
            Assert.Equal("MATERIALIZED VIEW", table.Type);
        }

        [Fact]
        public async Task GetObjectsMapsRepeatedFieldsAsNonNullableArrays()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "array_table",
                },
                Type = "TABLE",
                Schema = new TableSchema
                {
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "req_id", Type = "INTEGER", Mode = "REQUIRED" },
                        new TableFieldSchema { Name = "opt_name", Type = "STRING", Mode = "NULLABLE" },
                        new TableFieldSchema { Name = "default_mode", Type = "STRING" },
                        new TableFieldSchema { Name = "int_array", Type = "INTEGER", Mode = "REPEATED" },
                        new TableFieldSchema { Name = "dec_array", Type = "NUMERIC", Mode = "REPEATED", Precision = 10, Scale = 2 },
                    },
                },
            });

            using AdbcConnection connection = Connect(mockServer);

            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: "array_table",
                tableTypes: null,
                columnNamePattern: null);

            var batch = await stream.ReadNextRecordBatchAsync();
            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(batch, DbSchema);
            List<AdbcColumn> columns = Assert.Single(Assert.Single(Assert.Single(catalogs).DbSchemas!).Tables!).Columns!;

            Assert.Equal(5, columns.Count);

            Assert.Equal("NO", columns[0].XdbcIsNullable);
            Assert.Equal("INTEGER", columns[0].XdbcTypeName);
            Assert.Equal((short)4, columns[0].XdbcSqlDataType);

            Assert.Equal("YES", columns[1].XdbcIsNullable);
            Assert.Equal("YES", columns[2].XdbcIsNullable);

            Assert.Equal("ARRAY", columns[3].XdbcTypeName);
            Assert.Equal((short)0, columns[3].XdbcSqlDataType);
            Assert.Equal("NO", columns[3].XdbcIsNullable);
            Assert.Null(columns[3].XdbcColumnSize);
            Assert.Null(columns[3].XdbcDecimalDigits);

            Assert.Equal("ARRAY", columns[4].XdbcTypeName);
            Assert.Equal((short)0, columns[4].XdbcSqlDataType);
            Assert.Equal("NO", columns[4].XdbcIsNullable);
            Assert.Null(columns[4].XdbcColumnSize);
            Assert.Null(columns[4].XdbcDecimalDigits);
        }

        [Theory]
        // A backtick would close the quoted identifier that names the INFORMATION_SCHEMA view.
        [InlineData("mock-project` UNION ALL SELECT 1 --")]
        // The allowlist is anchored at both ends, so a valid prefix is no longer sufficient.
        [InlineData("mock-project'; DROP TABLE secrets --")]
        [InlineData("mock-project.other")]
        public void InvalidCatalogIsRejectedBeforeAnyQueryRuns(string catalog)
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            AdbcException exception = Assert.Throws<AdbcException>(
                () => connection.GetTableSchema(catalog, DbSchema, "some_table"));

            Assert.Equal(AdbcStatusCode.InvalidArgument, exception.Status);
            Assert.Empty(mockServer.ExecutedQueries);
        }

        [Fact]
        public void InvalidDbSchemaIsRejectedBeforeAnyQueryRuns()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            Assert.Throws<AdbcException>(
                () => connection.GetTableSchema(ProjectId, "mock_dataset` UNION ALL SELECT 1 --", "some_table"));

            Assert.Empty(mockServer.ExecutedQueries);
        }

        [Fact]
        public void ValidNamesStillReachTheQuery()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            connection.GetTableSchema("mock-project", "mock_dataset", "my_table");

            string query = Assert.Single(mockServer.ExecutedQueries);
            Assert.Contains("`mock-project`.`mock_dataset`.INFORMATION_SCHEMA.COLUMNS", query);
        }

        [Fact]
        public void TableNamesBigQueryAllowsButTheOldAllowlistWouldRejectAreBound()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            // BigQuery table names may use any Unicode letter, mark, number, connector, dash or
            // space. None of these survive an ASCII identifier allowlist, which is one reason the
            // table name is bound rather than validated.
            const string tableName = "étudiant-01 お客様";
            connection.GetTableSchema(ProjectId, DbSchema, tableName);

            QueryParameter parameter = Assert.Single(Assert.Single(mockServer.ExecutedQueryParameters));
            Assert.Equal(tableName, parameter.ParameterValue.Value);
        }

        private static AdbcConnection Connect(BigQueryMockServer mockServer, IDictionary<string, string>? extraParameters = null)
        {
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, ProjectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            if (extraParameters != null)
            {
                foreach (var kvp in extraParameters)
                {
                    parameters[kvp.Key] = kvp.Value;
                }
            }

            var driver = new BigQueryDriver();
            AdbcDatabase database = driver.Open(parameters);

            return database.Connect(new Dictionary<string, string>());
        }
    }
}

#endif
