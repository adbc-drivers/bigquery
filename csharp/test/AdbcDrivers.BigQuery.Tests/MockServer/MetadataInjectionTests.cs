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
        public async Task GetObjectsUsesRestApiAndMatchesTableAndColumnPatternsClientSide()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "orders_2026",
                },
                Type = "TABLE",
                Schema = new TableSchema
                {
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
                        new TableFieldSchema { Name = "order_total", Type = "NUMERIC", Mode = "NULLABLE" },
                        new TableFieldSchema { Name = "order_status", Type = "STRING", Mode = "NULLABLE" },
                        new TableFieldSchema { Name = "notes", Type = "STRING", Mode = "NULLABLE" },
                    },
                },
            });
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "customers_2026",
                },
                Type = "TABLE",
                Schema = new TableSchema
                {
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "order_customer", Type = "STRING", Mode = "NULLABLE" },
                    },
                },
            });

            using AdbcConnection connection = Connect(mockServer);

            // Injection payload does not match any table and executes no SQL.
            using (var payloadStream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: Payload,
                tableTypes: new List<string> { "BASE TABLE" },
                columnNamePattern: Payload))
            {
                var payloadBatch = await payloadStream.ReadNextRecordBatchAsync();
                List<AdbcCatalog> payloadCatalogs = GetObjectsParser.ParseCatalog(payloadBatch, DbSchema);
                Assert.Empty(Assert.Single(Assert.Single(payloadCatalogs).DbSchemas!).Tables!);
            }

            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Equal(1, mockServer.CountOfKind(MockRequestKind.TablesList));
            Assert.Equal(0, mockServer.CountOfKind(MockRequestKind.TableGet));

            // Matching table pattern with injection payload as column pattern reaches GetColumnSchema safely.
            using (var columnPayloadStream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: "orders_%",
                tableTypes: new List<string> { "BASE TABLE" },
                columnNamePattern: Payload))
            {
                var columnPayloadBatch = await columnPayloadStream.ReadNextRecordBatchAsync();
                List<AdbcCatalog> columnPayloadCatalogs = GetObjectsParser.ParseCatalog(columnPayloadBatch, DbSchema);
                AdbcTable matchedTable = Assert.Single(Assert.Single(Assert.Single(columnPayloadCatalogs).DbSchemas!).Tables!);
                Assert.Equal("orders_2026", matchedTable.Name);
                Assert.Empty(matchedTable.Columns!);
            }

            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.TablesList));
            Assert.Equal(1, mockServer.CountOfKind(MockRequestKind.TableGet));

            // Matching table and column patterns filter out nonmatching tables/columns while preserving original ordinals.
            using (var filteredStream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: "orders_%",
                tableTypes: new List<string> { "BASE TABLE" },
                columnNamePattern: "order_%"))
            {
                var filteredBatch = await filteredStream.ReadNextRecordBatchAsync();
                List<AdbcCatalog> filteredCatalogs = GetObjectsParser.ParseCatalog(filteredBatch, DbSchema);
                AdbcTable table = Assert.Single(Assert.Single(Assert.Single(filteredCatalogs).DbSchemas!).Tables!);

                Assert.Equal("orders_2026", table.Name);
                Assert.NotNull(table.Columns);
                Assert.Equal(2, table.Columns!.Count);
                Assert.Equal("order_total", table.Columns[0].Name);
                Assert.Equal(2, table.Columns[0].OrdinalPosition);
                Assert.Equal("order_status", table.Columns[1].Name);
                Assert.Equal(3, table.Columns[1].OrdinalPosition);
            }

            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Equal(3, mockServer.CountOfKind(MockRequestKind.TablesList));
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.TableGet));
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

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(batch, pcntDataset);
            AdbcCatalog catalog = Assert.Single(catalogs);
            Assert.Equal(ProjectId, catalog.Name);

            AdbcDbSchema dbSchema = Assert.Single(catalog.DbSchemas!);
            Assert.Equal(pcntDataset, dbSchema.Name);

            AdbcTable table = Assert.Single(dbSchema.Tables!);
            Assert.Equal("pcnt_table", table.Name);
            Assert.Equal("BASE TABLE", table.Type);

            Assert.NotNull(table.Columns);
            Assert.Equal(2, table.Columns!.Count);
            Assert.Equal("id", table.Columns[0].Name);
            Assert.Equal(1, table.Columns[0].OrdinalPosition);
            Assert.Equal("INTEGER", table.Columns[0].XdbcTypeName);
            Assert.Equal("NO", table.Columns[0].XdbcIsNullable);
            Assert.Equal("name", table.Columns[1].Name);
            Assert.Equal(2, table.Columns[1].OrdinalPosition);
            Assert.Equal("STRING", table.Columns[1].XdbcTypeName);
            Assert.Equal("YES", table.Columns[1].XdbcIsNullable);

            AdbcConstraint pk = Assert.Single(table.Constraints!);
            Assert.Equal(string.Empty, pk.Name);
            Assert.Equal("PRIMARY KEY", pk.Type);
            Assert.Equal(new[] { "id" }, pk.ColumnNames);
            Assert.Empty(pk.ColumnUsage);

            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.TablesList));
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.TableGet));
        }

        [Fact]
        public async Task GetObjectsMapsCompositeForeignKeysScopedPerTable()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "orders",
                },
                Type = "TABLE",
                Schema = new TableSchema
                {
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "order_id", Type = "INTEGER", Mode = "REQUIRED" },
                        new TableFieldSchema { Name = "tenant_id", Type = "STRING", Mode = "REQUIRED" },
                        new TableFieldSchema { Name = "customer_id", Type = "INTEGER", Mode = "REQUIRED" },
                    },
                },
                TableConstraints = new TableConstraints
                {
                    PrimaryKey = new TableConstraints.PrimaryKeyData
                    {
                        Columns = new[] { "order_id" },
                    },
                    ForeignKeys = new[]
                    {
                        new TableConstraints.ForeignKeysData
                        {
                            Name = "fk_shared",
                            ReferencedTable = new TableConstraints.ForeignKeysData.ReferencedTableData
                            {
                                ProjectId = "ref-project",
                                DatasetId = "ref_dataset",
                                TableId = "customers",
                            },
                            ColumnReferences = new[]
                            {
                                new TableConstraints.ForeignKeysData.ColumnReferencesData
                                {
                                    ReferencingColumn = "tenant_id",
                                    ReferencedColumn = "id_tenant",
                                },
                                new TableConstraints.ForeignKeysData.ColumnReferencesData
                                {
                                    ReferencingColumn = "customer_id",
                                    ReferencedColumn = "id_customer",
                                },
                            },
                        },
                    },
                },
            });
            mockServer.AddTable(new Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "invoices",
                },
                Type = "TABLE",
                Schema = new TableSchema
                {
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "account_id", Type = "INTEGER", Mode = "REQUIRED" },
                    },
                },
                TableConstraints = new TableConstraints
                {
                    ForeignKeys = new[]
                    {
                        new TableConstraints.ForeignKeysData
                        {
                            Name = "fk_shared",
                            ReferencedTable = new TableConstraints.ForeignKeysData.ReferencedTableData
                            {
                                ProjectId = "other-project",
                                DatasetId = "other_dataset",
                                TableId = "accounts",
                            },
                            ColumnReferences = new[]
                            {
                                new TableConstraints.ForeignKeysData.ColumnReferencesData
                                {
                                    ReferencingColumn = "account_id",
                                    ReferencedColumn = "acct_id",
                                },
                            },
                        },
                    },
                },
            });

            using AdbcConnection connection = Connect(mockServer);

            using var stream = connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);

            var batch = await stream.ReadNextRecordBatchAsync();
            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(batch, DbSchema);
            List<AdbcTable> tables = Assert.Single(Assert.Single(catalogs).DbSchemas!).Tables!;

            Assert.Equal(2, tables.Count);

            AdbcTable orders = Assert.Single(tables, t => t.Name == "orders");
            Assert.NotNull(orders.Constraints);
            Assert.Equal(2, orders.Constraints!.Count);

            AdbcConstraint ordersPk = Assert.Single(orders.Constraints, c => c.Type == "PRIMARY KEY");
            Assert.Equal(string.Empty, ordersPk.Name);
            Assert.Equal(new[] { "order_id" }, ordersPk.ColumnNames);
            Assert.Empty(ordersPk.ColumnUsage);

            AdbcConstraint ordersFk = Assert.Single(orders.Constraints, c => c.Type == "FOREIGN KEY");
            Assert.Equal("fk_shared", ordersFk.Name);
            Assert.Equal(new[] { "tenant_id", "customer_id" }, ordersFk.ColumnNames);
            Assert.Equal(2, ordersFk.ColumnUsage.Count);
            Assert.Equal("ref-project", ordersFk.ColumnUsage[0].FkCatalog);
            Assert.Equal("ref_dataset", ordersFk.ColumnUsage[0].FkDbSchema);
            Assert.Equal("customers", ordersFk.ColumnUsage[0].FkTable);
            Assert.Equal("id_tenant", ordersFk.ColumnUsage[0].FkColumnName);
            Assert.Equal("ref-project", ordersFk.ColumnUsage[1].FkCatalog);
            Assert.Equal("ref_dataset", ordersFk.ColumnUsage[1].FkDbSchema);
            Assert.Equal("customers", ordersFk.ColumnUsage[1].FkTable);
            Assert.Equal("id_customer", ordersFk.ColumnUsage[1].FkColumnName);

            AdbcTable invoices = Assert.Single(tables, t => t.Name == "invoices");
            AdbcConstraint invoicesFk = Assert.Single(invoices.Constraints!);
            Assert.Equal("fk_shared", invoicesFk.Name);
            Assert.Equal("FOREIGN KEY", invoicesFk.Type);
            Assert.Equal(new[] { "account_id" }, invoicesFk.ColumnNames);
            AdbcUsageSchema invoiceUsage = Assert.Single(invoicesFk.ColumnUsage);
            Assert.Equal("other-project", invoiceUsage.FkCatalog);
            Assert.Equal("other_dataset", invoiceUsage.FkDbSchema);
            Assert.Equal("accounts", invoiceUsage.FkTable);
            Assert.Equal("acct_id", invoiceUsage.FkColumnName);

            Assert.Empty(mockServer.ExecutedQueries);
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
