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
using AdbcDrivers.BigQuery.MockServer;
using Apache.Arrow.Adbc;
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
        public void GetTableSchemaUsesRestApiInsteadOfInterpolatingSql()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            connection.GetTableSchema(ProjectId, DbSchema, Payload);

            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.TableGet));
        }

        [Fact]
        public void GetObjectsBindsTableAndColumnPatterns()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            connection.GetObjects(
                AdbcConnection.GetObjectsDepth.All,
                catalogPattern: ProjectId,
                dbSchemaPattern: DbSchema,
                tableNamePattern: Payload,
                tableTypes: new List<string> { "BASE TABLE" },
                columnNamePattern: Payload);

            Assert.NotEmpty(mockServer.ExecutedQueries);
            foreach (string executed in mockServer.ExecutedQueries)
            {
                Assert.DoesNotContain("DROP TABLE secrets", executed);
            }

            string tablesQuery = mockServer.ExecutedQueries.First(q => q.Contains("INFORMATION_SCHEMA.TABLES"));
            Assert.Contains("table_name LIKE @tableNamePattern", tablesQuery);

            // An array parameter with IN UNNEST; BigQuery will not expand one into an IN list.
            Assert.Contains("table_type IN UNNEST(@tableTypes)", tablesQuery);

            IReadOnlyList<QueryParameter> parameters =
                mockServer.ExecutedQueryParameters[mockServer.ExecutedQueries.ToList().IndexOf(tablesQuery)];
            QueryParameter pattern = parameters.Single(p => p.Name == "tableNamePattern");
            Assert.Equal(Payload, pattern.ParameterValue.Value);

            QueryParameter types = parameters.Single(p => p.Name == "tableTypes");
            Assert.Equal("ARRAY", types.ParameterType.Type);
            Assert.Equal(
                new[] { "BASE TABLE" },
                types.ParameterValue.ArrayValues.Select(v => v.Value).ToArray());
        }

        [Theory]
        // A backtick would close a quoted identifier.
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
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.TableGet));
        }

        [Fact]
        public void InvalidDbSchemaIsRejectedBeforeAnyQueryRuns()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            Assert.Throws<AdbcException>(
                () => connection.GetTableSchema(ProjectId, "mock_dataset` UNION ALL SELECT 1 --", "some_table"));

            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.TableGet));
        }

        [Fact]
        public void ValidNamesReachRestApi()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            connection.GetTableSchema("mock-project", "mock_dataset", "my_table");

            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.TableGet));
        }

        [Fact]
        public void TableNamesWithUnicodeAndSpacesReachRestApi()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            // BigQuery table names may use any Unicode letter, mark, number, connector, dash or
            // space. None of these survive an ASCII identifier allowlist.
            const string tableName = "étudiant-01 お客様";
            connection.GetTableSchema(ProjectId, DbSchema, tableName);

            Assert.Empty(mockServer.ExecutedQueries);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.TableGet));
        }

        private static AdbcConnection Connect(BigQueryMockServer mockServer)
        {
            var driver = new BigQueryDriver();
            AdbcDatabase database = driver.Open(new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, ProjectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            });

            return database.Connect(new Dictionary<string, string>());
        }
    }
}

#endif
