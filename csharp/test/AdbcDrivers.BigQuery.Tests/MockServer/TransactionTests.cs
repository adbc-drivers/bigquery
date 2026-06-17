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
using Apache.Arrow.Adbc;
using AdbcDrivers.BigQuery.MockServer;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests.MockServer
{
    [Trait("Category", "MockServer")]
    public class TransactionTests
    {
        private static (BigQueryMockServer server, AdbcConnection connection) CreateMockConnection()
        {
            var mockServer = new BigQueryMockServer();
            string projectId = "mock-project";
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, projectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            var driver = new BigQueryDriver();
            AdbcDatabase database = driver.Open(parameters);
            AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            return (mockServer, connection);
        }

        [Fact]
        public void AutoCommit_IsEnabledByDefault()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            using (connection)
            {
                Assert.True(connection.AutoCommit);
            }
        }

        [Fact]
        public void AutoCommit_CanBeDisabled()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            using (connection)
            {
                connection.AutoCommit = false;

                Assert.False(connection.AutoCommit);

                // Should have executed: SELECT 1 (create session) and BEGIN TRANSACTION
                Assert.Contains(server.ExecutedQueries, q => q == "SELECT 1");
                Assert.Contains(server.ExecutedQueries, q => q == "BEGIN TRANSACTION");
            }
        }

        [Fact]
        public void AutoCommit_CanBeReEnabled()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            using (connection)
            {
                connection.AutoCommit = false;
                connection.AutoCommit = true;

                Assert.True(connection.AutoCommit);

                // Should have executed COMMIT TRANSACTION and CALL BQ.ABORT_SESSION
                Assert.Contains(server.ExecutedQueries, q => q == "COMMIT TRANSACTION");
                Assert.Contains(server.ExecutedQueries, q => q.StartsWith("CALL BQ.ABORT_SESSION"));
            }
        }

        [Fact]
        public void AutoCommit_DisableWhenAlreadyDisabled_Throws()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            using (connection)
            {
                connection.AutoCommit = false;

                var ex = Assert.Throws<AdbcException>(() => connection.AutoCommit = false);
                Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
            }
        }

        [Fact]
        public void AutoCommit_EnableWhenAlreadyEnabled_Throws()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            using (connection)
            {
                var ex = Assert.Throws<AdbcException>(() => connection.AutoCommit = true);
                Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
            }
        }

        [Fact]
        public void Commit_WhenAutocommitEnabled_Throws()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            using (connection)
            {
                var ex = Assert.Throws<AdbcException>(() => connection.Commit());
                Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
            }
        }

        [Fact]
        public void Rollback_WhenAutocommitEnabled_Throws()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            using (connection)
            {
                var ex = Assert.Throws<AdbcException>(() => connection.Rollback());
                Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
            }
        }

        [Fact]
        public void Commit_ExecutesCommitAndBeginTransaction()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            using (connection)
            {
                connection.AutoCommit = false;

                // Clear the queries recorded during setup
                var queriesBefore = server.ExecutedQueries.Count;

                connection.Commit();

                // Get only the new queries after commit
                var newQueries = server.ExecutedQueries.Skip(queriesBefore).ToList();
                Assert.Equal(2, newQueries.Count);
                Assert.Equal("COMMIT TRANSACTION", newQueries[0]);
                Assert.Equal("BEGIN TRANSACTION", newQueries[1]);
            }
        }

        [Fact]
        public void Rollback_ExecutesRollbackAndBeginTransaction()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            using (connection)
            {
                connection.AutoCommit = false;

                var queriesBefore = server.ExecutedQueries.Count;

                connection.Rollback();

                var newQueries = server.ExecutedQueries.Skip(queriesBefore).ToList();
                Assert.Equal(2, newQueries.Count);
                Assert.Equal("ROLLBACK TRANSACTION", newQueries[0]);
                Assert.Equal("BEGIN TRANSACTION", newQueries[1]);
            }
        }

        [Fact]
        public void Dispose_WithActiveSession_AbortsSession()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            {
                connection.AutoCommit = false;
                connection.Dispose();

                Assert.Contains(server.ExecutedQueries, q => q.StartsWith("CALL BQ.ABORT_SESSION"));
            }
        }

        [Fact]
        public void Dispose_WithoutActiveSession_DoesNotAbortSession()
        {
            var (server, connection) = CreateMockConnection();
            using (server)
            {
                connection.Dispose();

                Assert.DoesNotContain(server.ExecutedQueries, q => q.StartsWith("CALL BQ.ABORT_SESSION"));
            }
        }
    }
}

#endif
