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
using System.Linq;
using Apache.Arrow.Adbc;
using AdbcDrivers.BigQuery.MockServer;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests.MockServer
{
    [Trait("Category", "MockServer")]
    public class TransactionTests
    {
        [Fact]
        public void AutoCommit_IsEnabledByDefault()
        {
            using (var resource = new BigQueryResource())
            {
                Assert.True(resource.Connection.AutoCommit);
            }
        }

        [Fact]
        public void AutoCommit_CanBeDisabled()
        {
            using (var resource = new BigQueryResource())
            {
                resource.Connection.AutoCommit = false;

                Assert.False(resource.Connection.AutoCommit);

                // Should have executed: SELECT 1 (create session) and BEGIN TRANSACTION
                Assert.Contains(resource.Server.ExecutedQueries, q => q == "SELECT 1");
                Assert.Contains(resource.Server.ExecutedQueries, q => q == "BEGIN TRANSACTION");
            }
        }

        [Fact]
        public void AutoCommit_CanBeReEnabled()
        {
            using (var resource = new BigQueryResource())
            {
                resource.Connection.AutoCommit = false;
                resource.Connection.AutoCommit = true;

                Assert.True(resource.Connection.AutoCommit);

                // Should have executed COMMIT TRANSACTION and CALL BQ.ABORT_SESSION
                Assert.Contains(resource.Server.ExecutedQueries, q => q == "COMMIT TRANSACTION");
                Assert.Contains(resource.Server.ExecutedQueries, q => q.StartsWith("CALL BQ.ABORT_SESSION"));
            }
        }

        [Fact]
        public void AutoCommit_DisableWhenAlreadyDisabled_Throws()
        {
            using (var resource = new BigQueryResource())
            {
                resource.Connection.AutoCommit = false;

                var ex = Assert.Throws<AdbcException>(() => resource.Connection.AutoCommit = false);
                Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
            }
        }

        [Fact]
        public void AutoCommit_EnableWhenAlreadyEnabled_Throws()
        {
            using (var resource = new BigQueryResource())
            {
                var ex = Assert.Throws<AdbcException>(() => resource.Connection.AutoCommit = true);
                Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
            }
        }

        [Fact]
        public void Commit_WhenAutocommitEnabled_Throws()
        {
            using (var resource = new BigQueryResource())
            {
                var ex = Assert.Throws<AdbcException>(() => resource.Connection.Commit());
                Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
            }
        }

        [Fact]
        public void Rollback_WhenAutocommitEnabled_Throws()
        {
            using (var resource = new BigQueryResource())
            {
                var ex = Assert.Throws<AdbcException>(() => resource.Connection.Rollback());
                Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
            }
        }

        [Fact]
        public void Commit_ExecutesCommitAndBeginTransaction()
        {
            using (var resource = new BigQueryResource())
            {
                resource.Connection.AutoCommit = false;

                // Clear the queries recorded during setup
                var queriesBefore = resource.Server.ExecutedQueries.Count;

                resource.Connection.Commit();

                // Get only the new queries after commit
                var newQueries = resource.Server.ExecutedQueries.Skip(queriesBefore).ToList();
                Assert.Equal(2, newQueries.Count);
                Assert.Equal("COMMIT TRANSACTION", newQueries[0]);
                Assert.Equal("BEGIN TRANSACTION", newQueries[1]);
            }
        }

        [Fact]
        public void Rollback_ExecutesRollbackAndBeginTransaction()
        {
            using (var resource = new BigQueryResource())
            {
                resource.Connection.AutoCommit = false;

                var queriesBefore = resource.Server.ExecutedQueries.Count;

                resource.Connection.Rollback();

                var newQueries = resource.Server.ExecutedQueries.Skip(queriesBefore).ToList();
                Assert.Equal(2, newQueries.Count);
                Assert.Equal("ROLLBACK TRANSACTION", newQueries[0]);
                Assert.Equal("BEGIN TRANSACTION", newQueries[1]);
            }
        }

        [Fact]
        public void Dispose_WithActiveSession_AbortsSession()
        {
            using (var resource = new BigQueryResource())
            {
                resource.Connection.AutoCommit = false;
                resource.Connection.Dispose();

                Assert.Contains(resource.Server.ExecutedQueries, q => q.StartsWith("CALL BQ.ABORT_SESSION"));
            }
        }

        [Fact]
        public void Dispose_WithoutActiveSession_DoesNotAbortSession()
        {
            using (var resource = new BigQueryResource())
            {
                resource.Connection.Dispose();

                Assert.DoesNotContain(resource.Server.ExecutedQueries, q => q.StartsWith("CALL BQ.ABORT_SESSION"));
            }
        }

        class BigQueryResource : IDisposable
        {
            private readonly BigQueryMockServer _server;
            private readonly AdbcDatabase _database;
            private readonly AdbcConnection _connection;

            public BigQueryResource()
            {
                _server = new BigQueryMockServer();
                string projectId = "mock-project";
                var parameters = new Dictionary<string, string>
                {
                    { BigQueryParameters.ProjectId, projectId },
                    { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                    { BigQueryParameters.TestRestEndpoint, _server.RestEndpoint },
                    { BigQueryParameters.TestStorageEndpoint, _server.GrpcEndpoint },
                };

                var driver = new BigQueryDriver();
                _database = driver.Open(parameters);
                _connection = _database.Connect(new Dictionary<string, string>());
            }

            public BigQueryMockServer Server => _server;
            public AdbcConnection Connection => _connection;

            public void Dispose()
            {
                _connection.Dispose();
                _database.Dispose();
                _server.Dispose();
            }
        }
    }
}

#endif
