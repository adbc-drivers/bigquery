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
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using AdbcDrivers.BigQuery.MockServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Json;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests.MockServer
{
    /// <summary>
    /// End-to-end retry coverage against <see cref="BigQueryMockServer"/>. BigQueryUtilsTests
    /// already covers how <c>IsRetryableException</c> classifies individual exceptions; these tests
    /// cover the wiring around it - that a transient REST failure really does make the driver
    /// reissue the call, that exhaustion stops where it was configured to, and that a non-retryable
    /// failure is not retried at all.
    /// </summary>
    /// <remarks>
    /// There are two retry layers here, and picking the injected error carelessly measures the
    /// wrong one. Google.Apis installs its own back-off handler that reissues a failed request up
    /// to three times inside the HTTP client, keyed on the BigQuery error reason (plus 502 and 503
    /// by status). Its retryable reasons overlap the driver's, so injecting a realistic
    /// "rateLimitExceeded" or "backendError" gets absorbed and retried below the driver: with
    /// driver retries switched off entirely, one injected 403, 429, 500 or 503 carrying those
    /// reasons still produces two requests and a successful query. Asserting on request counts
    /// there would pass whether or not <c>RetryManager</c> works.
    /// <para>
    /// So these tests pair a 500 - which the driver retries on status alone - with a reason the
    /// HTTP handler does not recognise. That combination is retried by the driver and ignored by
    /// the HTTP client, so every recorded request is exactly one driver attempt, and no test waits
    /// on a back-off it cannot configure. The pairing is synthetic; realistic status/reason
    /// combinations are covered by NonRetryableErrorFailsWithoutRetrying and by the classification
    /// tests in BigQueryUtilsTests.
    /// </para>
    /// </remarks>
    [Trait("Category", "MockServer")]
    public class RetryTests
    {
        private const string ProjectId = "mock-project";

        // Retryable to the driver on status alone (>= 500).
        private const HttpStatusCode RetryableStatus = HttpStatusCode.InternalServerError;

        // Not in the Google.Apis handler's retryable-reason set, so it does not retry underneath
        // us. See the class remarks for why that matters.
        private const string InertReason = "invalidQuery";

        [Theory]
        [InlineData(HttpStatusCode.BadRequest, "invalidQuery")]
        [InlineData(HttpStatusCode.NotFound, "notFound")]
        public void NonRetryableErrorFailsWithoutRetrying(HttpStatusCode status, string reason)
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.QueueError(MockRequestKind.JobInsert, status, reason);

            using StatementScope scope = OpenStatement(mockServer);
            scope.Statement.SqlQuery = "SELECT not_a_column";

            Assert.ThrowsAny<Exception>(() => scope.Statement.ExecuteQuery());

            // Neither layer retries these, so failing fast preserves the original error.
            Assert.Equal(1, mockServer.CountOfKind(MockRequestKind.JobInsert));
        }

        [Theory]
        [InlineData(0, 1)]
        [InlineData(1, 2)]
        [InlineData(2, 3)]
        public void RetryExhaustionStopsAtTheConfiguredAttemptCount(int maxRetries, int expectedAttempts)
        {
            const int queuedErrors = 10;

            using var mockServer = new BigQueryMockServer();

            // More errors than the driver will ever consume, so exhaustion is what ends the run
            // rather than the queue running dry.
            for (int i = 0; i < queuedErrors; i++)
            {
                mockServer.QueueError(MockRequestKind.JobInsert, RetryableStatus, InertReason);
            }

            using StatementScope scope = OpenStatement(mockServer, maxRetries);
            scope.Statement.SqlQuery = "SELECT 42 AS value";

            AdbcException exception = Assert.Throws<AdbcException>(() => scope.Statement.ExecuteQuery());

            // Only RetryManager produces this message.
            Assert.Contains($"{expectedAttempts} attempt(s)", exception.Message);

            // One request per driver attempt, since the HTTP layer leaves this error alone.
            Assert.Equal(expectedAttempts, mockServer.CountOfKind(MockRequestKind.JobInsert));
            Assert.Equal(queuedErrors - expectedAttempts, mockServer.PendingErrorCount(MockRequestKind.JobInsert));
        }

        [Fact]
        public async Task TransientErrorIsRetriedAndSucceeds()
        {
            using var mockServer = new BigQueryMockServer();
            ConfigureSingleRowResults(mockServer);
            mockServer.QueueError(MockRequestKind.JobInsert, RetryableStatus, InertReason);

            using StatementScope scope = OpenStatement(mockServer);
            scope.Statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = scope.Statement.ExecuteQuery();

            await AssertSingleRowAsync(result);
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.JobInsert));
            Assert.Equal(0, mockServer.PendingErrorCount(MockRequestKind.JobInsert));

            // The failed insert created no job, so only the retry carries an id.
            IReadOnlyList<MockRequest> inserts = mockServer.RequestsOfKind(MockRequestKind.JobInsert);
            Assert.Null(inserts[0].JobId);
            Assert.NotNull(inserts[1].JobId);
        }

        [Fact]
        public async Task RetryReissuesTheWholeGetResultsSequence()
        {
            using var mockServer = new BigQueryMockServer();
            ConfigureSingleRowResults(mockServer);

            // The driver wraps jobs.get and jobs.getQueryResults in one retryable unit, so a
            // driver-level retry reissues jobs.get as well - something retrying just the failed
            // request could never produce.
            mockServer.QueueError(MockRequestKind.QueryResults, RetryableStatus, InertReason);

            using StatementScope scope = OpenStatement(mockServer);
            scope.Statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = scope.Statement.ExecuteQuery();

            await AssertSingleRowAsync(result);
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.JobGet));
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.QueryResults));
            Assert.Equal(0, mockServer.PendingErrorCount(MockRequestKind.QueryResults));
        }

        [Fact]
        public async Task QueuedErrorsAreConsumedInOrderByMatchingRequests()
        {
            using var mockServer = new BigQueryMockServer();
            using var httpClient = new HttpClient();

            mockServer.QueueError(MockRequestKind.JobInsert, RetryableStatus, InertReason);
            mockServer.QueueError(MockRequestKind.JobInsert, HttpStatusCode.BadRequest, "invalidQuery");
            Assert.Equal(2, mockServer.PendingErrorCount(MockRequestKind.JobInsert));

            using (HttpResponseMessage first = await PostJobAsync(mockServer, httpClient))
            {
                Assert.Equal(RetryableStatus, first.StatusCode);
            }

            using (HttpResponseMessage second = await PostJobAsync(mockServer, httpClient))
            {
                Assert.Equal(HttpStatusCode.BadRequest, second.StatusCode);
            }

            // Queue drained, so the third insert behaves normally.
            using (HttpResponseMessage third = await PostJobAsync(mockServer, httpClient))
            {
                Assert.Equal(HttpStatusCode.OK, third.StatusCode);
            }

            Assert.Equal(0, mockServer.PendingErrorCount(MockRequestKind.JobInsert));

            // Every attempt is recorded, but only the one that created a job carries an id.
            IReadOnlyList<MockRequest> inserts = mockServer.RequestsOfKind(MockRequestKind.JobInsert);
            Assert.Equal(3, inserts.Count);
            Assert.Null(inserts[0].JobId);
            Assert.Null(inserts[1].JobId);
            Assert.NotNull(inserts[2].JobId);

            // A failed insert never reaches the point of recording the query.
            Assert.Single(mockServer.ExecutedQueries);
        }

        [Fact]
        public async Task QueuedErrorsDoNotConsumeScriptedJobStates()
        {
            using var mockServer = new BigQueryMockServer();
            using var httpClient = new HttpClient();

            mockServer.ScriptJobStates(BigQueryMockServer.JobStateRunning, BigQueryMockServer.JobStateDone);

            string jobId;
            using (HttpResponseMessage inserted = await PostJobAsync(mockServer, httpClient))
            {
                Job? job = NewtonsoftJsonSerializer.Instance.Deserialize<Job>(
                    await inserted.Content.ReadAsStringAsync());
                jobId = job!.JobReference!.JobId;
            }

            mockServer.QueueError(MockRequestKind.JobGet, RetryableStatus, InertReason);

            string url = $"http://{mockServer.RestEndpoint}/bigquery/v2/projects/{ProjectId}/jobs/{jobId}";
            using (HttpResponseMessage failed = await httpClient.GetAsync(url))
            {
                Assert.Equal(RetryableStatus, failed.StatusCode);
            }

            // The failed poll must not have advanced the script, so RUNNING is still pending.
            Assert.Equal(BigQueryMockServer.JobStateRunning, await GetJobStateAsync(httpClient, url));
            Assert.Equal(BigQueryMockServer.JobStateDone, await GetJobStateAsync(httpClient, url));
        }

        [Theory]
        [InlineData(MockRequestKind.TableGet)]
        [InlineData(MockRequestKind.TableInsert)]
        [InlineData(MockRequestKind.TableDelete)]
        public void QueueErrorRejectsTableKinds(MockRequestKind kind)
        {
            using var mockServer = new BigQueryMockServer();

            Assert.Throws<ArgumentException>(
                () => mockServer.QueueError(kind, HttpStatusCode.InternalServerError));
        }

        private static async Task<HttpResponseMessage> PostJobAsync(BigQueryMockServer mockServer, HttpClient httpClient)
        {
            var jobRequest = new Job
            {
                Configuration = new JobConfiguration
                {
                    Query = new JobConfigurationQuery { Query = "SELECT 1", UseLegacySql = false },
                },
            };

            using var content = new StringContent(
                NewtonsoftJsonSerializer.Instance.Serialize(jobRequest), Encoding.UTF8, "application/json");
            return await httpClient.PostAsync(
                $"http://{mockServer.RestEndpoint}/bigquery/v2/projects/{ProjectId}/jobs", content);
        }

        private static async Task<string?> GetJobStateAsync(HttpClient httpClient, string url)
        {
            Job? job = NewtonsoftJsonSerializer.Instance.Deserialize<Job>(await httpClient.GetStringAsync(url));
            return job?.Status?.State;
        }

        private static StatementScope OpenStatement(BigQueryMockServer mockServer, int? maxRetries = null)
        {
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, ProjectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },

                // Keep the driver back-off out of the test runtime; the attempt count is what
                // matters here, not the delay between attempts.
                { BigQueryParameters.RetryDelayMs, "1" },
            };

            if (maxRetries.HasValue)
            {
                parameters[BigQueryParameters.MaximumRetryAttempts] = maxRetries.Value.ToString();
            }

            var driver = new BigQueryDriver();
            AdbcDatabase database = driver.Open(parameters);
            AdbcConnection connection = database.Connect(new Dictionary<string, string>());

            return new StatementScope(driver, database, connection);
        }

        private static async Task AssertSingleRowAsync(QueryResult result)
        {
            Assert.NotNull(result.Stream);
            using (result.Stream)
            {
                using RecordBatch? batch = await result.Stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(42L, Assert.IsType<Int64Array>(batch!.Column(0)).GetValue(0));
            }
        }

        private static void ConfigureSingleRowResults(BigQueryMockServer mockServer)
        {
            var schema = new Schema(new[] { new Field("value", Int64Type.Default, nullable: true) }, null);

            var builder = new Int64Array.Builder();
            builder.Append(42);
            using var batch = new RecordBatch(schema, new IArrowArray[] { builder.Build() }, 1);

            mockServer.ReadService.DefaultArrowSchema = ArrowSerializationHelpers.SerializeSchema(schema);
            mockServer.ReadService.DefaultArrowBatch = ArrowSerializationHelpers.SerializeRecordBatch(batch);
            mockServer.ReadService.DefaultRowCount = 1;
        }

        /// <summary>
        /// Owns a driver, database, connection and statement for one test, and disposes them
        /// innermost-first. Holding the statement here rather than handing it back alongside a
        /// separate scope is what keeps that order right: two separate <c>using</c>s at the call
        /// site are easy to write in the order that tears the connection down first.
        /// </summary>
        private sealed class StatementScope : IDisposable
        {
            private readonly BigQueryDriver _driver;
            private readonly AdbcDatabase _database;
            private readonly AdbcConnection _connection;

            public StatementScope(BigQueryDriver driver, AdbcDatabase database, AdbcConnection connection)
            {
                _driver = driver;
                _database = database;
                _connection = connection;
                Statement = connection.CreateStatement();
            }

            public AdbcStatement Statement { get; }

            public void Dispose()
            {
                Statement.Dispose();
                _connection.Dispose();
                _database.Dispose();
                _driver.Dispose();
            }
        }
    }
}

#endif
