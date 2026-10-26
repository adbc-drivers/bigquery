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
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
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
    /// Cancellation coverage that runs entirely against <see cref="BigQueryMockServer"/>, so it
    /// needs no BigQuery credentials and runs on every PR. The credentialed cancel tests in
    /// StatementTests assert only that an OperationCanceledException surfaces; the driver calls
    /// Job.CancelAsync from a catch block that swallows its own failures, so these tests assert
    /// that the driver actually asked the server to stop the job.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class JobCancellationTests
    {
        private const string ProjectId = "mock-project";

        [Fact]
        public async Task ExecuteQueryCancellationRequestsJobCancel()
        {
            using var mockServer = new BigQueryMockServer();

            // The job never reports DONE, so getQueryResults keeps long-polling until we cancel.
            mockServer.ScriptJobStates(BigQueryMockServer.JobStateRunning);

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            // Cancel while a getQueryResults call is in flight - the window the driver's job-cancel
            // path is written for.
            CancelOnRequest(mockServer, MockRequestKind.QueryResults, occurrence: 1, statement);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteQuery()));

            MockRequest insert = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            MockRequest cancel = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
            Assert.Equal(insert.JobId, cancel.JobId);
            Assert.True(cancel.Sequence > insert.Sequence, "jobs.cancel should follow the job insert");
        }

        [Fact]
        public async Task ExecuteQueryCancellationDuringJobInsertCancelsJob()
        {
            using var mockServer = new BigQueryMockServer();

            // The job would complete immediately, so a dropped cancel lets the query succeed.
            ConfigureSingleRowResults(mockServer);

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            CancelOnRequest(mockServer, MockRequestKind.JobInsert, occurrence: 1, statement);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteQuery()));

            // The insert never returned, so the driver can only cancel the job by the id it chose.
            MockRequest insert = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            MockRequest cancel = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
            Assert.Equal(insert.JobId, cancel.JobId);
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.JobGet));
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.QueryResults));
        }

        [Fact]
        public async Task ExecuteUpdateCancellationDuringJobInsertCancelsJob()
        {
            using var mockServer = new BigQueryMockServer();

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "UPDATE test_table SET value = 1";

            CancelOnRequest(mockServer, MockRequestKind.JobInsert, occurrence: 1, statement);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteUpdate()));

            MockRequest insert = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            MockRequest cancel = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
            Assert.Equal(insert.JobId, cancel.JobId);
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.JobGet));
        }

        [Fact]
        public async Task ExecuteQueryCancellationRetriesJobCancelWhileJobIsNotFound()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.ScriptJobStates(BigQueryMockServer.JobStateRunning);

            // Models a cancel that reaches BigQuery before the interrupted insert does.
            mockServer.QueueError(MockRequestKind.JobCancel, HttpStatusCode.NotFound, "notFound");

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            CancelOnRequest(mockServer, MockRequestKind.QueryResults, occurrence: 1, statement);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteQuery()));

            MockRequest insert = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            IReadOnlyList<MockRequest> cancels = mockServer.RequestsOfKind(MockRequestKind.JobCancel);
            Assert.Equal(2, cancels.Count);
            Assert.All(cancels, cancel => Assert.Equal(insert.JobId, cancel.JobId));
            Assert.Equal(0, mockServer.PendingErrorCount(MockRequestKind.JobCancel));
        }

        [Fact]
        public async Task ExecuteQueryCancellationDuringRetryBackOffCancelsJob()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.ScriptJobStates(BigQueryMockServer.JobStateRunning);

            // A retryable failure of the first jobs.get makes the driver wait before retrying. The client
            // library retries 5xx and rateLimitExceeded itself, inside the attempt, so use a 429 that only
            // the driver retries.
            mockServer.QueueError(MockRequestKind.JobGet, HttpStatusCode.TooManyRequests, "tooManyRequests");

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer, LongRetryDelay));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            CancelAfterRequest(mockServer, MockRequestKind.JobGet, occurrence: 1, CancelDelay, statement);

            Stopwatch stopwatch = Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteQuery()));
            stopwatch.Stop();

            Assert.True(stopwatch.Elapsed < LongRetryDelayTime, $"cancel should interrupt the retry delay, took {stopwatch.Elapsed}");
            MockRequest insert = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            MockRequest cancel = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
            Assert.Equal(insert.JobId, cancel.JobId);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobGet));
        }

        [Fact]
        public async Task ExecuteUpdateCancellationDuringRetryBackOffCancelsJob()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.ScriptJobStates(BigQueryMockServer.JobStateRunning);
            mockServer.QueueError(MockRequestKind.JobGet, HttpStatusCode.TooManyRequests, "tooManyRequests");

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer, LongRetryDelay));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "UPDATE test_table SET value = 1";

            CancelAfterRequest(mockServer, MockRequestKind.JobGet, occurrence: 1, CancelDelay, statement);

            Stopwatch stopwatch = Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteUpdate()));
            stopwatch.Stop();

            Assert.True(stopwatch.Elapsed < LongRetryDelayTime, $"cancel should interrupt the retry delay, took {stopwatch.Elapsed}");
            MockRequest insert = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            MockRequest cancel = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
            Assert.Equal(insert.JobId, cancel.JobId);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobGet));
        }

        [Fact]
        public async Task ExecuteQueryCancellationAfterTokenRefreshUsesNewCredentials()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.ScriptJobStates(BigQueryMockServer.JobStateRunning);

            // The first jobs.get is rejected as unauthenticated, so the driver refreshes its token,
            // which replaces the client, and then waits before retrying.
            mockServer.QueueError(MockRequestKind.JobGet, HttpStatusCode.Unauthorized, "authError");

            Dictionary<string, string> parameters = CreateParameters(mockServer, LongRetryDelay);
            parameters[BigQueryParameters.AccessToken] = "token-1";

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(parameters);
            using var connection = (BigQueryConnection)database.Connect(new Dictionary<string, string>());
            connection.UpdateToken = () => Task.Run(() => connection.SetOption(BigQueryParameters.AccessToken, "token-2"));
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            CancelAfterRequest(mockServer, MockRequestKind.JobGet, occurrence: 1, CancelDelay, statement);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteQuery()));

            MockRequest insert = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            Assert.Equal("token-1", insert.AccessToken);
            MockRequest cancel = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
            Assert.Equal(insert.JobId, cancel.JobId);
            Assert.Equal("token-2", cancel.AccessToken);
        }

        [Fact]
        public async Task ExecuteQueryCancellationDuringJobInsertCancelsRegionalJob()
        {
            // No location is configured, so BigQuery picks a single region and jobs.cancel needs it.
            using var mockServer = new BigQueryMockServer { InferredJobLocation = "asia-northeast1" };
            ConfigureSingleRowResults(mockServer);

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            CancelOnRequest(mockServer, MockRequestKind.JobInsert, occurrence: 1, statement);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteQuery()));

            // The first jobs.cancel cannot find the job without its location; the driver finds the
            // location with jobs.list and cancels again.
            MockRequest insert = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            Assert.True(mockServer.IsJobCancelled(insert.JobId!), "the job should have been cancelled");
            Assert.NotEmpty(mockServer.RequestsOfKind(MockRequestKind.JobList));
            Assert.All(mockServer.RequestsOfKind(MockRequestKind.JobCancel), cancel => Assert.Equal(insert.JobId, cancel.JobId));
        }

        [Fact]
        public async Task ExecuteQueryUsesExistingRegionalJobWhenInsertResponseIsLost()
        {
            using var mockServer = new BigQueryMockServer { InferredJobLocation = "asia-northeast1" };
            ConfigureSingleRowResults(mockServer);
            mockServer.LoseNextInsertResponse();

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = statement.ExecuteQuery();

            Assert.NotNull(result.Stream);
            using (result.Stream)
            {
                using RecordBatch? batch = await result.Stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(42L, Assert.IsType<Int64Array>(batch!.Column(0)).GetValue(0));
            }

            // jobs.get without the location cannot find the job, so the driver finds it with jobs.list.
            IReadOnlyList<MockRequest> inserts = mockServer.RequestsOfKind(MockRequestKind.JobInsert);
            Assert.Equal(2, inserts.Count);
            Assert.Equal(inserts[0].JobId, inserts[1].JobId);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobList));
        }

        [Fact]
        public async Task ExecuteQueryUsesExistingJobWhenInsertResponseIsLost()
        {
            using var mockServer = new BigQueryMockServer();
            ConfigureSingleRowResults(mockServer);
            mockServer.LoseNextInsertResponse();

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = statement.ExecuteQuery();

            Assert.NotNull(result.Stream);
            using (result.Stream)
            {
                using RecordBatch? batch = await result.Stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(42L, Assert.IsType<Int64Array>(batch!.Column(0)).GetValue(0));
            }

            // The retried insert reuses the job id and is rejected as a duplicate; the driver then
            // continues with the job the first insert created.
            IReadOnlyList<MockRequest> inserts = mockServer.RequestsOfKind(MockRequestKind.JobInsert);
            Assert.Equal(2, inserts.Count);
            Assert.Equal(inserts[0].JobId, inserts[1].JobId);
            Assert.All(mockServer.RequestsOfKind(MockRequestKind.JobGet), get => Assert.Equal(inserts[0].JobId, get.JobId));
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
        }

        [Fact]
        public void ExecuteUpdateUsesExistingJobWhenInsertResponseIsLost()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.LoseNextInsertResponse();

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "UPDATE test_table SET value = 1";

            UpdateResult result = statement.ExecuteUpdate();

            Assert.Equal(2, result.AffectedRows);
            IReadOnlyList<MockRequest> inserts = mockServer.RequestsOfKind(MockRequestKind.JobInsert);
            Assert.Equal(2, inserts.Count);
            Assert.Equal(inserts[0].JobId, inserts[1].JobId);

            // Setting the job id means passing QueryOptions where none were passed before; that must
            // not change the SQL dialect.
            Assert.False(mockServer.LastInsertedJob?.Configuration?.Query?.UseLegacySql);
        }

        [Fact]
        public async Task ExecuteQueryCancellationDuringJobsQueryStopsQuery()
        {
            using var mockServer = new BigQueryMockServer { OptionalQueryCreatesJob = true };
            ConfigureSingleRowResults(mockServer);

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(BigQueryParameters.UseJobCreationMode, "true");
            statement.SqlQuery = "SELECT 42 AS value";

            CancelOnRequest(mockServer, MockRequestKind.JobQuery, occurrence: 1, statement);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteQuery()));

            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobQuery));
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.JobGet));
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.QueryResults));
        }

        [Fact]
        public async Task ExecuteQueryCancellationAfterJobsQueryFallbackRequestsJobCancel()
        {
            // jobs.query returns only a job reference, so the driver falls back to polling a job
            // it has not yet fetched.
            using var mockServer = new BigQueryMockServer { OptionalQueryCreatesJob = true };
            mockServer.ScriptJobStates(BigQueryMockServer.JobStateRunning);

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(BigQueryParameters.UseJobCreationMode, "true");
            statement.SqlQuery = "SELECT 42 AS value";

            // Cancel during the first jobs.get, before the driver has a BigQueryJob for the
            // reference; the job must still be cancelled by its reference.
            CancelOnRequest(mockServer, MockRequestKind.JobGet, occurrence: 1, statement);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteQuery()));

            MockRequest query = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobQuery));
            MockRequest cancel = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
            Assert.Equal(query.JobId, cancel.JobId);
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.QueryResults));
        }

        [Fact]
        public async Task ExecuteUpdateCancellationRequestsJobCancel()
        {
            using var mockServer = new BigQueryMockServer();

            // ExecuteUpdate polls jobs.get through PollUntilCompletedAsync rather than
            // getQueryResults, so it needs its own coverage.
            mockServer.ScriptJobStates(BigQueryMockServer.JobStateRunning);

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "UPDATE test_table SET value = 1";

            // The first jobs.get is the driver refreshing the job; cancel on the second, once
            // PollUntilCompletedAsync is polling.
            CancelOnRequest(mockServer, MockRequestKind.JobGet, occurrence: 2, statement);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => Task.Run(() => statement.ExecuteUpdate()));

            MockRequest insert = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobInsert));
            MockRequest cancel = Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
            Assert.Equal(insert.JobId, cancel.JobId);
            Assert.True(mockServer.CountOfKind(MockRequestKind.JobGet) >= 2);
        }

        [Fact]
        public async Task ScriptedRunningStatesMakeTheDriverPollBeforeCompleting()
        {
            using var mockServer = new BigQueryMockServer();
            ConfigureSingleRowResults(mockServer);

            // Successive status observations report RUNNING, RUNNING, then DONE.
            mockServer.ScriptJobStates(
                BigQueryMockServer.JobStateRunning,
                BigQueryMockServer.JobStateRunning,
                BigQueryMockServer.JobStateDone);

            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(CreateParameters(mockServer));
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42 AS value";

            QueryResult result = statement.ExecuteQuery();

            Assert.NotNull(result.Stream);
            using (result.Stream)
            {
                using RecordBatch? batch = await result.Stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(42L, Assert.IsType<Int64Array>(batch!.Column(0)).GetValue(0));
            }

            // The single jobs.get consumes the first RUNNING, so the driver has to poll query
            // results twice: once more for RUNNING, then once for DONE.
            Assert.Equal(MockRequestKind.JobInsert, mockServer.Requests[0].Kind);
            Assert.Equal(MockRequestKind.JobGet, mockServer.Requests[1].Kind);
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.QueryResults));
            Assert.Empty(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
        }

        [Fact]
        public async Task JobsCancelReturnsStoppedJobResource()
        {
            using var mockServer = new BigQueryMockServer();
            using var httpClient = new HttpClient();

            string jobId = await InsertJobAsync(mockServer, httpClient);

            using HttpResponseMessage response = await httpClient.PostAsync(CancelUri(mockServer, jobId), content: null);

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            string json = await response.Content.ReadAsStringAsync();
            JobCancelResponse? cancelResponse = NewtonsoftJsonSerializer.Instance.Deserialize<JobCancelResponse>(json);

            Assert.NotNull(cancelResponse);
            Assert.Equal("bigquery#jobCancelResponse", cancelResponse!.Kind);
            Assert.Equal(jobId, cancelResponse.Job?.JobReference?.JobId);
            Assert.Equal(BigQueryMockServer.JobStateDone, cancelResponse.Job?.Status?.State);
            Assert.Equal("stopped", cancelResponse.Job?.Status?.ErrorResult?.Reason);
        }

        [Fact]
        public async Task CancelledJobReportsStoppedStateOnSubsequentPoll()
        {
            using var mockServer = new BigQueryMockServer();
            using var httpClient = new HttpClient();

            // A job that would otherwise never complete must still report a terminal state once
            // it has been cancelled.
            mockServer.ScriptJobStates(BigQueryMockServer.JobStateRunning);
            string jobId = await InsertJobAsync(mockServer, httpClient);

            using (await httpClient.PostAsync(CancelUri(mockServer, jobId), content: null))
            {
            }

            string json = await httpClient.GetStringAsync(
                $"http://{mockServer.RestEndpoint}/bigquery/v2/projects/{ProjectId}/jobs/{jobId}");
            Job? job = NewtonsoftJsonSerializer.Instance.Deserialize<Job>(json);

            Assert.NotNull(job);
            Assert.Equal(BigQueryMockServer.JobStateDone, job!.Status?.State);
            Assert.Equal("stopped", job.Status?.ErrorResult?.Reason);
        }

        [Fact]
        public async Task JobsCancelForUnknownJobReturnsNotFound()
        {
            using var mockServer = new BigQueryMockServer();

            using var httpClient = new HttpClient();
            using HttpResponseMessage response = await httpClient.PostAsync(
                CancelUri(mockServer, "no-such-job"), content: null);

            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.JobCancel));
        }

        // Long enough that a test finishing well before it shows the cancel interrupted the delay.
        private const int LongRetryDelay = 30000;
        private static readonly TimeSpan LongRetryDelayTime = TimeSpan.FromMilliseconds(LongRetryDelay);

        // Long enough for the failed response to reach the driver, so the cancel lands in the retry delay.
        private static readonly TimeSpan CancelDelay = TimeSpan.FromMilliseconds(500);

        private static Dictionary<string, string> CreateParameters(BigQueryMockServer mockServer, int? retryDelayMs = null)
        {
            Dictionary<string, string> parameters = new()
            {
                { BigQueryParameters.ProjectId, ProjectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };
            if (retryDelayMs.HasValue)
            {
                parameters[BigQueryParameters.RetryDelayMs] = retryDelayMs.Value.ToString(CultureInfo.InvariantCulture);
            }
            return parameters;
        }

        private static string CancelUri(BigQueryMockServer mockServer, string jobId) =>
            $"http://{mockServer.RestEndpoint}/bigquery/v2/projects/{ProjectId}/jobs/{jobId}/cancel";

        /// <summary>
        /// Cancels the statement from inside the server handler for the given occurrence of a
        /// request kind, so the cancellation lands while that request is still in flight.
        /// </summary>
        private static void CancelOnRequest(
            BigQueryMockServer mockServer,
            MockRequestKind kind,
            int occurrence,
            AdbcStatement statement)
        {
            int seen = 0;
            mockServer.RequestReceived += request =>
            {
                if (request.Kind == kind && Interlocked.Increment(ref seen) == occurrence)
                {
                    statement.Cancel();
                }
            };
        }

        /// <summary>
        /// Cancels the statement a fixed time after the given occurrence of a request kind arrives,
        /// so the cancellation lands after the driver has received that request's response.
        /// </summary>
        private static void CancelAfterRequest(
            BigQueryMockServer mockServer,
            MockRequestKind kind,
            int occurrence,
            TimeSpan delay,
            AdbcStatement statement)
        {
            int seen = 0;
            mockServer.RequestReceived += request =>
            {
                if (request.Kind == kind && Interlocked.Increment(ref seen) == occurrence)
                {
                    _ = Task.Delay(delay).ContinueWith(_ => statement.Cancel(), TaskScheduler.Default);
                }
            };
        }

        /// <summary>
        /// Inserts a job straight through the REST surface so the mock has one to act on,
        /// without needing the driver to drive it to completion first.
        /// </summary>
        private static async Task<string> InsertJobAsync(BigQueryMockServer mockServer, HttpClient httpClient)
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
            using HttpResponseMessage response = await httpClient.PostAsync(
                $"http://{mockServer.RestEndpoint}/bigquery/v2/projects/{ProjectId}/jobs", content);
            response.EnsureSuccessStatusCode();

            Job? job = NewtonsoftJsonSerializer.Instance.Deserialize<Job>(
                await response.Content.ReadAsStringAsync());
            return job!.JobReference!.JobId;
        }

        private static void ConfigureSingleRowResults(BigQueryMockServer mockServer)
        {
            var schema = new Schema(new[] { new Field("value", Int64Type.Default, nullable: true) }, null);

            var builder = new Int64Array.Builder();
            builder.Append(42);
            var batch = new RecordBatch(schema, new IArrowArray[] { builder.Build() }, 1);

            mockServer.ReadService.DefaultArrowSchema = ArrowSerializationHelpers.SerializeSchema(schema);
            mockServer.ReadService.DefaultArrowBatch = ArrowSerializationHelpers.SerializeRecordBatch(batch);
            mockServer.ReadService.DefaultRowCount = 1;
        }
    }
}

#endif
