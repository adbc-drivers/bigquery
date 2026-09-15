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

        private static Dictionary<string, string> CreateParameters(BigQueryMockServer mockServer) => new()
        {
            { BigQueryParameters.ProjectId, ProjectId },
            { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
            { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
            { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
        };

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
