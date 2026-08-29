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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AdbcDrivers.BigQuery.MockServer
{
    /// <summary>
    /// A self-hosted mock BigQuery server that provides both a REST API (for jobs/queries)
    /// and a gRPC service (for the Storage Read API). Runs on random loopback ports
    /// using HTTP (not HTTPS) for test simplicity.
    /// </summary>
    public sealed class BigQueryMockServer : IDisposable
    {
        /// <summary>The BigQuery job state for a job that has not started running.</summary>
        public const string JobStatePending = "PENDING";

        /// <summary>The BigQuery job state for a job that is executing.</summary>
        public const string JobStateRunning = "RUNNING";

        /// <summary>The BigQuery job state for a job that has finished, successfully or not.</summary>
        public const string JobStateDone = "DONE";

        private readonly WebApplication _restApp;
        private readonly WebApplication _grpcApp;
        private readonly CancellationTokenSource _cts = new();
        private readonly ConcurrentDictionary<string, MockJob> _jobs = new();
        private readonly ConcurrentDictionary<string, Table> _tables = new();
        private readonly ConcurrentDictionary<string, bool> _sessions = new();
        private readonly ConcurrentQueue<string> _executedQueries = new();
        private readonly ConcurrentQueue<MockRequest> _requests = new();
        private IReadOnlyList<string> _jobStateScript = new[] { JobStateDone };
        private int _queryResultsRequestCount;
        private int _requestSequence;

        /// <summary>
        /// The REST API endpoint as host:port (e.g., "127.0.0.1:12345").
        /// Set this as the <c>adbc.bigquery.test.rest_endpoint</c> parameter.
        /// </summary>
        public string RestEndpoint { get; }

        /// <summary>
        /// The gRPC endpoint for the BigQuery Storage Read API as host:port (e.g., "127.0.0.1:12346").
        /// Set this as the <c>adbc.bigquery.test.storage_endpoint</c> parameter.
        /// </summary>
        public string GrpcEndpoint { get; }

        /// <summary>
        /// Returns the list of SQL queries that were executed against this mock server, in order.
        /// </summary>
        public IReadOnlyList<string> ExecutedQueries => _executedQueries.ToArray();

        /// <summary>
        /// The number of requests made to the query-results endpoint.
        /// </summary>
        public int QueryResultsRequestCount => _queryResultsRequestCount;

        /// <summary>
        /// Every REST request the server has handled, in arrival order.
        /// </summary>
        public IReadOnlyList<MockRequest> Requests => _requests.ToArray();

        /// <summary>
        /// Raised on the server thread as each request is recorded, before its response is produced.
        /// Lets a test act (for example, cancel a statement) while the request is still in flight.
        /// Handlers must not throw; an exception here surfaces to the driver as an HTTP 500.
        /// </summary>
        public event Action<MockRequest>? RequestReceived;

        /// <summary>
        /// The sequence of job states reported for successive status observations of a job
        /// (both <c>jobs.get</c> and <c>jobs.getQueryResults</c> advance it). The last entry repeats
        /// indefinitely, so a script of "RUNNING" models a job that never completes, and
        /// "RUNNING", "RUNNING", "DONE" models one that completes on the third observation.
        /// Applies to jobs created after it is set; defaults to a single "DONE".
        /// </summary>
        public IReadOnlyList<string> JobStateScript
        {
            get => _jobStateScript;
            set
            {
                if (value == null || value.Count == 0)
                {
                    throw new ArgumentException("The job state script must contain at least one state.", nameof(value));
                }

                _jobStateScript = value.ToArray();
            }
        }

        /// <summary>
        /// How long <c>jobs.getQueryResults</c> holds a request open before reporting an incomplete
        /// job, capped by the timeoutMs the client asked for. Real BigQuery long-polls here;
        /// responding immediately would spin the client in a tight request loop. Defaults to 100ms.
        /// </summary>
        public TimeSpan IncompleteQueryResultsDelay { get; set; } = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// The mock gRPC service for configuring Storage Read API responses.
        /// </summary>
        public MockBigQueryReadService ReadService { get; }

        /// <summary>
        /// The mock gRPC service for tracking Storage Write API requests.
        /// </summary>
        public MockBigQueryWriteService WriteService { get; }

        /// <summary>
        /// Creates and starts a new mock BigQuery server on random loopback ports.
        /// </summary>
        public BigQueryMockServer()
        {
            ReadService = new MockBigQueryReadService();
            WriteService = new MockBigQueryWriteService();

            int restPort = GetFreePort();
            int grpcPort = GetFreePort();

            _restApp = BuildRestApp(restPort);
            _grpcApp = BuildGrpcApp(grpcPort);

            _restApp.StartAsync().GetAwaiter().GetResult();
            _grpcApp.StartAsync().GetAwaiter().GetResult();

            RestEndpoint = $"127.0.0.1:{restPort}";
            GrpcEndpoint = $"127.0.0.1:{grpcPort}";
        }

        /// <summary>
        /// Sets <see cref="JobStateScript"/> from the given states, e.g.
        /// <c>ScriptJobStates(JobStateRunning, JobStateRunning, JobStateDone)</c>.
        /// </summary>
        public void ScriptJobStates(params string[] states) => JobStateScript = states;

        /// <summary>
        /// Returns the recorded requests of the given kind, in arrival order.
        /// </summary>
        public IReadOnlyList<MockRequest> RequestsOfKind(MockRequestKind kind) =>
            _requests.Where(request => request.Kind == kind).ToArray();

        /// <summary>
        /// Returns the number of recorded requests of the given kind.
        /// </summary>
        public int CountOfKind(MockRequestKind kind) => _requests.Count(request => request.Kind == kind);

        private MockRequest Record(MockRequestKind kind, string? jobId = null)
        {
            MockRequest request = new(kind, jobId, Interlocked.Increment(ref _requestSequence) - 1);
            _requests.Enqueue(request);
            RequestReceived?.Invoke(request);
            return request;
        }

        private WebApplication BuildRestApp(int port)
        {
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(options =>
            {
                options.Listen(IPAddress.Loopback, port, listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http1;
                });
            });

            var app = builder.Build();
            MapRestRoutes(app);
            return app;
        }

        private WebApplication BuildGrpcApp(int port)
        {
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddGrpc();
            builder.Services.AddSingleton(ReadService);
            builder.Services.AddSingleton(WriteService);
            builder.WebHost.ConfigureKestrel(options =>
            {
                options.Listen(IPAddress.Loopback, port, listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http2;
                });
            });

            var app = builder.Build();
            app.MapGrpcService<MockBigQueryReadService>();
            app.MapGrpcService<MockBigQueryWriteService>();
            return app;
        }

        private void MapRestRoutes(WebApplication app)
        {
            // The Google.Apis BigQuery client serializes/deserializes using
            // Google.Apis.Json.NewtonsoftJsonSerializer. We must return JSON
            // produced by the same serializer operating on the real model classes.

            // POST /bigquery/v2/projects/{projectId}/jobs - Create a query job
            app.MapPost("/bigquery/v2/projects/{projectId}/jobs", async (HttpContext ctx, string projectId) =>
            {
                string body = await ReadBodyAsync(ctx).ConfigureAwait(false);

                Job? jobRequest = null;
                try
                {
                    jobRequest = NewtonsoftJsonSerializer.Instance.Deserialize<Job>(body);
                }
                catch
                {
                    // If deserialization fails, proceed without parsed request
                }

                string jobId = $"mock-job-{Guid.NewGuid():N}";
                string? queryText = jobRequest?.Configuration?.Query?.Query;
                bool createSession = jobRequest?.Configuration?.Query?.CreateSession == true;
                string? sessionId = null;

                // Check for session_id in ConnectionProperties
                if (jobRequest?.Configuration?.Query?.ConnectionProperties != null)
                {
                    foreach (var prop in jobRequest.Configuration.Query.ConnectionProperties)
                    {
                        if (prop.Key == "session_id")
                        {
                            sessionId = prop.Value;
                            break;
                        }
                    }
                }

                if (queryText != null)
                {
                    _executedQueries.Enqueue(queryText);
                }

                var mockJob = new MockJob
                {
                    JobId = jobId,
                    ProjectId = projectId,
                    StateScript = _jobStateScript,
                    StatementType = queryText?.TrimStart().StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase) == true ? "UPDATE" : "SELECT",
                    NumDmlAffectedRows = queryText?.TrimStart().StartsWith("UPDATE", StringComparison.OrdinalIgnoreCase) == true ? 2 : null,
                };

                // If CreateSession is requested, generate a new session ID
                if (createSession)
                {
                    string newSessionId = $"mock-session-{Guid.NewGuid():N}";
                    _sessions[newSessionId] = true;
                    mockJob.SessionId = newSessionId;
                }
                else if (sessionId != null)
                {
                    mockJob.SessionId = sessionId;
                }

                _jobs[jobId] = mockJob;
                Record(MockRequestKind.JobInsert, jobId);

                // The insert response reports the initial state without consuming a scripted
                // observation; only status polls advance the script.
                var job = CreateJobResource(mockJob, mockJob.PeekState());
                string json = NewtonsoftJsonSerializer.Instance.Serialize(job);
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync(json);
            });

            // GET /bigquery/v2/projects/{projectId}/jobs/{jobId} - Get job status
            app.MapGet("/bigquery/v2/projects/{projectId}/jobs/{jobId}", async (HttpContext ctx, string projectId, string jobId) =>
            {
                Record(MockRequestKind.JobGet, jobId);
                if (!_jobs.TryGetValue(jobId, out var mockJob))
                {
                    ctx.Response.StatusCode = 404;
                    return;
                }

                var job = CreateJobResource(mockJob, mockJob.NextState());
                string json = NewtonsoftJsonSerializer.Instance.Serialize(job);
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync(json);
            });

            // POST /bigquery/v2/projects/{projectId}/jobs/{jobId}/cancel - Request job cancellation
            app.MapPost("/bigquery/v2/projects/{projectId}/jobs/{jobId}/cancel", async (HttpContext ctx, string projectId, string jobId) =>
            {
                Record(MockRequestKind.JobCancel, jobId);
                if (!_jobs.TryGetValue(jobId, out var mockJob))
                {
                    ctx.Response.StatusCode = 404;
                    var notFound = new { error = new { code = 404, message = $"Not found: Job {projectId}:{jobId}", status = "NOT_FOUND" } };
                    await ctx.Response.WriteAsJsonAsync(notFound);
                    return;
                }

                mockJob.Cancel();

                // jobs.cancel returns the job resource wrapped in a JobCancelResponse. Cancellation is
                // asynchronous in real BigQuery, but the returned job is already terminal here so that
                // a subsequent poll observes the stopped state deterministically.
                var response = new JobCancelResponse
                {
                    Kind = "bigquery#jobCancelResponse",
                    Job = CreateJobResource(mockJob, JobStateDone),
                };

                string json = NewtonsoftJsonSerializer.Instance.Serialize(response);
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync(json);
            });

            // GET /bigquery/v2/projects/{projectId}/queries/{jobId} - Get query results
            app.MapGet("/bigquery/v2/projects/{projectId}/queries/{jobId}", async (HttpContext ctx, string projectId, string jobId) =>
            {
                Interlocked.Increment(ref _queryResultsRequestCount);
                Record(MockRequestKind.QueryResults, jobId);
                if (!_jobs.TryGetValue(jobId, out var mockJob))
                {
                    ctx.Response.StatusCode = 404;
                    return;
                }

                var jobReference = new JobReference { ProjectId = projectId, JobId = jobId, Location = "US" };

                if (mockJob.NextState() != JobStateDone)
                {
                    await DelayForIncompleteResultsAsync(ctx).ConfigureAwait(false);

                    var pending = new GetQueryResultsResponse
                    {
                        Kind = "bigquery#getQueryResultsResponse",
                        JobReference = jobReference,
                        JobComplete = false,
                    };

                    string pendingJson = NewtonsoftJsonSerializer.Instance.Serialize(pending);
                    ctx.Response.ContentType = "application/json";
                    await ctx.Response.WriteAsync(pendingJson);
                    return;
                }

                var response = new GetQueryResultsResponse
                {
                    Kind = "bigquery#getQueryResultsResponse",
                    JobReference = jobReference,
                    JobComplete = true,
                    TotalRows = 1,
                    Schema = new TableSchema
                    {
                        Fields = new[]
                        {
                            new TableFieldSchema { Name = "value", Type = "INTEGER", Mode = "NULLABLE" }
                        }
                    },
                };

                if (mockJob.IsCancelled)
                {
                    response.Errors = new List<ErrorProto> { CreateStoppedError() };
                }

                string json = NewtonsoftJsonSerializer.Instance.Serialize(response);
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync(json);
            });

            // GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId} - Get table
            app.MapGet("/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}", async (HttpContext ctx, string projectId, string datasetId, string tableId) =>
            {
                Record(MockRequestKind.TableGet);
                string key = $"{projectId}.{datasetId}.{tableId}";
                if (!_tables.TryGetValue(key, out var table))
                {
                    ctx.Response.StatusCode = 404;
                    var error = new { error = new { code = 404, message = $"Not found: Table {projectId}:{datasetId}.{tableId}", status = "NOT_FOUND" } };
                    await ctx.Response.WriteAsJsonAsync(error);
                    return;
                }

                string json = NewtonsoftJsonSerializer.Instance.Serialize(table);
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync(json);
            });

            // POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables - Create table
            app.MapPost("/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables", async (HttpContext ctx, string projectId, string datasetId) =>
            {
                Record(MockRequestKind.TableInsert);
                string body = await ReadBodyAsync(ctx).ConfigureAwait(false);
                var table = NewtonsoftJsonSerializer.Instance.Deserialize<Table>(body);
                if (table == null)
                {
                    ctx.Response.StatusCode = 400;
                    return;
                }

                string tableId = table.TableReference?.TableId ?? "";
                if (string.IsNullOrEmpty(tableId))
                {
                    ctx.Response.StatusCode = 400;
                    var badRequest = new { error = new { code = 400, message = "tableReference.tableId is required", status = "INVALID_ARGUMENT" } };
                    await ctx.Response.WriteAsJsonAsync(badRequest);
                    return;
                }
                string key = $"{projectId}.{datasetId}.{tableId}";

                if (_tables.ContainsKey(key))
                {
                    ctx.Response.StatusCode = 409;
                    var error = new { error = new { code = 409, message = $"Already Exists: Table {projectId}:{datasetId}.{tableId}", status = "ALREADY_EXISTS" } };
                    await ctx.Response.WriteAsJsonAsync(error);
                    return;
                }

                table.TableReference ??= new TableReference();
                table.TableReference.ProjectId = projectId;
                table.TableReference.DatasetId = datasetId;
                table.Kind = "bigquery#table";
                _tables[key] = table;

                string json = NewtonsoftJsonSerializer.Instance.Serialize(table);
                ctx.Response.ContentType = "application/json";
                ctx.Response.StatusCode = 200;
                await ctx.Response.WriteAsync(json);
            });

            // DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId} - Delete table
            app.MapDelete("/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}", (HttpContext ctx, string projectId, string datasetId, string tableId) =>
            {
                Record(MockRequestKind.TableDelete);
                string key = $"{projectId}.{datasetId}.{tableId}";
                _tables.TryRemove(key, out _);
                ctx.Response.StatusCode = 204;
                return Task.CompletedTask;
            });

            // Catch-all for unhandled endpoints
            app.MapFallback(async (HttpContext ctx) =>
            {
                ctx.Response.StatusCode = 404;
                await ctx.Response.WriteAsync($"Mock server: no handler for {ctx.Request.Method} {ctx.Request.Path}");
            });
        }

        private static async Task<string> ReadBodyAsync(HttpContext ctx)
        {
            if (string.Equals(ctx.Request.Headers["Content-Encoding"].ToString(), "gzip", StringComparison.OrdinalIgnoreCase))
            {
                await using var gzipStream = new GZipStream(ctx.Request.Body, CompressionMode.Decompress, leaveOpen: true);
                using var gzipReader = new System.IO.StreamReader(gzipStream);
                return await gzipReader.ReadToEndAsync();
            }

            using var reader = new System.IO.StreamReader(ctx.Request.Body);
            return await reader.ReadToEndAsync();
        }

        private async Task DelayForIncompleteResultsAsync(HttpContext ctx)
        {
            double waitMs = IncompleteQueryResultsDelay.TotalMilliseconds;
            if (ctx.Request.Query.TryGetValue("timeoutMs", out var rawTimeout) &&
                int.TryParse(rawTimeout.ToString(), out int timeoutMs) &&
                timeoutMs >= 0)
            {
                waitMs = Math.Min(waitMs, timeoutMs);
            }

            if (waitMs <= 0)
            {
                return;
            }

            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(waitMs), ctx.RequestAborted).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // The client gave up (typically a cancelled statement); fall through and let the
                // response write fail or be discarded.
            }
        }

        private static ErrorProto CreateStoppedError() => new ErrorProto
        {
            Reason = "stopped",
            Message = "Job execution was cancelled: User requested cancellation",
        };

        private static Job CreateJobResource(MockJob mockJob, string state)
        {
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var statistics = new JobStatistics
            {
                CreationTime = now,
                StartTime = now,
                EndTime = now,
                Query = new JobStatistics2
                {
                    StatementType = mockJob.StatementType,
                    NumDmlAffectedRows = mockJob.NumDmlAffectedRows,
                    TotalBytesProcessed = 0,
                    TotalBytesBilled = 0,
                }
            };

            if (mockJob.SessionId != null)
            {
                statistics.SessionInfo = new SessionInfo
                {
                    SessionId = mockJob.SessionId
                };
            }

            var status = new JobStatus { State = state };
            if (mockJob.IsCancelled && state == JobStateDone)
            {
                ErrorProto stopped = CreateStoppedError();
                status.ErrorResult = stopped;
                status.Errors = new List<ErrorProto> { stopped };
            }

            return new Job
            {
                Kind = "bigquery#job",
                Id = $"{mockJob.ProjectId}:{mockJob.JobId}",
                JobReference = new JobReference
                {
                    ProjectId = mockJob.ProjectId,
                    JobId = mockJob.JobId,
                    Location = "US"
                },
                Status = status,
                Configuration = new JobConfiguration
                {
                    Query = new JobConfigurationQuery
                    {
                        DestinationTable = new TableReference
                        {
                            ProjectId = mockJob.ProjectId,
                            DatasetId = "_mock_temp",
                            TableId = $"mock_results_{mockJob.JobId}"
                        },
                        UseLegacySql = false,
                    },
                },
                Statistics = statistics
            };
        }

        private static int GetFreePort()
        {
            using var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }

        public void Dispose()
        {
            _cts.CancelAfter(TimeSpan.FromSeconds(5));
            _restApp.StopAsync(_cts.Token).GetAwaiter().GetResult();
            _grpcApp.StopAsync(_cts.Token).GetAwaiter().GetResult();
            (_restApp as IDisposable)?.Dispose();
            (_grpcApp as IDisposable)?.Dispose();
            _cts.Dispose();
        }

        private sealed class MockJob
        {
            private int _observationCount;
            private int _cancelled;

            public string JobId { get; set; } = string.Empty;
            public string ProjectId { get; set; } = string.Empty;
            public IReadOnlyList<string> StateScript { get; set; } = new[] { JobStateDone };
            public string? SessionId { get; set; }
            public string StatementType { get; set; } = "SELECT";
            public long? NumDmlAffectedRows { get; set; }

            /// <summary>Whether jobs.cancel has been called for this job.</summary>
            public bool IsCancelled => Volatile.Read(ref _cancelled) != 0;

            /// <summary>Marks the job cancelled. Returns true if this call was the one that cancelled it.</summary>
            public bool Cancel() => Interlocked.Exchange(ref _cancelled, 1) == 0;

            /// <summary>
            /// The state for the next status observation, advancing the script by one. The last
            /// scripted state repeats once the script is exhausted.
            /// </summary>
            public string NextState()
            {
                int index = Interlocked.Increment(ref _observationCount) - 1;
                return IsCancelled ? JobStateDone : StateScript[Math.Min(index, StateScript.Count - 1)];
            }

            /// <summary>The state the next observation would report, without advancing the script.</summary>
            public string PeekState()
            {
                int index = Volatile.Read(ref _observationCount);
                return IsCancelled ? JobStateDone : StateScript[Math.Min(index, StateScript.Count - 1)];
            }
        }
    }
}
