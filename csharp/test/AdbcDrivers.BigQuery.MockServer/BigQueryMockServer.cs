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
        private readonly WebApplication _restApp;
        private readonly WebApplication _grpcApp;
        private readonly CancellationTokenSource _cts = new();
        private readonly ConcurrentDictionary<string, MockJob> _jobs = new();
        private readonly ConcurrentDictionary<string, Table> _tables = new();
        private readonly ConcurrentDictionary<string, bool> _sessions = new();
        private readonly ConcurrentQueue<string> _executedQueries = new();

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
                string body;
                if (string.Equals(ctx.Request.Headers["Content-Encoding"].ToString(), "gzip", StringComparison.OrdinalIgnoreCase))
                {
                    await using var gzipStream = new GZipStream(ctx.Request.Body, CompressionMode.Decompress, leaveOpen: true);
                    using var reader = new System.IO.StreamReader(gzipStream);
                    body = await reader.ReadToEndAsync();
                }
                else
                {
                    body = await new System.IO.StreamReader(ctx.Request.Body).ReadToEndAsync();
                }

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
                    Status = "DONE",
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

                var job = CreateJobResource(mockJob);
                string json = NewtonsoftJsonSerializer.Instance.Serialize(job);
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync(json);
            });

            // GET /bigquery/v2/projects/{projectId}/jobs/{jobId} - Get job status
            app.MapGet("/bigquery/v2/projects/{projectId}/jobs/{jobId}", async (HttpContext ctx, string projectId, string jobId) =>
            {
                if (!_jobs.TryGetValue(jobId, out var mockJob))
                {
                    ctx.Response.StatusCode = 404;
                    return;
                }

                var job = CreateJobResource(mockJob);
                string json = NewtonsoftJsonSerializer.Instance.Serialize(job);
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync(json);
            });

            // GET /bigquery/v2/projects/{projectId}/queries/{jobId} - Get query results
            app.MapGet("/bigquery/v2/projects/{projectId}/queries/{jobId}", async (HttpContext ctx, string projectId, string jobId) =>
            {
                if (!_jobs.TryGetValue(jobId, out var mockJob))
                {
                    ctx.Response.StatusCode = 404;
                    return;
                }

                var response = new GetQueryResultsResponse
                {
                    Kind = "bigquery#getQueryResultsResponse",
                    JobReference = new JobReference { ProjectId = projectId, JobId = jobId, Location = "US" },
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

                string json = NewtonsoftJsonSerializer.Instance.Serialize(response);
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync(json);
            });

            // GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId} - Get table
            app.MapGet("/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}", async (HttpContext ctx, string projectId, string datasetId, string tableId) =>
            {
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
                string body;
                if (string.Equals(ctx.Request.Headers["Content-Encoding"].ToString(), "gzip", StringComparison.OrdinalIgnoreCase))
                {
                    await using var gzipStream = new GZipStream(ctx.Request.Body, CompressionMode.Decompress, leaveOpen: true);
                    using var reader = new System.IO.StreamReader(gzipStream);
                    body = await reader.ReadToEndAsync();
                }
                else
                {
                    using var reader = new System.IO.StreamReader(ctx.Request.Body);
                    body = await reader.ReadToEndAsync();
                }
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

        private static Job CreateJobResource(MockJob mockJob)
        {
            long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var statistics = new JobStatistics
            {
                CreationTime = now,
                StartTime = now,
                EndTime = now,
                Query = new JobStatistics2
                {
                    StatementType = "SELECT",
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
                Status = new JobStatus { State = mockJob.Status },
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

        private class MockJob
        {
            public string JobId { get; set; } = string.Empty;
            public string ProjectId { get; set; } = string.Empty;
            public string Status { get; set; } = "DONE";
            public string? SessionId { get; set; }
        }
    }
}
