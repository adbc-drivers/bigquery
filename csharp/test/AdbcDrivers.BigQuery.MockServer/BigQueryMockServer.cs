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
using System.Net;
using System.Threading;
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
        /// The mock gRPC service for configuring Storage Read API responses.
        /// </summary>
        public MockBigQueryReadService ReadService { get; }

        /// <summary>
        /// Creates and starts a new mock BigQuery server on random loopback ports.
        /// </summary>
        public BigQueryMockServer()
        {
            ReadService = new MockBigQueryReadService();

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
            builder.WebHost.ConfigureKestrel(options =>
            {
                options.Listen(IPAddress.Loopback, port, listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http2;
                });
            });

            var app = builder.Build();
            app.MapGrpcService<MockBigQueryReadService>();
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
                string body = await new System.IO.StreamReader(ctx.Request.Body).ReadToEndAsync();

                string jobId = $"mock-job-{Guid.NewGuid():N}";

                var mockJob = new MockJob
                {
                    JobId = jobId,
                    ProjectId = projectId,
                    Status = "DONE",
                };
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
                Statistics = new JobStatistics
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
                }
            };
        }

        private static int GetFreePort()
        {
            var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        public void Dispose()
        {
            _cts.Cancel();
            _restApp.StopAsync().GetAwaiter().GetResult();
            _grpcApp.StopAsync().GetAwaiter().GetResult();
            (_restApp as IDisposable)?.Dispose();
            (_grpcApp as IDisposable)?.Dispose();
            _cts.Dispose();
        }

        private class MockJob
        {
            public string JobId { get; set; } = string.Empty;
            public string ProjectId { get; set; } = string.Empty;
            public string Status { get; set; } = "DONE";
        }
    }
}
