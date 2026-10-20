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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.BigQuery.MockServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Cloud.BigQuery.Storage.V1;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests.MockServer
{
    [Trait("Category", "MockServer")]
    public class BulkIngestPipelineTests
    {
        [Theory]
        [InlineData(null, true)]
        [InlineData("none", false)]
        [InlineData("gzip", true)]
        public async Task CompressionChangesGrpcFramingButNotArrowPayload(string? compression, bool expectCompressed)
        {
            using var server = new BigQueryMockServer();
            var requests = new ConcurrentQueue<AppendRowsRequest>();
            server.WriteService.AppendResponseFactory = request =>
            {
                requests.Enqueue(request);
                return Task.FromResult<AppendRowsResponse?>(new AppendRowsResponse { AppendResult = new AppendRowsResponse.Types.AppendResult() });
            };
            using AdbcConnection connection = Connect(server);
            using RecordBatch batch = CreateBatch(4096);
            using var stream = new BatchStream(batch, batch);
            using AdbcStatement statement = connection.BulkIngest("mock-project", "mock_dataset", "compression", BulkIngestMode.Create, false);
            if (compression != null)
                statement.SetOption(BigQueryParameters.IngestGrpcCompression, compression);
            statement.BindStream(stream);

            UpdateResult result = await statement.ExecuteUpdateAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(8192, result.AffectedRows);
            Assert.Equal(2, requests.Count);
            byte[] expected = ArrowSerializationHelpers.SerializeRecordBatch(batch);
            Assert.All(requests, request => Assert.Equal(expected, request.ArrowRows.Rows.SerializedRecordBatch.ToByteArray()));
            var wire = Assert.Single(server.AppendTransportRequests);
            Assert.Equal(expectCompressed, wire.Compressed);
            if (expectCompressed)
            {
                Assert.Equal("gzip", wire.Encoding);
                Assert.True(wire.MessageBytes < requests.First().CalculateSize(), "The gRPC message must actually be compressed, not just labeled gzip.");
            }
            else
            {
                Assert.True(wire.Encoding == "" || wire.Encoding == "identity");
                Assert.Equal(requests.First().CalculateSize(), wire.MessageBytes);
            }
            Assert.True(Assert.Single(server.WriteService.Streams.Values).Finalized);
            Assert.Single(server.WriteService.CommittedStreams);
        }

        [Fact]
        public async Task UnsupportedCompressionFailsWithoutRetryOrCommit()
        {
            using var server = new BigQueryMockServer(enableGrpcGzip: false);
            using AdbcConnection connection = Connect(server);
            using RecordBatch batch = CreateBatch(4096);
            using AdbcStatement statement = connection.BulkIngest("mock-project", "mock_dataset", "unsupported_compression", BulkIngestMode.Create, false);
            statement.SetOption(BigQueryParameters.IngestGrpcCompression, "gzip");
            statement.Bind(batch, batch.Schema);

            AdbcException error = await Assert.ThrowsAsync<AdbcException>(() =>
                statement.ExecuteUpdateAsync().WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(Grpc.Core.StatusCode.Unimplemented, Assert.IsType<Grpc.Core.RpcException>(error.InnerException).StatusCode);
            Assert.Contains("gzip", error.Message);
            var wire = Assert.Single(server.AppendTransportRequests);
            Assert.True(wire.Compressed);
            Assert.Equal("gzip", wire.Encoding);
            Assert.Empty(Assert.Single(server.WriteService.Streams.Values).RecordBatches);
            Assert.False(Assert.Single(server.WriteService.Streams.Values).Finalized);
            Assert.Empty(server.WriteService.CommittedStreams);
        }

        [Theory]
        [InlineData("")]
        [InlineData("zstd")]
        public void RejectsUnsupportedCompressionOption(string value)
        {
            using var server = new BigQueryMockServer();
            using AdbcConnection connection = Connect(server);
            using AdbcStatement statement = connection.CreateStatement();
            Assert.Throws<ArgumentException>(() => statement.SetOption(BigQueryParameters.IngestGrpcCompression, value));
        }

        [Theory]
        [InlineData(null, "none")]
        [InlineData(1, "none")]
        [InlineData(3, "none")]
        [InlineData(3, "gzip")]
        public async Task PipelineFillsButDoesNotExceedWindowBeforeAcknowledgment(int? configuredWindow, string compression)
        {
            int window = configuredWindow ?? BigQueryConstants.DefaultIngestMaxInFlightBatches;
            using var server = new BigQueryMockServer();
            var windowFilled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseResponse = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var requests = new ConcurrentQueue<AppendRowsRequest>();
            server.WriteService.AppendRequestReceived = request =>
            {
                requests.Enqueue(request);
                if (requests.Count == window)
                    windowFilled.TrySetResult(true);
            };
            server.WriteService.AppendResponseFactory = async _ =>
            {
                await releaseResponse.Task;
                return new AppendRowsResponse { AppendResult = new AppendRowsResponse.Types.AppendResult() };
            };
            using AdbcConnection connection = Connect(server);
            using RecordBatch first = CreateBatch(2);
            using RecordBatch second = CreateBatch(1);
            RecordBatch[] batches = { first, second, first, second, first, second, first };
            using var stream = new BatchStream(batches);
            using AdbcStatement statement = connection.BulkIngest("mock-project", "mock_dataset", "pipeline", BulkIngestMode.Create, false);
            if (configuredWindow.HasValue)
                statement.SetOption(BigQueryParameters.IngestMaxInFlightBatches, configuredWindow.Value.ToString());
            statement.SetOption(BigQueryParameters.IngestGrpcCompression, compression);
            statement.BindStream(stream);

            Task<UpdateResult> execution = statement.ExecuteUpdateAsync();
            try
            {
                await windowFilled.Task.WaitAsync(TimeSpan.FromSeconds(10));
                await Task.Delay(100);
                Assert.Equal(window, requests.Count);
                Assert.Equal(window, stream.ReadCalls);
                Assert.False(execution.IsCompleted);
                Assert.False(Assert.Single(server.WriteService.Streams.Values).Finalized);
                Assert.Empty(server.WriteService.CommittedStreams);
            }
            finally
            {
                releaseResponse.TrySetResult(true);
            }

            UpdateResult result = await execution.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(11, result.AffectedRows);
            Assert.Equal(batches.Length, requests.Count);
            Assert.NotNull(requests.First().ArrowRows.WriterSchema);
            Assert.All(requests.Skip(1), request => Assert.Null(request.ArrowRows.WriterSchema));
            Assert.Equal(batches.Select(b => Convert.ToBase64String(ArrowSerializationHelpers.SerializeRecordBatch(b))),
                requests.Select(request => Convert.ToBase64String(request.ArrowRows.Rows.SerializedRecordBatch.ToByteArray())));
            Assert.True(Assert.Single(server.WriteService.Streams.Values).Finalized);
            Assert.Single(server.WriteService.CommittedStreams);
        }

        [Theory]
        [InlineData("error", 0, "rejected for test")]
        [InlineData("error", 6, "rejected for test")]
        [InlineData("end", 0, "ended before acknowledging")]
        [InlineData("end", 6, "ended before acknowledging")]
        [InlineData("empty", 0, "did not contain an append result")]
        public async Task FailedOrMissingResponsePreventsFinalizeAndCommit(string responseKind, int successfulResponses, string message)
        {
            using var server = new BigQueryMockServer();
            int responses = 0;
            server.WriteService.AppendResponseFactory = _ => Task.FromResult(++responses <= successfulResponses
                ? new AppendRowsResponse { AppendResult = new AppendRowsResponse.Types.AppendResult() }
                : responseKind switch
            {
                "end" => null,
                "empty" => new AppendRowsResponse(),
                _ => new AppendRowsResponse { Error = new Google.Rpc.Status { Code = 3, Message = "rejected for test" } },
            });
            using AdbcConnection connection = Connect(server);
            using RecordBatch batch = CreateBatch(2);
            using var stream = new BatchStream(Enumerable.Repeat(batch, 7).ToArray());
            using AdbcStatement statement = connection.BulkIngest("mock-project", "mock_dataset", "append_failure", BulkIngestMode.Create, false);
            statement.SetOption(BigQueryParameters.IngestMaxInFlightBatches, "2");
            statement.BindStream(stream);

            AdbcException error = await Assert.ThrowsAsync<AdbcException>(() =>
                statement.ExecuteUpdateAsync().WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Contains(message, error.Message);
            Assert.Empty(server.WriteService.CommittedStreams);
            Assert.False(Assert.Single(server.WriteService.Streams.Values).Finalized);
        }

        [Fact]
        public async Task SourceFailureCancelsPendingResponseAndPreventsCommit()
        {
            using var server = new BigQueryMockServer();
            var releaseResponse = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.WriteService.AppendResponseFactory = async _ =>
            {
                await releaseResponse.Task;
                return new AppendRowsResponse { AppendResult = new AppendRowsResponse.Types.AppendResult() };
            };
            using AdbcConnection connection = Connect(server);
            using RecordBatch batch = CreateBatch(2);
            using var stream = new BatchStream(batch) { EndError = new InvalidOperationException("source failed") };
            using AdbcStatement statement = connection.BulkIngest("mock-project", "mock_dataset", "source_failure", BulkIngestMode.Create, false);
            statement.BindStream(stream);
            try
            {
                AdbcException error = await Assert.ThrowsAsync<AdbcException>(() =>
                    statement.ExecuteUpdateAsync().WaitAsync(TimeSpan.FromSeconds(10)));
                Assert.Contains("source failed", error.Message);
            }
            finally
            {
                releaseResponse.TrySetResult(true);
            }
            Assert.Empty(server.WriteService.CommittedStreams);
            Assert.False(Assert.Single(server.WriteService.Streams.Values).Finalized);
        }

        [Fact]
        public async Task ResponseFailureCancelsBlockedInputReadAndPreservesOriginalError()
        {
            using var server = new BigQueryMockServer();
            using RecordBatch batch = CreateBatch(2);
            using var stream = new BlockingBatchStream(batch);
            server.WriteService.AppendResponseFactory = async _ =>
            {
                await stream.WaitingForRead.Task;
                return new AppendRowsResponse { Error = new Google.Rpc.Status { Code = 3, Message = "response failed" } };
            };
            using AdbcConnection connection = Connect(server);
            using AdbcStatement statement = connection.BulkIngest("mock-project", "mock_dataset", "response_failure", BulkIngestMode.Create, false);
            statement.BindStream(stream);
            AdbcException error = await Assert.ThrowsAsync<AdbcException>(() =>
                statement.ExecuteUpdateAsync().WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Contains("response failed", error.Message);
            Assert.True(stream.WaitingForRead.Task.IsCompletedSuccessfully);
            Assert.Empty(server.WriteService.CommittedStreams);
            Assert.False(Assert.Single(server.WriteService.Streams.Values).Finalized);
        }

        [Fact]
        public async Task CallerCancellationStopsFullPipelineWithoutCommit()
        {
            using var server = new BigQueryMockServer();
            var windowFilled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int received = 0;
            server.WriteService.AppendRequestReceived = _ =>
            {
                if (Interlocked.Increment(ref received) == 2)
                    windowFilled.TrySetResult(true);
            };
            var releaseResponse = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.WriteService.AppendResponseFactory = async _ =>
            {
                await releaseResponse.Task;
                return new AppendRowsResponse { AppendResult = new AppendRowsResponse.Types.AppendResult() };
            };
            using AdbcConnection connection = Connect(server);
            using RecordBatch batch = CreateBatch(2);
            using var stream = new BatchStream(Enumerable.Repeat(batch, 7).ToArray());
            var writeClient = new BigQueryWriteClientBuilder
            {
                Endpoint = server.GrpcEndpoint,
                ChannelCredentials = Grpc.Core.ChannelCredentials.Insecure,
            }.Build();
            var ingest = new BigQueryBulkIngest(((BigQueryConnection)connection).Client!, writeClient, "mock-project");
            using var cancellation = new CancellationTokenSource();
            Task<UpdateResult> execution = ingest.ExecuteAsync("mock-project", "mock_dataset", "canceled",
                BulkIngestMode.Create, null, stream, 2, cancellationToken: cancellation.Token);
            try
            {
                await windowFilled.Task.WaitAsync(TimeSpan.FromSeconds(10));
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => execution.WaitAsync(TimeSpan.FromSeconds(10)));
            }
            finally
            {
                cancellation.Cancel();
                releaseResponse.TrySetResult(true);
            }
            Assert.Empty(server.WriteService.CommittedStreams);
            Assert.False(Assert.Single(server.WriteService.Streams.Values).Finalized);
        }

        [Fact]
        public async Task FinalRpcFailurePreventsFinalizeAndCommit()
        {
            using var server = new BigQueryMockServer
            {
                WriteService = { AppendCompletionError = new Grpc.Core.RpcException(
                    new Grpc.Core.Status(Grpc.Core.StatusCode.Internal, "trailer failure")) },
            };
            using AdbcConnection connection = Connect(server);
            using RecordBatch batch = CreateBatch(2);
            using AdbcStatement statement = connection.BulkIngest("mock-project", "mock_dataset", "rpc_failure", BulkIngestMode.Create, false);
            statement.Bind(batch, batch.Schema);
            AdbcException error = await Assert.ThrowsAsync<AdbcException>(() =>
                statement.ExecuteUpdateAsync().WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Contains("trailer failure", error.Message);
            Assert.Empty(server.WriteService.CommittedStreams);
            Assert.False(Assert.Single(server.WriteService.Streams.Values).Finalized);
        }

        [Fact]
        public async Task EmptyInputCompletesWithoutWaitingForAnAcknowledgment()
        {
            using var server = new BigQueryMockServer();
            using AdbcConnection connection = Connect(server);
            using RecordBatch batch = CreateBatch(0);
            using var stream = new BatchStream(batch.Schema);
            using AdbcStatement statement = connection.BulkIngest("mock-project", "mock_dataset", "empty_input", BulkIngestMode.Create, false);
            statement.BindStream(stream);
            UpdateResult result = await statement.ExecuteUpdateAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(0, result.AffectedRows);
            Assert.Empty(Assert.Single(server.WriteService.Streams.Values).RecordBatches);
            Assert.Single(server.WriteService.CommittedStreams);
        }

        [Theory]
        [InlineData("0")]
        [InlineData("-1")]
        [InlineData("invalid")]
        public void RejectsInvalidWindow(string value)
        {
            using var server = new BigQueryMockServer();
            using AdbcConnection connection = Connect(server);
            using AdbcStatement statement = connection.CreateStatement();
            Assert.Throws<ArgumentException>(() => statement.SetOption(BigQueryParameters.IngestMaxInFlightBatches, value));
        }

        private static AdbcConnection Connect(BigQueryMockServer server)
        {
            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(new Dictionary<string, string>
            {
                [BigQueryParameters.ProjectId] = "mock-project",
                [BigQueryParameters.AuthenticationType] = BigQueryConstants.MockAuthenticationType,
                [BigQueryParameters.TestRestEndpoint] = server.RestEndpoint,
                [BigQueryParameters.TestStorageEndpoint] = server.GrpcEndpoint,
            });
            return database.Connect(new Dictionary<string, string>());
        }

        private static RecordBatch CreateBatch(int rows)
        {
            var schema = new Schema(new[] { new Field("id", Int64Type.Default, false) }, null);
            var values = new Int64Array.Builder();
            for (int i = 0; i < rows; i++) values.Append(i);
            return new RecordBatch(schema, new IArrowArray[] { values.Build() }, rows);
        }

        private sealed class BatchStream : IArrowArrayStream
        {
            private readonly Queue<RecordBatch> _batches;
            private int _readCalls;
            public BatchStream(params RecordBatch[] batches) : this(batches[0].Schema, batches) { }
            public BatchStream(Schema schema, params RecordBatch[] batches)
            {
                Schema = schema;
                _batches = new Queue<RecordBatch>(batches);
            }
            public Schema Schema { get; }
            public int ReadCalls => Volatile.Read(ref _readCalls);
            public Exception? EndError { get; set; }
            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                Interlocked.Increment(ref _readCalls);
                if (_batches.Count == 0 && EndError != null) throw EndError;
                return new ValueTask<RecordBatch?>(_batches.Count == 0 ? null : _batches.Dequeue());
            }
            public void Dispose() { }
        }

        private sealed class BlockingBatchStream : IArrowArrayStream
        {
            private readonly RecordBatch _batch;
            private bool _read;
            public BlockingBatchStream(RecordBatch batch) => _batch = batch;
            public Schema Schema => _batch.Schema;
            public TaskCompletionSource<bool> WaitingForRead { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (!_read)
                {
                    _read = true;
                    return _batch;
                }
                WaitingForRead.TrySetResult(true);
                await Task.Delay(Timeout.Infinite, cancellationToken);
                return null;
            }
            public void Dispose() { }
        }

    }
}

#endif
