/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
using System.Threading.Tasks;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace AdbcDrivers.BigQuery.MockServer
{
    /// <summary>
    /// Mock implementation of the BigQuery Storage Write API for testing bulk ingestion.
    /// Tracks all received data for verification in tests.
    /// </summary>
    public class MockBigQueryWriteService : BigQueryWrite.BigQueryWriteBase
    {
        private int _streamCounter;
        private readonly ConcurrentDictionary<string, WriteStreamInfo> _streams = new();

        /// <summary>
        /// Gets all write stream info for verification in tests.
        /// </summary>
        public IReadOnlyDictionary<string, WriteStreamInfo> Streams => _streams;

        /// <summary>
        /// Gets the list of committed stream names (from BatchCommitWriteStreams calls).
        /// </summary>
        public ConcurrentBag<string> CommittedStreams { get; } = new();

        public override Task<WriteStream> CreateWriteStream(CreateWriteStreamRequest request, ServerCallContext context)
        {
            int id = System.Threading.Interlocked.Increment(ref _streamCounter);
            string streamName = $"{request.Parent}/streams/mock_stream_{id}";

            var info = new WriteStreamInfo(streamName, request.WriteStream.Type);
            _streams[streamName] = info;

            return Task.FromResult(new WriteStream
            {
                Name = streamName,
                Type = request.WriteStream.Type,
                CreateTime = Timestamp.FromDateTime(DateTime.UtcNow),
            });
        }

        public override async Task AppendRows(
            IAsyncStreamReader<AppendRowsRequest> requestStream,
            IServerStreamWriter<AppendRowsResponse> responseStream,
            ServerCallContext context)
        {
            long offset = 0;

            while (await requestStream.MoveNext())
            {
                var request = requestStream.Current;

                if (_streams.TryGetValue(request.WriteStream, out var streamInfo))
                {
                    if (request.ArrowRows != null)
                    {
                        if (request.ArrowRows.WriterSchema?.SerializedSchema != null)
                        {
                            streamInfo.SchemaBytes = request.ArrowRows.WriterSchema.SerializedSchema.ToByteArray();
                        }

                        if (request.ArrowRows.Rows?.SerializedRecordBatch != null)
                        {
                            streamInfo.RecordBatches.Add(request.ArrowRows.Rows.SerializedRecordBatch.ToByteArray());
                        }
                    }
                }

                var response = new AppendRowsResponse
                {
                    AppendResult = new AppendRowsResponse.Types.AppendResult
                    {
                        Offset = offset
                    }
                };

                await responseStream.WriteAsync(response);
                offset++;
            }
        }

        public override Task<FinalizeWriteStreamResponse> FinalizeWriteStream(FinalizeWriteStreamRequest request, ServerCallContext context)
        {
            if (_streams.TryGetValue(request.Name, out var streamInfo))
            {
                streamInfo.Finalized = true;
            }

            return Task.FromResult(new FinalizeWriteStreamResponse
            {
                RowCount = _streams.TryGetValue(request.Name, out var info) ? info.RecordBatches.Count : 0
            });
        }

        public override Task<BatchCommitWriteStreamsResponse> BatchCommitWriteStreams(BatchCommitWriteStreamsRequest request, ServerCallContext context)
        {
            foreach (var streamName in request.WriteStreams)
            {
                CommittedStreams.Add(streamName);
            }

            return Task.FromResult(new BatchCommitWriteStreamsResponse
            {
                CommitTime = Timestamp.FromDateTime(DateTime.UtcNow)
            });
        }

        /// <summary>
        /// Tracks information about a write stream for test verification.
        /// </summary>
        public class WriteStreamInfo
        {
            public string Name { get; }
            public WriteStream.Types.Type Type { get; }
            public byte[]? SchemaBytes { get; set; }
            public List<byte[]> RecordBatches { get; } = new();
            public bool Finalized { get; set; }

            public WriteStreamInfo(string name, WriteStream.Types.Type type)
            {
                Name = name;
                Type = type;
            }
        }
    }
}
