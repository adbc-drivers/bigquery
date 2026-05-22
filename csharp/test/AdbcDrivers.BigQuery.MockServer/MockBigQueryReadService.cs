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
using System.Threading.Tasks;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf;
using Grpc.Core;

namespace AdbcDrivers.BigQuery.MockServer
{
    /// <summary>
    /// A mock implementation of the BigQuery Storage Read gRPC service.
    /// Returns pre-configured Arrow data for CreateReadSession and ReadRows.
    /// </summary>
    public class MockBigQueryReadService : BigQueryRead.BigQueryReadBase
    {
        private readonly ConcurrentDictionary<string, ReadSession> _sessions = new();
        private readonly ConcurrentDictionary<string, ReadSession> _streamToSession = new();
        private readonly ConcurrentDictionary<string, (byte[] batch, long rowCount)> _streamData = new();

        /// <summary>
        /// Default Arrow schema bytes returned when no table-specific config is found.
        /// </summary>
        public byte[]? DefaultArrowSchema { get; set; }

        /// <summary>
        /// Default Arrow record batch bytes returned when no table-specific config is found.
        /// </summary>
        public byte[]? DefaultArrowBatch { get; set; }

        /// <summary>
        /// Default row count returned for each batch.
        /// </summary>
        public long DefaultRowCount { get; set; } = 1;

        /// <summary>
        /// Configures mock data for a table. When a CreateReadSession is received for this table,
        /// the service will return a session with one stream containing this Arrow data.
        /// </summary>
        /// <param name="tableName">The full table name (projects/X/datasets/Y/tables/Z).</param>
        /// <param name="arrowSchema">Serialized Arrow schema bytes.</param>
        /// <param name="arrowBatch">Serialized Arrow record batch bytes.</param>
        /// <param name="rowCount">Number of rows in the batch.</param>
        public void ConfigureTable(string tableName, byte[] arrowSchema, byte[] arrowBatch, long rowCount)
        {
            string sessionName = $"projects/mock-project/locations/us/sessions/mock-session-{Guid.NewGuid():N}";
            string streamName = $"{sessionName}/streams/mock-stream-0";

            var session = new ReadSession
            {
                Name = sessionName,
                Table = tableName,
                DataFormat = DataFormat.Arrow,
                ArrowSchema = new ArrowSchema { SerializedSchema = ByteString.CopyFrom(arrowSchema) },
            };
            session.Streams.Add(new ReadStream { Name = streamName });

            _sessions[tableName] = session;
            _streamToSession[streamName] = session;
            _streamData[streamName] = (arrowBatch, rowCount);
        }

        public override Task<ReadSession> CreateReadSession(CreateReadSessionRequest request, ServerCallContext context)
        {
            string tableName = request.ReadSession.Table;

            if (_sessions.TryGetValue(tableName, out var session))
            {
                return Task.FromResult(session);
            }

            // Fall back to default data if configured
            if (DefaultArrowSchema != null)
            {
                string sessionName = $"projects/mock-project/locations/us/sessions/default-{Guid.NewGuid():N}";
                string streamName = $"{sessionName}/streams/default-stream-0";

                var defaultSession = new ReadSession
                {
                    Name = sessionName,
                    Table = tableName,
                    DataFormat = DataFormat.Arrow,
                    ArrowSchema = new ArrowSchema { SerializedSchema = ByteString.CopyFrom(DefaultArrowSchema) },
                };
                defaultSession.Streams.Add(new ReadStream { Name = streamName });

                _streamToSession[streamName] = defaultSession;

                // Store stream data for ReadRows
                if (DefaultArrowBatch != null)
                {
                    _streamData[streamName] = (DefaultArrowBatch, DefaultRowCount);
                }

                return Task.FromResult(defaultSession);
            }

            // Return an empty session if nothing configured
            var emptySession = new ReadSession
            {
                Name = $"projects/mock-project/locations/us/sessions/empty-{Guid.NewGuid():N}",
                Table = tableName,
                DataFormat = DataFormat.Arrow,
                ArrowSchema = new ArrowSchema { SerializedSchema = ByteString.Empty },
            };
            return Task.FromResult(emptySession);
        }

        public override async Task ReadRows(ReadRowsRequest request, IServerStreamWriter<ReadRowsResponse> responseStream, ServerCallContext context)
        {
            string streamName = request.ReadStream;

            if (_streamData.TryGetValue(streamName, out var streamData))
            {
                byte[]? schemaData = _streamToSession.TryGetValue(streamName, out var session)
                    ? session.ArrowSchema?.SerializedSchema?.ToByteArray()
                    : null;

                // Fall back to default schema
                schemaData ??= DefaultArrowSchema;

                var response = new ReadRowsResponse
                {
                    ArrowRecordBatch = new ArrowRecordBatch
                    {
                        SerializedRecordBatch = ByteString.CopyFrom(streamData.batch),
                    },
                    RowCount = streamData.rowCount,
                };

                if (schemaData != null)
                {
                    response.ArrowSchema = new ArrowSchema
                    {
                        SerializedSchema = ByteString.CopyFrom(schemaData),
                    };
                }

                await responseStream.WriteAsync(response);
            }
        }
    }
}
