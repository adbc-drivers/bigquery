/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
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
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Google.Api.Gax.Grpc;
using Google.Cloud.BigQuery.Storage.V1;
using Grpc.Core;
using Moq;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    public class BigQueryStatementTests
    {
        [Fact]
        public async Task ReadRowsStream_ThrowsAdbcException_WhenMoveNextFailsAfterRetries()
        {
            // Arrange - MoveNextAsync always throws, so retries are exhausted
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() => CreateFailingReadRowsStream());

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 0);

            // Act & Assert - exception should propagate after retries are exhausted
            var ex = await Assert.ThrowsAsync<AdbcException>(() =>
                stream.ReadNextRecordBatchAsync(CancellationToken.None).AsTask());
            Assert.Contains("test-stream", ex.Message);
        }

        [Fact]
        public async Task ReadRowsStream_ThrowsAdbcException_AfterExhaustingRetries()
        {
            // Arrange - MoveNextAsync always throws with a gRPC error,
            // and maxRetries=2 so it retries twice then fails.
            int callCount = 0;
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() =>
                {
                    callCount++;
                    return CreateFailingReadRowsStream(
                        new RpcException(new Status(StatusCode.Unavailable, "Stream temporarily unavailable")));
                });

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 2, retryDelayMs: 10);

            // Act & Assert - should retry twice then throw AdbcException
            var ex = await Assert.ThrowsAsync<AdbcException>(() =>
                stream.ReadNextRecordBatchAsync(CancellationToken.None).AsTask());
            Assert.Contains("test-stream", ex.Message);
            // 1 initial call + 2 retries = 3 calls to ReadRows (1 initial + 2 reconnects)
            Assert.Equal(3, callCount);
        }

        [Fact]
        public async Task ReadRowsStream_RetriesAndSucceeds_WhenMoveNextFailsThenRecovers()
        {
            // Arrange - First stream fails on MoveNext, second stream (after reconnect) returns data
            int callCount = 0;
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        // First call fails
                        return CreateFailingReadRowsStream(
                            new RpcException(new Status(StatusCode.Unavailable, "Stream temporarily unavailable")));
                    }
                    // Second call (retry) returns an empty stream (MoveNext returns false = no data)
                    return CreateEmptyReadRowsStream();
                });

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 2, retryDelayMs: 10);

            // Act - should retry and get null (empty stream after reconnect)
            var result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);

            // Assert - reconnect succeeded, returned null because stream was empty
            Assert.Null(result);
            Assert.Equal(2, callCount); // 1 initial + 1 reconnect
        }

        [Fact]
        public async Task ReadRowsStream_ThrowsException_WhenCurrentThrowsAfterSuccessfulMoveNext()
        {
            // Arrange - MoveNextAsync succeeds but .Current throws
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() => CreateCurrentThrowsReadRowsStream());

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 0);

            // Act & Assert - exception should propagate, not be silently swallowed
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                stream.ReadNextRecordBatchAsync(CancellationToken.None).AsTask());
        }

        [Fact]
        public async Task ReadRowsStream_ReturnsNull_WhenMoveNextReturnsFalse()
        {
            // Arrange - MoveNextAsync returns false (empty stream)
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() => CreateEmptyReadRowsStream());

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream");

            // Act
            var result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);

            // Assert - empty stream returns null
            Assert.Null(result);
        }

        private static BigQueryReadClient.ReadRowsStream CreateMockReadRowsStream(Action<Mock<IAsyncStreamReader<ReadRowsResponse>>> configureMock)
        {
            var mockAsyncResponseStream = new Mock<IAsyncStreamReader<ReadRowsResponse>>();
            configureMock(mockAsyncResponseStream);

            var mockedResponseStream = typeof(AsyncResponseStream<ReadRowsResponse>)
                .GetConstructor(
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    new Type[] { typeof(IAsyncStreamReader<ReadRowsResponse>) },
                    null)?
                .Invoke(new object[] { mockAsyncResponseStream.Object }) as AsyncResponseStream<ReadRowsResponse>;

            var mockReadRowsStream = new Mock<BigQueryReadClient.ReadRowsStream>();
            mockReadRowsStream
                .Setup(c => c.GetResponseStream())
                .Returns(mockedResponseStream!);

            return mockReadRowsStream.Object;
        }

        private static BigQueryReadClient.ReadRowsStream CreateFailingReadRowsStream(Exception? exception = null)
        {
            exception ??= new InvalidOperationException("No current element is available.");
            return CreateMockReadRowsStream(mock =>
                mock.Setup(s => s.MoveNext(It.IsAny<CancellationToken>()))
                    .Throws(exception));
        }

        private static BigQueryReadClient.ReadRowsStream CreateCurrentThrowsReadRowsStream()
        {
            return CreateMockReadRowsStream(mock =>
            {
                mock.Setup(s => s.MoveNext(It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(true));
                mock.SetupGet(s => s.Current)
                    .Throws(new InvalidOperationException("No current element is available."));
            });
        }

        private static BigQueryReadClient.ReadRowsStream CreateEmptyReadRowsStream()
        {
            return CreateMockReadRowsStream(mock =>
                mock.Setup(s => s.MoveNext(It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(false)));
        }

        private static IActivityTracer CreateStubTracer()
        {
            var trace = new ActivityTrace("BigQueryStatementTests");
            var mock = new Mock<IActivityTracer>();
            mock.Setup(t => t.Trace).Returns(trace);
            mock.Setup(t => t.TraceParent).Returns((string?)null);
            return mock.Object;
        }

        private static IArrowArrayStream CreateReadRowsStreamForTest(
            BigQueryReadClient client,
            string streamName,
            int maxRetryAttempts = 3,
            int retryDelayMs = 100)
        {
            // Use reflection to create ReadRowsStream since it's a private nested class.
            // Constructor: ReadRowsStream(IActivityTracer, BigQueryReadClient, string, int, int, CancellationToken)
            var readRowsStreamType = typeof(BigQueryStatement).GetNestedType("ReadRowsStream", BindingFlags.NonPublic);
            Assert.NotNull(readRowsStreamType);

            return (IArrowArrayStream)Activator.CreateInstance(
                readRowsStreamType,
                BindingFlags.Instance | BindingFlags.Public,
                null,
                new object[] { CreateStubTracer(), client, streamName, maxRetryAttempts, retryDelayMs, CancellationToken.None },
                null)!;
        }
    }
}
