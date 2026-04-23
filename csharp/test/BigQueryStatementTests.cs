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
using Google.Apis.Auth.OAuth2;
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
            int retryDelayMs = 100,
            CancellationToken streamCancellationToken = default,
            Func<Task>? updateToken = null)
        {
            // Use reflection to create ReadRowsStream since it's a private nested class.
            // Constructor: ReadRowsStream(IActivityTracer, TokenProtectedReadClientManger, string, int, int, CancellationToken)
            var readRowsStreamType = typeof(BigQueryStatement).GetNestedType("ReadRowsStream", BindingFlags.NonPublic);
            Assert.NotNull(readRowsStreamType);

            var clientMgr = new TokenProtectedReadClientManger(GoogleCredential.FromAccessToken("test-token"));
            // Replace the read client with the mock
            typeof(TokenProtectedReadClientManger)
                .GetField("bigQueryReadClient", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(clientMgr, client);
            if (updateToken != null)
            {
                clientMgr.UpdateToken = updateToken;
            }

            return (IArrowArrayStream)Activator.CreateInstance(
                readRowsStreamType,
                BindingFlags.Instance | BindingFlags.Public,
                null,
                new object[] { CreateStubTracer(), clientMgr, streamName, maxRetryAttempts, retryDelayMs, streamCancellationToken },
                null)!;
        }

        [Fact]
        public async Task ReadRowsStream_PerCallCancellationToken_StopsRetryDelay()
        {
            // Arrange - MoveNextAsync always throws, retries=2, delay=5000ms
            // but per-call token is cancelled immediately so delay should not complete
            var perCallCts = new CancellationTokenSource();
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() => CreateFailingReadRowsStream(
                    new RpcException(new Status(StatusCode.Unavailable, "Stream temporarily unavailable"))));

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 2, retryDelayMs: 5000);

            // Cancel the per-call token after a short delay so it fires during the retry delay
            perCallCts.CancelAfter(100);

            // Act & Assert - should throw OperationCanceledException quickly, not wait the full 5s delay
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                stream.ReadNextRecordBatchAsync(perCallCts.Token).AsTask());
            sw.Stop();

            // Should complete well under the 5000ms retry delay
            Assert.True(sw.ElapsedMilliseconds < 3000, $"Expected cancellation to stop the retry delay quickly, but took {sw.ElapsedMilliseconds}ms");
        }

        [Fact]
        public async Task ReadRowsStream_PerCallCancellationToken_AlreadyCancelled_ThrowsImmediately()
        {
            // Arrange - per-call token is already cancelled before calling ReadNextRecordBatchAsync
            var perCallCts = new CancellationTokenSource();
            perCallCts.Cancel();

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

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 2, retryDelayMs: 5000);

            // Act & Assert - should throw immediately without retrying.
            // When the token is already cancelled and MoveNext throws, the code detects
            // cancellation and re-throws the original exception instead of retrying.
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await Assert.ThrowsAsync<RpcException>(() =>
                stream.ReadNextRecordBatchAsync(perCallCts.Token).AsTask());
            sw.Stop();

            // Should not have retried (only 1 call to ReadRows for initial setup)
            Assert.Equal(1, callCount);
            // Should complete nearly instantly, not wait for retry delays
            Assert.True(sw.ElapsedMilliseconds < 1000, $"Expected immediate throw but took {sw.ElapsedMilliseconds}ms");
        }

        [Fact]
        public async Task ReadRowsStream_StreamLevelCancellationToken_StillWorks_WhenPerCallTokenIsDefault()
        {
            // Arrange - stream-level token is cancelled, per-call token is CancellationToken.None
            var streamCts = new CancellationTokenSource();

            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() => CreateFailingReadRowsStream(
                    new RpcException(new Status(StatusCode.Unavailable, "Stream temporarily unavailable"))));

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 2, retryDelayMs: 5000, streamCancellationToken: streamCts.Token);

            // Cancel the stream-level token after a short delay
            streamCts.CancelAfter(100);

            // Act & Assert - should throw OperationCanceledException via the stream-level token
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                stream.ReadNextRecordBatchAsync(CancellationToken.None).AsTask());
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 3000, $"Expected stream-level cancellation to stop the retry delay quickly, but took {sw.ElapsedMilliseconds}ms");
        }

        [Fact]
        public async Task ReadRowsStream_PerCallCancellationToken_CancelsBeforeMoveNext()
        {
            // Arrange - MoveNextAsync blocks until cancellation, per-call token fires quickly
            var perCallCts = new CancellationTokenSource();

            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() =>
                {
                    return CreateMockReadRowsStream(mock =>
                        mock.Setup(s => s.MoveNext(It.IsAny<CancellationToken>()))
                            .Returns(async (CancellationToken ct) =>
                            {
                                // Simulate a slow gRPC call that respects cancellation
                                await Task.Delay(Timeout.Infinite, ct);
                                return false;
                            }));
                });

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 0);

            // Cancel after a short delay
            perCallCts.CancelAfter(200);

            // Act & Assert - per-call cancellation should stop the blocking MoveNext
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                stream.ReadNextRecordBatchAsync(perCallCts.Token).AsTask());
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 3000, $"Expected per-call cancellation to stop MoveNext quickly, but took {sw.ElapsedMilliseconds}ms");
        }

        #region GetEffectiveQueryResultsTimeout tests

        [Fact]
        public void GetEffectiveQueryResultsTimeout_ReturnsNull_WhenNeitherConnectionNorStatementSetsTimeout()
        {
            // Arrange - no timeout set anywhere
            var connection = new BigQueryConnection(new Dictionary<string, string>());
            var statement = new BigQueryStatement(connection);

            // Act
            TimeSpan? result = InvokeGetEffectiveQueryResultsTimeout(statement);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public void GetEffectiveQueryResultsTimeout_ReturnsConnectionValue_WhenOnlyConnectionSetsTimeout()
        {
            // Arrange - connection sets timeout to 120 seconds
            var connection = new BigQueryConnection(new Dictionary<string, string>
            {
                { BigQueryParameters.GetQueryResultsOptionsTimeout, "120" }
            });
            var statement = new BigQueryStatement(connection);

            // Act
            TimeSpan? result = InvokeGetEffectiveQueryResultsTimeout(statement);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(TimeSpan.FromSeconds(120), result!.Value);
        }

        [Fact]
        public void GetEffectiveQueryResultsTimeout_ReturnsStatementValue_WhenOnlyStatementSetsTimeout()
        {
            // Arrange - no connection timeout, statement sets 60 seconds
            var connection = new BigQueryConnection(new Dictionary<string, string>());
            var statement = new BigQueryStatement(connection);
            statement.SetOption(BigQueryParameters.GetQueryResultsOptionsTimeout, "60");

            // Act
            TimeSpan? result = InvokeGetEffectiveQueryResultsTimeout(statement);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(TimeSpan.FromSeconds(60), result!.Value);
        }

        [Fact]
        public void GetEffectiveQueryResultsTimeout_StatementOverridesConnection()
        {
            // Arrange - connection sets 120s, statement overrides to 30s
            var connection = new BigQueryConnection(new Dictionary<string, string>
            {
                { BigQueryParameters.GetQueryResultsOptionsTimeout, "120" }
            });
            var statement = new BigQueryStatement(connection);
            statement.SetOption(BigQueryParameters.GetQueryResultsOptionsTimeout, "30");

            // Act
            TimeSpan? result = InvokeGetEffectiveQueryResultsTimeout(statement);

            // Assert - statement value should take precedence
            Assert.NotNull(result);
            Assert.Equal(TimeSpan.FromSeconds(30), result!.Value);
        }

        [Fact]
        public void GetEffectiveQueryResultsTimeout_FallsBackToConnection_WhenStatementNotSet()
        {
            // Arrange - connection sets 300s, statement does not override
            var connection = new BigQueryConnection(new Dictionary<string, string>
            {
                { BigQueryParameters.GetQueryResultsOptionsTimeout, "300" }
            });
            var statement = new BigQueryStatement(connection);

            // Act
            TimeSpan? result = InvokeGetEffectiveQueryResultsTimeout(statement);

            // Assert - should fall back to connection value
            Assert.NotNull(result);
            Assert.Equal(TimeSpan.FromSeconds(300), result!.Value);
        }

        [Fact]
        public void SetOption_ThrowsArgumentException_WhenTimeoutIsNotPositiveInteger()
        {
            var connection = new BigQueryConnection(new Dictionary<string, string>());
            var statement = new BigQueryStatement(connection);

            Assert.Throws<ArgumentException>(() =>
                statement.SetOption(BigQueryParameters.GetQueryResultsOptionsTimeout, "0"));
            Assert.Throws<ArgumentException>(() =>
                statement.SetOption(BigQueryParameters.GetQueryResultsOptionsTimeout, "-5"));
            Assert.Throws<ArgumentException>(() =>
                statement.SetOption(BigQueryParameters.GetQueryResultsOptionsTimeout, "abc"));
        }

        private static TimeSpan? InvokeGetEffectiveQueryResultsTimeout(BigQueryStatement statement)
        {
            const BindingFlags bindingAttr = BindingFlags.NonPublic | BindingFlags.Instance;
            var method = typeof(BigQueryStatement).GetMethod("GetEffectiveQueryResultsTimeout", bindingAttr);
            Assert.NotNull(method);
            return (TimeSpan?)method!.Invoke(statement, null);
        }

        #endregion

        #region ReadRowsStream Unauthenticated tests

        [Fact]
        public async Task ReadRowsStream_ThrowsUnauthenticated_ImmediatelyWithoutRetries()
        {
            // Arrange - MoveNextAsync throws gRPC Unauthenticated and no UpdateToken is set.
            // Without a token refresh delegate, the driver should throw immediately.
            int callCount = 0;
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() =>
                {
                    callCount++;
                    return CreateFailingReadRowsStream(
                        new RpcException(new Status(StatusCode.Unauthenticated,
                            "Request had invalid authentication credentials. Expected OAuth 2 access token.")));
                });

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 5, retryDelayMs: 10);

            // Act & Assert
            var ex = await Assert.ThrowsAsync<AdbcException>(() =>
                stream.ReadNextRecordBatchAsync(CancellationToken.None).AsTask());

            Assert.Equal(AdbcStatusCode.Unauthenticated, ex.Status);
            Assert.Contains("Authentication failed", ex.Message);
            Assert.Contains("test-stream", ex.Message);
            // Should NOT have retried - only 1 call to ReadRows (the initial setup)
            Assert.Equal(1, callCount);
        }

        [Fact]
        public async Task ReadRowsStream_ThrowsUnauthenticated_PreservesInnerException()
        {
            // Arrange - the original RpcException should be preserved as InnerException
            var rpcException = new RpcException(new Status(StatusCode.Unauthenticated,
                "Request had invalid authentication credentials."));

            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);
            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() => CreateFailingReadRowsStream(rpcException));

            var stream = CreateReadRowsStreamForTest(mockReadClient.Object, "test-stream", maxRetryAttempts: 3, retryDelayMs: 10);

            // Act
            var ex = await Assert.ThrowsAsync<AdbcException>(() =>
                stream.ReadNextRecordBatchAsync(CancellationToken.None).AsTask());

            // Assert - inner exception is the original RpcException
            Assert.IsType<RpcException>(ex.InnerException);
            Assert.Equal(StatusCode.Unauthenticated, ((RpcException)ex.InnerException).StatusCode);
        }

        [Fact]
        public async Task ReadRowsStream_RetriesNonAuthErrors_ThenFailsWithIOError()
        {
            // Arrange - gRPC Unavailable errors should still be retried and surface as IOError
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

            // Act
            var ex = await Assert.ThrowsAsync<AdbcException>(() =>
                stream.ReadNextRecordBatchAsync(CancellationToken.None).AsTask());

            // Assert - non-auth errors still get IOError status and do retry
            Assert.Equal(AdbcStatusCode.IOError, ex.Status);
            Assert.Equal(3, callCount); // 1 initial + 2 retries
        }

        [Fact]
        public async Task ReadRowsStream_RefreshesToken_OnUnauthenticated_WhenUpdateTokenIsSet()
        {
            // Arrange - First MoveNextAsync throws Unauthenticated, but UpdateToken is set.
            // After token refresh, the reconnected stream should succeed.
            int callCount = 0;
            bool tokenRefreshed = false;
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() =>
                {
                    callCount++;
                    if (callCount == 1)
                    {
                        // First call: stream opens for lazy init
                        return CreateFailingReadRowsStream(
                            new RpcException(new Status(StatusCode.Unauthenticated,
                                "Request had invalid authentication credentials.")));
                    }
                    // After token refresh: return empty stream (no rows)
                    return CreateEmptyReadRowsStream();
                });

            var stream = CreateReadRowsStreamForTest(
                mockReadClient.Object, "test-stream",
                maxRetryAttempts: 5, retryDelayMs: 10,
                updateToken: () => { tokenRefreshed = true; return Task.CompletedTask; });

            // Act - ReadNextRecordBatchAsync should refresh the token and reconnect
            var result = await stream.ReadNextRecordBatchAsync(CancellationToken.None);

            // Assert
            Assert.Null(result); // empty stream returns null
            Assert.True(tokenRefreshed, "UpdateToken should have been called");
            Assert.Equal(2, callCount); // 1 initial (failed) + 1 after refresh
        }

        [Fact]
        public async Task ReadRowsStream_ThrowsUnauthenticated_WhenTokenRefreshAlsoFails()
        {
            // Arrange - Unauthenticated error AND UpdateToken throws (e.g., Entra ID token also expired).
            // Should still throw AdbcException with Unauthenticated status.
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(() => CreateFailingReadRowsStream(
                    new RpcException(new Status(StatusCode.Unauthenticated,
                        "Request had invalid authentication credentials."))));

            var stream = CreateReadRowsStreamForTest(
                mockReadClient.Object, "test-stream",
                maxRetryAttempts: 5, retryDelayMs: 10,
                updateToken: () => throw new Exception("Entra ID token also expired"));

            // Act
            var ex = await Assert.ThrowsAsync<AdbcException>(() =>
                stream.ReadNextRecordBatchAsync(CancellationToken.None).AsTask());

            // Assert - still surfaces as Unauthenticated
            Assert.Equal(AdbcStatusCode.Unauthenticated, ex.Status);
            Assert.Contains("Authentication failed", ex.Message);
        }

        #endregion
    }
}
