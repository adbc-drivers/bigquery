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
        [Theory]
        [InlineData(true)]  // MoveNextAsync throws the error
        [InlineData(false)] // .Current throws the error
        public async Task ReadChunkAsync_ReturnsNull_WhenStreamThrowsInvalidOperationException(bool moveNextThrowsError)
        {
            // Arrange
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);
            var mockReadRowsStream = new Mock<BigQueryReadClient.ReadRowsStream>();
            var mockAsyncResponseStream = new Mock<IAsyncStreamReader<ReadRowsResponse>>();

            if (moveNextThrowsError)
            {
                mockAsyncResponseStream
                    .Setup(s => s.MoveNext(It.IsAny<CancellationToken>()))
                    .Throws(new InvalidOperationException("No current element is available."));
            }
            else
            {
                mockAsyncResponseStream
                    .Setup(s => s.MoveNext(It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(true));

                mockAsyncResponseStream
                    .SetupGet(s => s.Current)
                    .Throws(new InvalidOperationException("No current element is available."));
            }

            AsyncResponseStream<ReadRowsResponse>? mockedResponseStream = typeof(AsyncResponseStream<ReadRowsResponse>)
                .GetConstructor(
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    new Type[] { typeof(IAsyncStreamReader<ReadRowsResponse>) },
                    null)?
                .Invoke(new object[] { mockAsyncResponseStream.Object }) as AsyncResponseStream<ReadRowsResponse>;

            Assert.NotNull(mockedResponseStream);

            mockReadRowsStream
                .Setup(c => c.GetResponseStream())
                .Returns(mockedResponseStream);

            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(mockReadRowsStream.Object);

            var statement = CreateBigQueryStatementForTest();

            // Act
            var result = await InvokeReadChunkAsync(statement, mockReadClient.Object, "test-stream");

            // Assert
            Assert.Null(result);
        }

        private static async Task<IArrowReader?> InvokeReadChunkAsync(
            BigQueryStatement statement,
            BigQueryReadClient client,
            string streamName)
        {
            var method = typeof(BigQueryStatement).GetMethod(
                "ReadChunkAsync",
                BindingFlags.NonPublic | BindingFlags.Instance);

            Assert.NotNull(method);

            var task = (Task<IArrowReader?>)method.Invoke(
                statement,
                new object?[] { client, streamName, null, false, CancellationToken.None })!;

            return await task;
        }

        private static BigQueryStatement CreateBigQueryStatementForTest()
        {
            var properties = new Dictionary<string, string>
            {
                ["projectid"] = "test-project"
            };

            var connection = new BigQueryConnection(properties);
            return new BigQueryStatement(connection);
        }
    }
}
