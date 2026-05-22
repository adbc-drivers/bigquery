/*
* Copyright (c) 2025 ADBC Drivers Contributors
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
using System.Net;
using Google;
using Grpc.Core;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    public class BigQueryUtilsTests
    {
        #region IsGrpcUnauthenticated tests

        [Fact]
        public void IsGrpcUnauthenticated_ReturnsTrue_ForRpcExceptionWithUnauthenticatedStatus()
        {
            var ex = new RpcException(new Status(StatusCode.Unauthenticated,
                "Request had invalid authentication credentials."));

            Assert.True(BigQueryUtils.IsGrpcUnauthenticated(ex));
        }

        [Fact]
        public void IsGrpcUnauthenticated_ReturnsFalse_ForRpcExceptionWithOtherStatus()
        {
            var ex = new RpcException(new Status(StatusCode.Unavailable, "Service unavailable"));

            Assert.False(BigQueryUtils.IsGrpcUnauthenticated(ex));
        }

        [Fact]
        public void IsGrpcUnauthenticated_ReturnsFalse_ForNonRpcException()
        {
            var ex = new InvalidOperationException("Something went wrong");

            Assert.False(BigQueryUtils.IsGrpcUnauthenticated(ex));
        }

        [Fact]
        public void IsGrpcUnauthenticated_ReturnsTrue_ForNestedRpcException()
        {
            var inner = new RpcException(new Status(StatusCode.Unauthenticated,
                "Request had invalid authentication credentials."));
            var outer = new InvalidOperationException("Wrapper", inner);

            Assert.True(BigQueryUtils.IsGrpcUnauthenticated(outer));
        }

        [Fact]
        public void IsGrpcUnauthenticated_ReturnsTrue_ForAggregateExceptionContainingUnauthenticated()
        {
            var rpcEx = new RpcException(new Status(StatusCode.Unauthenticated,
                "Request had invalid authentication credentials."));
            var aggregate = new AggregateException("Multiple errors", rpcEx);

            Assert.True(BigQueryUtils.IsGrpcUnauthenticated(aggregate));
        }

        [Fact]
        public void IsGrpcUnauthenticated_ReturnsFalse_ForRpcExceptionWithPermissionDenied()
        {
            // PermissionDenied (gRPC 7) is different from Unauthenticated (gRPC 16)
            var ex = new RpcException(new Status(StatusCode.PermissionDenied, "Access denied"));

            Assert.False(BigQueryUtils.IsGrpcUnauthenticated(ex));
        }

        #endregion

        #region TokenRequiresUpdate tests

        [Fact]
        public void TokenRequiresUpdate_ReturnsTrue_ForGoogleApiUnauthorized()
        {
            var ex = new GoogleApiException("BigQuery", "Unauthorized")
            {
                HttpStatusCode = HttpStatusCode.Unauthorized
            };

            Assert.True(BigQueryUtils.TokenRequiresUpdate(ex));
        }

        [Fact]
        public void TokenRequiresUpdate_ReturnsTrue_ForGrpcUnauthenticated()
        {
            var ex = new RpcException(new Status(StatusCode.Unauthenticated,
                "Request had invalid authentication credentials."));

            Assert.True(BigQueryUtils.TokenRequiresUpdate(ex));
        }

        [Fact]
        public void TokenRequiresUpdate_ReturnsFalse_ForGrpcUnavailable()
        {
            var ex = new RpcException(new Status(StatusCode.Unavailable, "Service unavailable"));

            Assert.False(BigQueryUtils.TokenRequiresUpdate(ex));
        }

        [Fact]
        public void TokenRequiresUpdate_ReturnsFalse_ForGenericException()
        {
            var ex = new Exception("Something failed");

            Assert.False(BigQueryUtils.TokenRequiresUpdate(ex));
        }

        [Fact]
        public void TokenRequiresUpdate_ReturnsTrue_ForNestedGrpcUnauthenticated()
        {
            var inner = new RpcException(new Status(StatusCode.Unauthenticated,
                "Request had invalid authentication credentials."));
            var outer = new Exception("Wrapper", inner);

            Assert.True(BigQueryUtils.TokenRequiresUpdate(outer));
        }

        [Fact]
        public void TokenRequiresUpdate_ReturnsFalse_ForGoogleApiForbidden()
        {
            // HTTP 403 Forbidden is not the same as 401 Unauthorized
            var ex = new GoogleApiException("BigQuery", "Forbidden")
            {
                HttpStatusCode = HttpStatusCode.Forbidden
            };

            Assert.False(BigQueryUtils.TokenRequiresUpdate(ex));
        }

        #endregion

        #region IsRetryableException tests

        [Theory]
        [InlineData(HttpStatusCode.InternalServerError)] // 500
        [InlineData(HttpStatusCode.BadGateway)]          // 502
        [InlineData(HttpStatusCode.ServiceUnavailable)]  // 503
        [InlineData(HttpStatusCode.GatewayTimeout)]      // 504
        public void IsRetryableException_ReturnsTrue_ForServerErrors(HttpStatusCode statusCode)
        {
            var ex = new GoogleApiException("BigQuery", "Server error")
            {
                HttpStatusCode = statusCode
            };

            Assert.True(BigQueryUtils.IsRetryableException(ex));
        }

        [Fact]
        public void IsRetryableException_ReturnsTrue_ForTooManyRequests()
        {
            var ex = new GoogleApiException("BigQuery", "Rate limited")
            {
                HttpStatusCode = (HttpStatusCode)429 // Too Many Requests
            };

            Assert.True(BigQueryUtils.IsRetryableException(ex));
        }

        [Theory]
        [InlineData(HttpStatusCode.BadRequest)]    // 400 - Invalid SQL
        [InlineData(HttpStatusCode.Unauthorized)]  // 401 - Auth (handled separately)
        [InlineData(HttpStatusCode.Forbidden)]     // 403 - Permission denied
        [InlineData(HttpStatusCode.NotFound)]      // 404 - Resource not found
        public void IsRetryableException_ReturnsFalse_ForClientErrors(HttpStatusCode statusCode)
        {
            var ex = new GoogleApiException("BigQuery", "Client error")
            {
                HttpStatusCode = statusCode
            };

            Assert.False(BigQueryUtils.IsRetryableException(ex));
        }

        [Theory]
        [InlineData(StatusCode.Unavailable)]
        [InlineData(StatusCode.DeadlineExceeded)]
        [InlineData(StatusCode.ResourceExhausted)]
        [InlineData(StatusCode.Aborted)]
        [InlineData(StatusCode.Internal)]
        public void IsRetryableException_ReturnsTrue_ForRetryableGrpcStatus(StatusCode statusCode)
        {
            var ex = new RpcException(new Status(statusCode, "Transient error"));

            Assert.True(BigQueryUtils.IsRetryableException(ex));
        }

        [Theory]
        [InlineData(StatusCode.InvalidArgument)]    // Bad request
        [InlineData(StatusCode.PermissionDenied)]   // Permission denied
        [InlineData(StatusCode.NotFound)]           // Resource not found
        [InlineData(StatusCode.Unauthenticated)]    // Auth (handled separately)
        [InlineData(StatusCode.FailedPrecondition)] // Invalid state
        public void IsRetryableException_ReturnsFalse_ForNonRetryableGrpcStatus(StatusCode statusCode)
        {
            var ex = new RpcException(new Status(statusCode, "Non-retryable error"));

            Assert.False(BigQueryUtils.IsRetryableException(ex));
        }

        [Fact]
        public void IsRetryableException_ReturnsTrue_ForBackendErrorReason()
        {
            var ex = new GoogleApiException("BigQuery", "Backend error")
            {
                HttpStatusCode = HttpStatusCode.BadRequest,
                Error = new Google.Apis.Requests.RequestError
                {
                    Errors = new[] { new Google.Apis.Requests.SingleError { Reason = "backendError" } }
                }
            };

            Assert.True(BigQueryUtils.IsRetryableException(ex));
        }

        [Fact]
        public void IsRetryableException_ReturnsTrue_ForInternalErrorReason()
        {
            var ex = new GoogleApiException("BigQuery", "Internal error")
            {
                HttpStatusCode = HttpStatusCode.BadRequest,
                Error = new Google.Apis.Requests.RequestError
                {
                    Errors = new[] { new Google.Apis.Requests.SingleError { Reason = "internalError" } }
                }
            };

            Assert.True(BigQueryUtils.IsRetryableException(ex));
        }

        [Fact]
        public void IsRetryableException_ReturnsTrue_ForRateLimitExceededReason()
        {
            var ex = new GoogleApiException("BigQuery", "Rate limit exceeded")
            {
                HttpStatusCode = HttpStatusCode.Forbidden,
                Error = new Google.Apis.Requests.RequestError
                {
                    Errors = new[] { new Google.Apis.Requests.SingleError { Reason = "rateLimitExceeded" } }
                }
            };

            Assert.True(BigQueryUtils.IsRetryableException(ex));
        }

        [Fact]
        public void IsRetryableException_ReturnsFalse_ForInvalidQueryReason()
        {
            var ex = new GoogleApiException("BigQuery", "Invalid query")
            {
                HttpStatusCode = HttpStatusCode.BadRequest,
                Error = new Google.Apis.Requests.RequestError
                {
                    Errors = new[] { new Google.Apis.Requests.SingleError { Reason = "invalidQuery" } }
                }
            };

            Assert.False(BigQueryUtils.IsRetryableException(ex));
        }

        [Fact]
        public void IsRetryableException_ReturnsTrue_ForConnectionResetMessage()
        {
            var ex = new Exception("Connection reset by peer");

            Assert.True(BigQueryUtils.IsRetryableException(ex));
        }

        [Fact]
        public void IsRetryableException_ReturnsTrue_ForConnectionRefusedMessage()
        {
            var ex = new Exception("Connection refused");

            Assert.True(BigQueryUtils.IsRetryableException(ex));
        }

        [Fact]
        public void IsRetryableException_ReturnsTrue_ForNestedRetryableException()
        {
            var inner = new RpcException(new Status(StatusCode.Unavailable, "Transient error"));
            var outer = new Exception("Wrapper", inner);

            Assert.True(BigQueryUtils.IsRetryableException(outer));
        }

        [Fact]
        public void IsRetryableException_ReturnsFalse_ForGenericException()
        {
            var ex = new Exception("Unknown error");

            Assert.False(BigQueryUtils.IsRetryableException(ex));
        }

        #endregion
    }
}
