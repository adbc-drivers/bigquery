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
using System.Diagnostics;
using Google;
using Google.Apis.Requests;
using Grpc.Core;

namespace AdbcDrivers.BigQuery
{
    internal class BigQueryUtils
    {
        public static bool TokenRequiresUpdate(Exception ex)
        {
            bool result = false;

            if (ex is GoogleApiException gaex && gaex.HttpStatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                result = true;
            }
            else if (IsGrpcUnauthenticated(ex))
            {
                result = true;
            }

            return result;
        }

        internal static bool IsGrpcUnauthenticated(Exception ex)
        {
            return ContainsException<RpcException>(ex, out RpcException? rpcEx)
                && rpcEx!.StatusCode == Grpc.Core.StatusCode.Unauthenticated;
        }

        /// <summary>
        /// Determines if an exception represents a transient error that should be retried.
        /// Non-transient errors (invalid SQL, permission issues, etc.) should fail immediately
        /// to preserve fast feedback and error fidelity.
        /// </summary>
        /// <remarks>
        /// Retryable conditions:
        /// - HTTP 429 (Too Many Requests)
        /// - HTTP 5xx (Server errors: 500, 502, 503, 504)
        /// - gRPC Unavailable, DeadlineExceeded, ResourceExhausted, Aborted, Internal
        /// - GoogleApiException with "backendError" or "internalError" reason
        /// - Connection-related errors (connection reset, connection refused)
        /// </remarks>
        internal static bool IsRetryableException(Exception ex)
        {
            // Check for GoogleApiException (HTTP errors)
            if (ContainsException<GoogleApiException>(ex, out GoogleApiException? googleEx))
            {
                // Retryable HTTP status codes
                int statusCode = (int)googleEx!.HttpStatusCode;
                if (statusCode == 429 || // Too Many Requests
                    statusCode >= 500)   // Server errors (500, 502, 503, 504, etc.)
                {
                    return true;
                }

                // Check for retryable error reasons
                if (googleEx.Error?.Errors != null && googleEx.Error.Errors.Count > 0)
                {
                    string? reason = googleEx.Error.Errors[0].Reason;
                    if (reason == "backendError" || reason == "internalError" || reason == "rateLimitExceeded")
                    {
                        return true;
                    }
                }

                // Non-retryable: 400 (Bad Request/Invalid SQL), 401, 403 (Permission), 404, etc.
                return false;
            }

            // Check for gRPC exceptions
            if (ContainsException<RpcException>(ex, out RpcException? rpcEx))
            {
                // Retryable gRPC status codes
                switch (rpcEx!.StatusCode)
                {
                    case StatusCode.Unavailable:       // Service unavailable
                    case StatusCode.DeadlineExceeded:  // Timeout
                    case StatusCode.ResourceExhausted: // Rate limiting
                    case StatusCode.Aborted:           // Conflict/retry
                    case StatusCode.Internal:          // Internal server error
                        return true;
                    default:
                        // Non-retryable: InvalidArgument, PermissionDenied, NotFound, etc.
                        return false;
                }
            }

            // Check for connection-related errors
            string message = ex.Message?.ToLowerInvariant() ?? string.Empty;
            if (message.Contains("connection reset") ||
                message.Contains("connection refused") ||
                message.Contains("http2: stream closed") ||
                ex is System.IO.IOException)
            {
                return true;
            }

            // Check inner exceptions
            if (ex.InnerException != null && ex.InnerException != ex)
            {
                return IsRetryableException(ex.InnerException);
            }

            // Default: not retryable (fail fast for unknown errors)
            return false;
        }

        internal static string BigQueryAssemblyName = GetAssemblyName(typeof(BigQueryConnection));

        internal static string BigQueryAssemblyVersion = GetAssemblyVersion(typeof(BigQueryConnection));

        internal static string GetAssemblyName(Type type) => type.Assembly.GetName().Name!;

        internal static string GetAssemblyVersion(Type type) => FileVersionInfo.GetVersionInfo(type.Assembly.Location).ProductVersion ?? string.Empty;

        public static bool ContainsException<T>(Exception exception, out T? containedException) where T : Exception
        {
            Exception? e = exception;
            while (e != null)
            {
                if (e is T ce)
                {
                    containedException = ce;
                    return true;
                }
                else if (e is AggregateException aggregateException)
                {
                    foreach (Exception? ex in aggregateException.InnerExceptions)
                    {
                        if (ContainsException(ex, out T? inner))
                        {
                            containedException = inner;
                            return true;
                        }
                    }
                }
                e = e.InnerException;
            }

            containedException = null;
            return false;
        }

        internal static TagList BuildExceptionTagList(int retryCount, Exception ex, IEnumerable<KeyValuePair<string, object?>>? additionalTags = null)
        {
            List<KeyValuePair<string, object?>> tags = [new KeyValuePair<string, object?>("retry.attempt", retryCount)];

            if (additionalTags != null)
            {
                tags.AddRange(additionalTags);
            }

            if (ex is RpcException rpcEx)
            {
                tags.AddRange([
                    new($"retry.attempt_{retryCount}.grpc_status_code", rpcEx.StatusCode.ToString()),
                    new($"retry.attempt_{retryCount}.grpc_status_detail", rpcEx.Status.Detail),
                ]);
            }
            else if (ex is GoogleApiException googleEx)
            {
                tags.AddRange([
                    new($"retry.attempt_{retryCount}.http_status_code", (int)googleEx.HttpStatusCode),
                    new($"retry.attempt_{retryCount}.error_code", googleEx.Error?.Code),
                    new($"retry.attempt_{retryCount}.error_message", googleEx.Error?.Message),
                ]);
                if (googleEx.Error?.Errors != null)
                {
                    for (int i = 0; i < googleEx.Error.Errors.Count; i++)
                    {
                        SingleError error = googleEx.Error.Errors[i];
                        tags.Add(new($"retry.attempt_{retryCount}.error_{i}_details", error.ToString()));
                    }
                }
            }

            return new TagList(tags.ToArray());
        }
    }
}
