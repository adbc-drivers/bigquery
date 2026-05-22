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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.BigQuery
{
    /// <summary>
    /// Class that will retry calling a method with a backoff.
    /// Only transient errors (server errors, rate limiting, connection issues) are retried.
    /// Non-transient errors (invalid SQL, permission issues) fail immediately.
    /// </summary>
    internal class RetryManager
    {
        public static async Task<T> ExecuteWithRetriesAsync<T>(
            ITokenProtectedResource tokenProtectedResource,
            Func<Task<T>> action,
            Activity? activity,
            int maxRetries = 5,
            int initialDelayMilliseconds = 200,
            CancellationToken cancellationToken = default)
        {
            if (action == null)
            {
                throw new AdbcException("There is no method to retry", AdbcStatusCode.InvalidArgument);
            }

            int attempt = 0;
            int tokenRefreshAttempts = 0;
            int maxAttempts = maxRetries + 1; // maxRetries=0 means 1 attempt, maxRetries=5 means 6 attempts
            int delay = initialDelayMilliseconds;

            while (attempt < maxAttempts)
            {
                try
                {
                    T result = await action();
                    return result;
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    // Note: OperationCanceledException could be thrown from the call,
                    // but we only want to break out when the cancellation was requested from the caller.
                    activity?.AddException(ex, BigQueryUtils.BuildExceptionTagList(attempt, ex));

                    // Check if this is an authentication error that requires token refresh
                    bool isAuthError = tokenProtectedResource?.TokenRequiresUpdate(ex) == true;

                    // Check if this is a retryable transient error
                    bool isRetryable = BigQueryUtils.IsRetryableException(ex);

                    // Only retry if it's an auth error (with token refresh) or a transient error
                    if (!isAuthError && !isRetryable)
                    {
                        // Non-retryable error: fail fast to preserve error fidelity
                        activity?.AddBigQueryTag("retry.skipped", "non_retryable_error");
                        throw;
                    }

                    attempt++;
                    if (attempt >= maxAttempts)
                    {
                        // Build a clear error message that describes what actually happened
                        string tokenRefreshInfo = tokenRefreshAttempts > 0
                            ? $" ({tokenRefreshAttempts} token refresh(es) attempted)"
                            : string.Empty;

                        AdbcStatusCode statusCode = isAuthError
                            ? AdbcStatusCode.Unauthenticated
                            : AdbcStatusCode.UnknownError;

                        activity?.AddBigQueryTag("retry.token_refresh_attempts", tokenRefreshAttempts);

                        throw new AdbcException(
                            $"Operation failed after {maxAttempts} attempt(s){tokenRefreshInfo}. Last exception: {ex.GetType().Name}: {ex.Message}",
                            statusCode,
                            ex);
                    }

                    // Attempt token refresh if needed
                    if (isAuthError && tokenProtectedResource?.UpdateToken != null)
                    {
                        tokenRefreshAttempts++;
                        activity?.AddBigQueryTag("update_token.status", "Required");
                        activity?.AddBigQueryTag("update_token.attempt", tokenRefreshAttempts);
                        await tokenProtectedResource.UpdateToken();
                        activity?.AddBigQueryTag("update_token.status", "Completed");
                    }

                    await Task.Delay(delay, cancellationToken);
                    delay = Math.Min(2 * delay, 5000);
                }
            }

            // This should be unreachable, but kept as a safety net
            throw new AdbcException($"Could not successfully call {action.Method.Name}", AdbcStatusCode.UnknownError);
        }
    }
}
