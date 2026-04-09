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
using Apache.Arrow.Adbc;
using Google;
using Google.Apis.Requests;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.BigQuery
{
    /// <summary>
    /// Class that will retry calling a method with a backoff.
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

            int retryCount = 0;
            int delay = initialDelayMilliseconds;

            while (retryCount < maxRetries)
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
                    activity?.AddException(ex, BuildExceptionTagList(retryCount, ex));

                    retryCount++;
                    if (retryCount >= maxRetries)
                    {
                        if ((tokenProtectedResource?.UpdateToken != null))
                        {
                            if (tokenProtectedResource?.TokenRequiresUpdate(ex) == true)
                            {
                                activity?.AddBigQueryTag("update_token.status", "Expired");
                                throw new AdbcException($"Cannot update access token after {maxRetries} tries. Last exception: {ex.GetType().Name}: {ex.Message}", AdbcStatusCode.Unauthenticated, ex);
                            }
                        }

                        throw new AdbcException($"Cannot execute {action.Method.Name} after {maxRetries} tries. Last exception: {ex.GetType().Name}: {ex.Message}", AdbcStatusCode.UnknownError, ex);
                    }

                    if ((tokenProtectedResource?.UpdateToken != null))
                    {
                        if (tokenProtectedResource.TokenRequiresUpdate(ex) == true)
                        {
                            activity?.AddBigQueryTag("update_token.status", "Required");
                            await tokenProtectedResource.UpdateToken();
                            activity?.AddBigQueryTag("update_token.status", "Completed");
                        }
                    }

                    await Task.Delay(delay);
                    delay = Math.Min(2 * delay, 5000);
                }
            }

            throw new AdbcException($"Could not successfully call {action.Method.Name}", AdbcStatusCode.UnknownError);
        }

        private static TagList BuildExceptionTagList(int retryCount, Exception ex)
        {
            List<KeyValuePair<string, object?>> tags = [new KeyValuePair<string, object?>("retry.attempt", retryCount)];
            // Add HTTP status code if available
            if (ex is GoogleApiException googleEx)
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
