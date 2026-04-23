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
