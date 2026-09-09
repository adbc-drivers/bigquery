/*
* Copyright (c) 2026 ADBC Drivers Contributors
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
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.BigQuery
{
    /// <summary>
    /// Implements the Workload Identity Federation flow described at
    /// https://cloud.google.com/iam/docs/workload-identity-federation-with-other-clouds.
    /// </summary>
    /// <remarks>
    /// An Entra service principal authenticates with the client credentials grant, the resulting
    /// JWT is exchanged at the Google Security Token Service for a federated access token, and the
    /// federated token optionally impersonates a Google service account. No long-lived Google
    /// service account key is involved, and every token produced here is short-lived.
    /// </remarks>
    internal static class WorkloadIdentityFederation
    {
        internal const string ImpersonationStep = "sa_impersonation";

        private const string TagPrefix = "wif.";

        /// <summary>
        /// Exchanges an already-federated token for one belonging to <paramref name="serviceAccountEmail"/>.
        /// Used by the user-token flow, which federates at the Security Token Service elsewhere.
        /// </summary>
        public static string ImpersonateServiceAccount(
            HttpClient httpClient,
            string serviceAccountEmail,
            string scope,
            string federatedToken,
            Activity? activity = null)
        {
            if (httpClient == null) throw new ArgumentNullException(nameof(httpClient));

            if (!IsSafeServiceAccountEmail(serviceAccountEmail))
            {
                throw new ArgumentException(
                    $"The {BigQueryParameters.ServiceAccountImpersonationEmail} parameter is not a valid service account email address.");
            }

            return ImpersonateServiceAccountAsync(httpClient, serviceAccountEmail, scope, federatedToken, activity, default)
                .GetAwaiter().GetResult();
        }

        private static bool IsSafeServiceAccountEmail(string value)
        {
            int atIndex = value.IndexOf('@');
            if (atIndex <= 0 || atIndex != value.LastIndexOf('@') || atIndex == value.Length - 1)
            {
                return false;
            }

            foreach (char c in value)
            {
                if (!char.IsLetterOrDigit(c) && c != '-' && c != '.' && c != '_' && c != '@')
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Uses the federated token to mint an access token for the target Google service account.
        /// </summary>
        private static async Task<string> ImpersonateServiceAccountAsync(
            HttpClient httpClient,
            string serviceAccountEmail,
            string scope,
            string federatedToken,
            Activity? activity,
            CancellationToken cancellationToken)
        {
            string impersonationUrl = string.Format(
                CultureInfo.InvariantCulture,
                BigQueryConstants.ServiceAccountImpersonationUrlFormat,
                Uri.EscapeDataString(serviceAccountEmail));

            string json = JsonSerializer.Serialize(new ImpersonationRequest { Scope = new[] { scope } });

            using HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, impersonationUrl)
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            };
            request.Headers.Add("Accept", "application/json");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", federatedToken);

            string body = await SendAsync(httpClient, request, ImpersonationStep, "Google IAM Service Account Credentials", activity, cancellationToken).ConfigureAwait(false);

            ImpersonationResponse? response = JsonSerializer.Deserialize<ImpersonationResponse>(body);

            if (string.IsNullOrEmpty(response?.AccessToken))
            {
                throw new AdbcException(
                    $"Unable to impersonate the service account '{serviceAccountEmail}'.",
                    AdbcStatusCode.Unauthenticated);
            }

            activity?.AddBigQueryTag(TagPrefix + ImpersonationStep + ".expire_time", response!.ExpireTime);

            return response!.AccessToken!;
        }

        private static async Task<string> SendAsync(
            HttpClient httpClient,
            HttpRequestMessage request,
            string step,
            string endpointDescription,
            Activity? activity,
            CancellationToken cancellationToken)
        {
            string stepTag = TagPrefix + step;
            activity?.AddBigQueryTag(stepTag + ".endpoint", request.RequestUri?.GetLeftPart(UriPartial.Path));

            Stopwatch stopwatch = Stopwatch.StartNew();
            using HttpResponseMessage response = await httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();

            string body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

            activity?.AddBigQueryTag(stepTag + ".duration_ms", stopwatch.ElapsedMilliseconds);
            activity?.AddBigQueryTag(stepTag + ".status_code", (int)response.StatusCode);

            string? requestId = GetHeader(response, "x-ms-request-id", "request-id", "x-request-id", "x-guploader-uploadid");
            if (requestId != null)
            {
                activity?.AddBigQueryTag(stepTag + ".request_id", requestId);
            }

            if (response.IsSuccessStatusCode)
            {
                return body;
            }

            TokenEndpointError error = TokenEndpointError.Parse(body);

            activity?.AddBigQueryTag(stepTag + ".error_code", error.Code);
            activity?.AddBigQueryTag(stepTag + ".error_description", error.Description);
            activity?.AddBigQueryTag(stepTag + ".correlation_id", error.CorrelationId);
            activity?.AddBigQueryTag(TagPrefix + "failed_step", step);

            // Token endpoints return actionable error codes (AADSTS*, invalid_grant, ...) and no secrets.
            throw new AdbcException(
                $"{endpointDescription} returned {(int)response.StatusCode} ({response.ReasonPhrase}) during the '{step}' step of Workload Identity Federation. " +
                $"error={error.Code ?? "(none)"}; description={error.Description ?? body}; " +
                $"requestId={requestId ?? "(none)"}; correlationId={error.CorrelationId ?? "(none)"}",
                AdbcStatusCode.Unauthenticated);
        }

        private static string? GetHeader(HttpResponseMessage response, params string[] names)
        {
            foreach (string name in names)
            {
                if (response.Headers.TryGetValues(name, out IEnumerable<string>? values))
                {
                    foreach (string value in values)
                    {
                        return value;
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Normalizes the flat OAuth error shape used by Entra and the Security Token Service and
        /// the nested error shape used by other Google APIs.
        /// </summary>
        private readonly struct TokenEndpointError
        {
            private TokenEndpointError(string? code, string? description, string? correlationId)
            {
                Code = code;
                Description = description;
                CorrelationId = correlationId;
            }

            public string? Code { get; }

            public string? Description { get; }

            public string? CorrelationId { get; }

            public static TokenEndpointError Parse(string body)
            {
                if (string.IsNullOrWhiteSpace(body))
                {
                    return new TokenEndpointError(null, null, null);
                }

                try
                {
                    using JsonDocument document = JsonDocument.Parse(body);
                    JsonElement root = document.RootElement;

                    if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("error", out JsonElement error))
                    {
                        return new TokenEndpointError(null, null, null);
                    }

                    if (error.ValueKind == JsonValueKind.Object)
                    {
                        string? status = error.TryGetProperty("status", out JsonElement s) ? s.GetString() : null;
                        string? message = error.TryGetProperty("message", out JsonElement m) ? m.GetString() : null;
                        return new TokenEndpointError(status, message, null);
                    }

                    string? code = error.ValueKind == JsonValueKind.String ? error.GetString() : null;
                    string? description = root.TryGetProperty("error_description", out JsonElement d) ? d.GetString() : null;
                    string? correlationId = root.TryGetProperty("correlation_id", out JsonElement c) ? c.GetString() : null;

                    return new TokenEndpointError(code, description, correlationId);
                }
                catch (JsonException)
                {
                    return new TokenEndpointError(null, null, null);
                }
            }
        }

        private sealed class ImpersonationRequest
        {
            [JsonPropertyName("scope")]
            public string[] Scope { get; set; } = Array.Empty<string>();
        }

        private sealed class ImpersonationResponse
        {
            [JsonPropertyName("accessToken")]
            public string? AccessToken { get; set; }

            [JsonPropertyName("expireTime")]
            public string? ExpireTime { get; set; }
        }
    }
}
