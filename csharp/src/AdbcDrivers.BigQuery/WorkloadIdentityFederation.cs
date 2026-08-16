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
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.BigQuery
{
    /// <summary>
    /// Configuration for exchanging a Microsoft Entra ID service principal credential for a
    /// short-lived Google Cloud access token using Workload Identity Federation.
    /// </summary>
    internal sealed class WorkloadIdentityFederationOptions
    {
        public string TenantId { get; set; } = string.Empty;

        public string ClientId { get; set; } = string.Empty;

        public string ClientSecret { get; set; } = string.Empty;

        /// <summary>
        /// The Google workload identity pool provider resource, for example
        /// <c>//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider</c>.
        /// </summary>
        public string AudienceUri { get; set; } = string.Empty;

        /// <summary>
        /// The Application ID URI of the Entra application listed as an allowed audience on the
        /// workload identity pool provider.
        /// </summary>
        public string EntraResourceUri { get; set; } = string.Empty;

        public string AuthorityUri { get; set; } = BigQueryConstants.DefaultEntraAuthorityUri;

        /// <summary>
        /// When set, the federated identity impersonates this Google service account instead of
        /// accessing resources directly.
        /// </summary>
        public string? ServiceAccountImpersonationEmail { get; set; }

        public string Scope { get; set; } = BigQueryConstants.EntraIdScope;
    }

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
        internal const string EntraTokenStep = "entra_token";
        internal const string SecurityTokenServiceStep = "sts_exchange";
        internal const string ImpersonationStep = "sa_impersonation";

        private const string TagPrefix = "wif.";

        public static string GetGoogleAccessToken(HttpClient httpClient, WorkloadIdentityFederationOptions options, Activity? activity = null) =>
            GetGoogleAccessTokenAsync(httpClient, options, activity).GetAwaiter().GetResult();

        public static async Task<string> GetGoogleAccessTokenAsync(
            HttpClient httpClient,
            WorkloadIdentityFederationOptions options,
            Activity? activity = null,
            CancellationToken cancellationToken = default)
        {
            if (httpClient == null) throw new ArgumentNullException(nameof(httpClient));
            if (options == null) throw new ArgumentNullException(nameof(options));

            Validate(options);
            TagConfiguration(activity, options);

            try
            {
                string subjectToken = await AcquireEntraTokenAsync(httpClient, options, activity, cancellationToken).ConfigureAwait(false);

                // The issuer and audience of this token are exactly what the pool provider must be
                // configured with, and a mismatch is the most common cause of a rejected exchange.
                TagSubjectTokenClaims(activity, subjectToken);

                string federatedToken = await ExchangeForGoogleTokenAsync(httpClient, options, subjectToken, activity, cancellationToken).ConfigureAwait(false);

                if (string.IsNullOrEmpty(options.ServiceAccountImpersonationEmail))
                {
                    activity?.AddBigQueryTag(TagPrefix + "outcome", "federated_token");
                    return federatedToken;
                }

                string impersonatedToken = await ImpersonateServiceAccountAsync(httpClient, options, federatedToken, activity, cancellationToken).ConfigureAwait(false);
                activity?.AddBigQueryTag(TagPrefix + "outcome", "impersonated_token");
                return impersonatedToken;
            }
            catch (AdbcException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new AdbcException(
                    "Unable to obtain a Google Cloud access token using Workload Identity Federation.",
                    AdbcStatusCode.Unauthenticated,
                    ex);
            }
        }

        private static void TagConfiguration(Activity? activity, WorkloadIdentityFederationOptions options)
        {
            if (activity == null)
            {
                return;
            }

            activity.AddBigQueryTag(TagPrefix + "tenant_id", options.TenantId);
            activity.AddBigQueryTag(TagPrefix + "client_id", options.ClientId);
            activity.AddBigQueryTag(TagPrefix + "entra_resource_uri", options.EntraResourceUri);
            activity.AddBigQueryTag(TagPrefix + "audience_uri", options.AudienceUri);
            activity.AddBigQueryTag(TagPrefix + "scope", options.Scope);
            activity.AddBigQueryTag(TagPrefix + "impersonation_enabled", !string.IsNullOrEmpty(options.ServiceAccountImpersonationEmail));

            if (!string.IsNullOrEmpty(options.ServiceAccountImpersonationEmail))
            {
                activity.AddBigQueryTag(TagPrefix + "service_account", options.ServiceAccountImpersonationEmail);
            }

            if (Uri.TryCreate(options.AuthorityUri, UriKind.Absolute, out Uri? authority))
            {
                activity.AddBigQueryTag(TagPrefix + "authority_host", authority.Host);
            }
        }

        /// <summary>
        /// Records the claims that determine whether Google accepts the token. The token itself is a
        /// bearer credential and is never recorded.
        /// </summary>
        private static void TagSubjectTokenClaims(Activity? activity, string subjectToken)
        {
            if (activity == null)
            {
                return;
            }

            try
            {
                string[] parts = subjectToken.Split('.');
                if (parts.Length < 2)
                {
                    return;
                }

                string payload = parts[1].Replace('-', '+').Replace('_', '/');
                switch (payload.Length % 4)
                {
                    case 2: payload += "=="; break;
                    case 3: payload += "="; break;
                }

                using JsonDocument document = JsonDocument.Parse(Convert.FromBase64String(payload));

                foreach (string claim in new[] { "iss", "aud", "sub", "tid", "appid", "azp", "oid", "exp" })
                {
                    if (document.RootElement.TryGetProperty(claim, out JsonElement value))
                    {
                        activity.AddBigQueryTag(
                            TagPrefix + "subject_token." + claim,
                            value.ValueKind == JsonValueKind.String ? value.GetString() : value.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                activity.AddBigQueryTag(TagPrefix + "subject_token.decode_error", ex.GetType().Name);
            }
        }

        private static void Validate(WorkloadIdentityFederationOptions options)
        {
            RequireValue(options.TenantId, BigQueryParameters.TenantId);
            RequireValue(options.ClientId, BigQueryParameters.ClientId);
            RequireValue(options.ClientSecret, BigQueryParameters.ClientSecret);
            RequireValue(options.AudienceUri, BigQueryParameters.AudienceUri);
            RequireValue(options.EntraResourceUri, BigQueryParameters.EntraResourceUri);

            if (!Uri.TryCreate(options.AuthorityUri, UriKind.Absolute, out Uri? authority) ||
                !authority.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException(
                    $"The {BigQueryParameters.EntraAuthorityUri} parameter must be an absolute https URI.");
            }

            // The tenant and service account flow into request URIs, so reject anything that could
            // alter the target endpoint.
            if (!IsSafeUriSegment(options.TenantId))
            {
                throw new ArgumentException($"The {BigQueryParameters.TenantId} parameter contains invalid characters.");
            }

            if (!string.IsNullOrEmpty(options.ServiceAccountImpersonationEmail) &&
                !IsSafeServiceAccountEmail(options.ServiceAccountImpersonationEmail!))
            {
                throw new ArgumentException(
                    $"The {BigQueryParameters.ServiceAccountImpersonationEmail} parameter is not a valid service account email address.");
            }
        }

        private static void RequireValue(string value, string parameterName)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException($"The {parameterName} parameter is not present");
            }
        }

        private static bool IsSafeUriSegment(string value)
        {
            foreach (char c in value)
            {
                if (!char.IsLetterOrDigit(c) && c != '-' && c != '.' && c != '_')
                {
                    return false;
                }
            }

            return true;
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
        /// Requests a JWT for the service principal using the OAuth 2.0 client credentials grant.
        /// </summary>
        private static async Task<string> AcquireEntraTokenAsync(
            HttpClient httpClient,
            WorkloadIdentityFederationOptions options,
            Activity? activity,
            CancellationToken cancellationToken)
        {
            string tokenEndpoint = string.Format(
                CultureInfo.InvariantCulture,
                BigQueryConstants.EntraTokenEndpointFormat,
                options.AuthorityUri.TrimEnd('/'),
                options.TenantId);

            Dictionary<string, string> form = new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials",
                ["client_id"] = options.ClientId,
                ["client_secret"] = options.ClientSecret,
                ["scope"] = options.EntraResourceUri.TrimEnd('/') + BigQueryConstants.EntraDefaultScopeSuffix
            };

            using HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint)
            {
                Content = new FormUrlEncodedContent(form)
            };
            request.Headers.Add("Accept", "application/json");

            string body = await SendAsync(httpClient, request, EntraTokenStep, "Microsoft Entra ID", activity, cancellationToken).ConfigureAwait(false);

            BigQueryTokenResponse? response = JsonSerializer.Deserialize<BigQueryTokenResponse>(body);

            if (string.IsNullOrEmpty(response?.AccessToken))
            {
                throw new AdbcException(
                    "Microsoft Entra ID did not return an access token for the service principal.",
                    AdbcStatusCode.Unauthenticated);
            }

            activity?.AddBigQueryTag(TagPrefix + EntraTokenStep + ".expires_in", response!.ExpiresIn);

            return response!.AccessToken!;
        }

        /// <summary>
        /// Exchanges the Entra JWT for a federated Google access token at the Security Token Service.
        /// </summary>
        private static async Task<string> ExchangeForGoogleTokenAsync(
            HttpClient httpClient,
            WorkloadIdentityFederationOptions options,
            string subjectToken,
            Activity? activity,
            CancellationToken cancellationToken)
        {
            Dictionary<string, string> form = new Dictionary<string, string>
            {
                ["audience"] = options.AudienceUri,
                ["grant_type"] = BigQueryConstants.EntraGrantType,
                ["requested_token_type"] = BigQueryConstants.EntraRequestedTokenType,
                ["scope"] = options.Scope,
                ["subject_token_type"] = BigQueryConstants.AzureSubjectTokenType,
                ["subject_token"] = subjectToken
            };

            using HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Post, BigQueryConstants.EntraStsTokenEndpoint)
            {
                Content = new FormUrlEncodedContent(form)
            };
            request.Headers.Add("Accept", "application/json");

            string body = await SendAsync(httpClient, request, SecurityTokenServiceStep, "Google Security Token Service", activity, cancellationToken).ConfigureAwait(false);

            BigQueryStsTokenResponse? response = JsonSerializer.Deserialize<BigQueryStsTokenResponse>(body);

            if (string.IsNullOrEmpty(response?.AccessToken))
            {
                throw new AdbcException(
                    "The Google Security Token Service did not return an access token.",
                    AdbcStatusCode.Unauthenticated);
            }

            activity?.AddBigQueryTag(TagPrefix + SecurityTokenServiceStep + ".expires_in", response!.ExpiresIn);

            return response!.AccessToken!;
        }

        /// <summary>
        /// Uses the federated token to mint an access token for the target Google service account.
        /// </summary>
        private static async Task<string> ImpersonateServiceAccountAsync(
            HttpClient httpClient,
            WorkloadIdentityFederationOptions options,
            string federatedToken,
            Activity? activity,
            CancellationToken cancellationToken)
        {
            string impersonationUrl = string.Format(
                CultureInfo.InvariantCulture,
                BigQueryConstants.ServiceAccountImpersonationUrlFormat,
                Uri.EscapeDataString(options.ServiceAccountImpersonationEmail!));

            string json = JsonSerializer.Serialize(new ImpersonationRequest { Scope = new[] { options.Scope } });

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
                    $"Unable to impersonate the service account '{options.ServiceAccountImpersonationEmail}'.",
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
