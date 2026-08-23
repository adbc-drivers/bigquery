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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Tests for the Workload Identity Federation flow that exchanges a Microsoft Entra ID
    /// service principal for a short-lived Google Cloud access token.
    /// </summary>
    public class WorkloadIdentityFederationTests
    {
        private const string EntraTokenEndpoint = "https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token";
        private const string StsEndpoint = "https://sts.googleapis.com/v1/token";
        private const string ImpersonationEndpoint =
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/bq-reader%40my-project.iam.gserviceaccount.com:generateAccessToken";

        private static WorkloadIdentityFederationOptions CreateOptions() => new WorkloadIdentityFederationOptions
        {
            TenantId = "tenant-id",
            ClientId = "client-id",
            ClientSecret = "client-secret",
            AudienceUri = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/entra",
            EntraResourceUri = "api://client-id"
        };

        [Fact]
        public async Task ExchangesServicePrincipalForFederatedToken()
        {
            RecordingHandler handler = new RecordingHandler();
            handler.Respond(EntraTokenEndpoint, "{\"access_token\":\"entra-jwt\",\"expires_in\":3599}");
            handler.Respond(StsEndpoint, "{\"access_token\":\"google-federated-token\",\"expires_in\":3600}");

            using HttpClient httpClient = new HttpClient(handler);

            string token = await WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, CreateOptions());

            Assert.Equal("google-federated-token", token);

            RecordedRequest entraRequest = handler.Requests[0];
            Assert.Equal(EntraTokenEndpoint, entraRequest.Uri);
            Assert.Equal("client_credentials", entraRequest.Form["grant_type"]);
            Assert.Equal("client-id", entraRequest.Form["client_id"]);
            Assert.Equal("client-secret", entraRequest.Form["client_secret"]);
            Assert.Equal("api://client-id/.default", entraRequest.Form["scope"]);

            RecordedRequest stsRequest = handler.Requests[1];
            Assert.Equal(StsEndpoint, stsRequest.Uri);
            Assert.Equal("entra-jwt", stsRequest.Form["subject_token"]);
            Assert.Equal("urn:ietf:params:oauth:token-type:jwt", stsRequest.Form["subject_token_type"]);
            Assert.Equal("urn:ietf:params:oauth:grant-type:token-exchange", stsRequest.Form["grant_type"]);
            Assert.Equal("urn:ietf:params:oauth:token-type:access_token", stsRequest.Form["requested_token_type"]);
            Assert.Equal(CreateOptions().AudienceUri, stsRequest.Form["audience"]);
            Assert.Equal("https://www.googleapis.com/auth/cloud-platform", stsRequest.Form["scope"]);

            Assert.Equal(2, handler.Requests.Count);
        }

        [Fact]
        public async Task ImpersonatesServiceAccountWhenConfigured()
        {
            RecordingHandler handler = new RecordingHandler();
            handler.Respond(EntraTokenEndpoint, "{\"access_token\":\"entra-jwt\",\"expires_in\":3599}");
            handler.Respond(StsEndpoint, "{\"access_token\":\"google-federated-token\",\"expires_in\":3600}");
            handler.Respond(ImpersonationEndpoint, "{\"accessToken\":\"impersonated-token\",\"expireTime\":\"2026-01-01T00:00:00Z\"}");

            using HttpClient httpClient = new HttpClient(handler);

            WorkloadIdentityFederationOptions options = CreateOptions();
            options.ServiceAccountImpersonationEmail = "bq-reader@my-project.iam.gserviceaccount.com";

            string token = await WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options);

            Assert.Equal("impersonated-token", token);

            RecordedRequest impersonationRequest = handler.Requests[2];
            Assert.Equal(ImpersonationEndpoint, impersonationRequest.Uri);
            Assert.Equal("Bearer google-federated-token", impersonationRequest.Authorization);
            Assert.Contains("https://www.googleapis.com/auth/cloud-platform", impersonationRequest.Body);
        }

        [Fact]
        public async Task UsesConfiguredScopeAndSovereignAuthority()
        {
            RecordingHandler handler = new RecordingHandler();
            handler.Respond("https://login.chinacloudapi.cn/tenant-id/oauth2/v2.0/token", "{\"access_token\":\"entra-jwt\"}");
            handler.Respond(StsEndpoint, "{\"access_token\":\"google-federated-token\"}");

            using HttpClient httpClient = new HttpClient(handler);

            WorkloadIdentityFederationOptions options = CreateOptions();
            options.AuthorityUri = "https://login.chinacloudapi.cn";
            options.Scope = "https://www.googleapis.com/auth/bigquery";

            await WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options);

            Assert.Equal("https://login.chinacloudapi.cn/tenant-id/oauth2/v2.0/token", handler.Requests[0].Uri);
            Assert.Equal("https://www.googleapis.com/auth/bigquery", handler.Requests[1].Form["scope"]);
        }

        [Theory]
        [InlineData("TenantId")]
        [InlineData("ClientId")]
        [InlineData("ClientSecret")]
        [InlineData("AudienceUri")]
        [InlineData("EntraResourceUri")]
        public async Task ThrowsWhenRequiredParameterIsMissing(string propertyName)
        {
            WorkloadIdentityFederationOptions options = CreateOptions();
            typeof(WorkloadIdentityFederationOptions).GetProperty(propertyName)!.SetValue(options, string.Empty);

            using HttpClient httpClient = new HttpClient(new RecordingHandler());

            await Assert.ThrowsAsync<ArgumentException>(
                () => WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options));
        }

        [Theory]
        [InlineData("http://login.microsoftonline.com")]
        [InlineData("not-a-uri")]
        public async Task RejectsAuthorityThatIsNotAbsoluteHttps(string authority)
        {
            WorkloadIdentityFederationOptions options = CreateOptions();
            options.AuthorityUri = authority;

            using HttpClient httpClient = new HttpClient(new RecordingHandler());

            await Assert.ThrowsAsync<ArgumentException>(
                () => WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options));
        }

        [Theory]
        [InlineData("tenant/../../evil")]
        [InlineData("tenant?x=y")]
        [InlineData("tenant id")]
        public async Task RejectsTenantIdThatCouldAlterTheTokenEndpoint(string tenantId)
        {
            WorkloadIdentityFederationOptions options = CreateOptions();
            options.TenantId = tenantId;

            using HttpClient httpClient = new HttpClient(new RecordingHandler());

            await Assert.ThrowsAsync<ArgumentException>(
                () => WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options));
        }

        [Theory]
        [InlineData("no-at-sign")]
        [InlineData("a@b@c")]
        [InlineData("evil@example.com/../../tokens")]
        public async Task RejectsInvalidServiceAccountEmail(string email)
        {
            WorkloadIdentityFederationOptions options = CreateOptions();
            options.ServiceAccountImpersonationEmail = email;

            using HttpClient httpClient = new HttpClient(new RecordingHandler());

            await Assert.ThrowsAsync<ArgumentException>(
                () => WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options));
        }

        [Fact]
        public async Task SurfacesEntraErrorDetails()
        {
            RecordingHandler handler = new RecordingHandler();
            handler.Respond(
                EntraTokenEndpoint,
                "{\"error\":\"invalid_client\",\"error_description\":\"AADSTS7000215: Invalid client secret provided.\",\"correlation_id\":\"corr-123\"}",
                HttpStatusCode.Unauthorized);

            using HttpClient httpClient = new HttpClient(handler);

            AdbcException exception = await Assert.ThrowsAsync<AdbcException>(
                () => WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, CreateOptions()));

            Assert.Equal(AdbcStatusCode.Unauthenticated, exception.Status);
            Assert.Contains("AADSTS7000215", exception.Message);
            Assert.Contains("invalid_client", exception.Message);
            Assert.Contains("corr-123", exception.Message);
            Assert.Contains(WorkloadIdentityFederation.EntraTokenStep, exception.Message);
        }

        [Fact]
        public async Task RecordsSubjectTokenClaimsAndStepDiagnostics()
        {
            RecordingHandler handler = new RecordingHandler();
            handler.Respond(EntraTokenEndpoint, "{\"access_token\":\"" + CreateJwt() + "\",\"expires_in\":3599}");
            handler.Respond(StsEndpoint, "{\"access_token\":\"google-federated-token\",\"expires_in\":3600}");

            using HttpClient httpClient = new HttpClient(handler);
            using Activity activity = new Activity("wif-test").Start();

            await WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, CreateOptions(), activity);

            activity.Stop();

            Dictionary<string, string?> tags = TagsOf(activity);

            // These are the claims the pool provider is configured against.
            Assert.Equal("https://sts.windows.net/tenant-id/", Tag(tags, "wif.subject_token.iss"));
            Assert.Equal("api://client-id", Tag(tags, "wif.subject_token.aud"));
            Assert.Equal("spn-object-id", Tag(tags, "wif.subject_token.sub"));
            Assert.Equal("client-id", Tag(tags, "wif.subject_token.appid"));

            Assert.Equal("tenant-id", Tag(tags, "wif.tenant_id"));
            Assert.Equal("login.microsoftonline.com", Tag(tags, "wif.authority_host"));
            Assert.Equal("False", Tag(tags, "wif.impersonation_enabled"));
            Assert.Equal("federated_token", Tag(tags, "wif.outcome"));
            Assert.Equal("200", Tag(tags, "wif.entra_token.status_code"));
            Assert.Equal("200", Tag(tags, "wif.sts_exchange.status_code"));
            Assert.NotNull(Tag(tags, "wif.sts_exchange.duration_ms"));
        }

        [Fact]
        public async Task RecordsWhichStepFailed()
        {
            RecordingHandler handler = new RecordingHandler();
            handler.Respond(EntraTokenEndpoint, "{\"access_token\":\"" + CreateJwt() + "\"}");
            handler.Respond(
                StsEndpoint,
                "{\"error\":\"invalid_request\",\"error_description\":\"The audience is invalid.\"}",
                HttpStatusCode.BadRequest);

            using HttpClient httpClient = new HttpClient(handler);
            using Activity activity = new Activity("wif-test").Start();

            await Assert.ThrowsAsync<AdbcException>(
                () => WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, CreateOptions(), activity));

            activity.Stop();

            Dictionary<string, string?> tags = TagsOf(activity);

            Assert.Equal(WorkloadIdentityFederation.SecurityTokenServiceStep, Tag(tags, "wif.failed_step"));
            Assert.Equal("400", Tag(tags, "wif.sts_exchange.status_code"));
            Assert.Equal("invalid_request", Tag(tags, "wif.sts_exchange.error_code"));
            Assert.Equal("The audience is invalid.", Tag(tags, "wif.sts_exchange.error_description"));

            // The preceding step still succeeded, which is what narrows the investigation.
            Assert.Equal("200", Tag(tags, "wif.entra_token.status_code"));
        }

        [Fact]
        public async Task ParsesNestedGoogleApiErrorShape()
        {
            RecordingHandler handler = new RecordingHandler();
            handler.Respond(EntraTokenEndpoint, "{\"access_token\":\"" + CreateJwt() + "\"}");
            handler.Respond(StsEndpoint, "{\"access_token\":\"google-federated-token\"}");
            handler.Respond(
                ImpersonationEndpoint,
                "{\"error\":{\"code\":403,\"message\":\"Permission iam.serviceAccounts.getAccessToken denied.\",\"status\":\"PERMISSION_DENIED\"}}",
                HttpStatusCode.Forbidden);

            using HttpClient httpClient = new HttpClient(handler);
            using Activity activity = new Activity("wif-test").Start();

            WorkloadIdentityFederationOptions options = CreateOptions();
            options.ServiceAccountImpersonationEmail = "bq-reader@my-project.iam.gserviceaccount.com";

            AdbcException exception = await Assert.ThrowsAsync<AdbcException>(
                () => WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options, activity));

            activity.Stop();

            Dictionary<string, string?> tags = TagsOf(activity);

            Assert.Equal(WorkloadIdentityFederation.ImpersonationStep, Tag(tags, "wif.failed_step"));
            Assert.Equal("PERMISSION_DENIED", Tag(tags, "wif.sa_impersonation.error_code"));
            Assert.Contains("PERMISSION_DENIED", exception.Message);
        }

        [Fact]
        public async Task NeverRecordsSecretsOrBearerTokens()
        {
            RecordingHandler handler = new RecordingHandler();
            handler.Respond(EntraTokenEndpoint, "{\"access_token\":\"" + CreateJwt() + "\"}");
            handler.Respond(StsEndpoint, "{\"access_token\":\"google-federated-token\"}");

            using HttpClient httpClient = new HttpClient(handler);
            using Activity activity = new Activity("wif-test").Start();

            await WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, CreateOptions(), activity);

            activity.Stop();

            string allTags = string.Join("|", activity.TagObjects.Select(t => t.Key + "=" + t.Value));

            Assert.DoesNotContain("client-secret", allTags);
            Assert.DoesNotContain("google-federated-token", allTags);
            Assert.DoesNotContain(CreateJwt(), allTags);
        }

        [Fact]
        public async Task AuthenticatesWithSignedAssertionWhenCertificateSupplied()
        {
            using X509Certificate2 certificate = CreateTestCertificate();

            RecordingHandler handler = new RecordingHandler();
            handler.Respond(EntraTokenEndpoint, "{\"access_token\":\"" + CreateJwt() + "\"}");
            handler.Respond(StsEndpoint, "{\"access_token\":\"google-federated-token\"}");

            using HttpClient httpClient = new HttpClient(handler);

            WorkloadIdentityFederationOptions options = CreateOptions();
            options.ClientSecret = string.Empty;
            options.ClientCertificate = Convert.ToBase64String(certificate.Export(X509ContentType.Pfx));

            string token = await WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options);

            Assert.Equal("google-federated-token", token);

            Dictionary<string, string> form = handler.Requests[0].Form;

            Assert.False(form.ContainsKey("client_secret"));
            Assert.Equal("urn:ietf:params:oauth:client-assertion-type:jwt-bearer", form["client_assertion_type"]);
            Assert.Equal("client_credentials", form["grant_type"]);

            string[] parts = form["client_assertion"].Split('.');
            Assert.Equal(3, parts.Length);

            using JsonDocument header = JsonDocument.Parse(FromBase64Url(parts[0]));
            Assert.Equal("RS256", header.RootElement.GetProperty("alg").GetString());
            Assert.Equal("JWT", header.RootElement.GetProperty("typ").GetString());

            // Entra locates the signing key by base64url SHA-1 thumbprint.
            string expectedThumbprint = Convert.ToBase64String(certificate.GetCertHash())
                .TrimEnd('=').Replace('+', '-').Replace('/', '_');
            Assert.Equal(expectedThumbprint, header.RootElement.GetProperty("x5t").GetString());

            using JsonDocument payload = JsonDocument.Parse(FromBase64Url(parts[1]));
            Assert.Equal(EntraTokenEndpoint, payload.RootElement.GetProperty("aud").GetString());
            Assert.Equal("client-id", payload.RootElement.GetProperty("iss").GetString());
            Assert.Equal("client-id", payload.RootElement.GetProperty("sub").GetString());
            Assert.False(string.IsNullOrEmpty(payload.RootElement.GetProperty("jti").GetString()));

            long nbf = payload.RootElement.GetProperty("nbf").GetInt64();
            long exp = payload.RootElement.GetProperty("exp").GetInt64();
            Assert.True(exp > nbf, "exp must be later than nbf");
            Assert.True(exp - nbf <= 600, "Entra rejects assertions living longer than 10 minutes");
        }

        [Fact]
        public void ClientAssertionSignatureVerifiesAgainstThePublicKey()
        {
            using X509Certificate2 certificate = CreateTestCertificate();

            WorkloadIdentityFederationOptions options = CreateOptions();
            options.ClientCertificate = Convert.ToBase64String(certificate.Export(X509ContentType.Pfx));

            string assertion = WorkloadIdentityFederation.CreateClientAssertion(options, EntraTokenEndpoint);

            int lastDot = assertion.LastIndexOf('.');
            byte[] signingInput = System.Text.Encoding.UTF8.GetBytes(assertion.Substring(0, lastDot));
            byte[] signature = FromBase64Url(assertion.Substring(lastDot + 1));

            using RSA publicKey = certificate.GetRSAPublicKey()!;

            Assert.True(
                publicKey.VerifyData(signingInput, signature, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1),
                "The assertion signature did not verify against the certificate public key.");
        }

        [Fact]
        public void CertificateRemovesTheClientSecretRequirement()
        {
            using X509Certificate2 certificate = CreateTestCertificate();

            WorkloadIdentityFederationOptions options = CreateOptions();
            options.ClientSecret = string.Empty;
            options.ClientCertificate = Convert.ToBase64String(certificate.Export(X509ContentType.Pfx));

            Assert.True(options.UsesCertificate);

            // Should not throw for the missing secret.
            string assertion = WorkloadIdentityFederation.CreateClientAssertion(options, EntraTokenEndpoint);
            Assert.False(string.IsNullOrEmpty(assertion));
        }

        [Fact]
        public async Task RejectsCertificateThatIsNotValidBase64()
        {
            WorkloadIdentityFederationOptions options = CreateOptions();
            options.ClientSecret = string.Empty;
            options.ClientCertificate = "not-base64!!";

            using HttpClient httpClient = new HttpClient(new RecordingHandler());

            await Assert.ThrowsAsync<ArgumentException>(
                () => WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options));
        }

        [Fact]
        public async Task NeverSendsOrRecordsTheCertificatePayload()
        {
            using X509Certificate2 certificate = CreateTestCertificate();
            string pfx = Convert.ToBase64String(certificate.Export(X509ContentType.Pfx));

            RecordingHandler handler = new RecordingHandler();
            handler.Respond(EntraTokenEndpoint, "{\"access_token\":\"" + CreateJwt() + "\"}");
            handler.Respond(StsEndpoint, "{\"access_token\":\"google-federated-token\"}");

            using HttpClient httpClient = new HttpClient(handler);
            using Activity activity = new Activity("wif-cert").Start();

            WorkloadIdentityFederationOptions options = CreateOptions();
            options.ClientSecret = string.Empty;
            options.ClientCertificate = pfx;

            await WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options, activity);

            activity.Stop();

            string allTags = string.Join("|", activity.TagObjects.Select(t => t.Key + "=" + t.Value));
            Assert.DoesNotContain(pfx, allTags);
            Assert.Equal("certificate", Tag(TagsOf(activity), "wif.client_auth"));

            // The private key must never leave the process.
            foreach (RecordedRequest request in handler.Requests)
            {
                Assert.DoesNotContain(pfx, request.Body);
            }
        }

        private static X509Certificate2 CreateTestCertificate()
        {
            using RSA rsa = RSA.Create(2048);
            CertificateRequest request = new CertificateRequest(
                "CN=wif-test",
                rsa,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);

            return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddYears(1));
        }

        private static byte[] FromBase64Url(string value)
        {
            string padded = value.Replace('-', '+').Replace('_', '/');
            switch (padded.Length % 4)
            {
                case 2: padded += "=="; break;
                case 3: padded += "="; break;
            }

            return Convert.FromBase64String(padded);
        }

        private static string? Tag(Dictionary<string, string?> tags, string key)
        {
            // ActivityExtensions namespaces every driver tag.
            return tags.TryGetValue("adbc.bigquery.tracing." + key, out string? value) ? value : null;
        }

        /// <summary>
        /// Activity.Tags only exposes string-valued tags and AddTag appends rather than replaces,
        /// so read TagObjects and collapse to the last value per key.
        /// </summary>
        private static Dictionary<string, string?> TagsOf(Activity activity)
        {
            Dictionary<string, string?> tags = new Dictionary<string, string?>();

            foreach (KeyValuePair<string, object?> tag in activity.TagObjects)
            {
                tags[tag.Key] = tag.Value?.ToString();
            }

            return tags;
        }

        /// <summary>
        /// Builds an unsigned JWT whose payload carries the claims Google validates.
        /// </summary>
        private static string CreateJwt()
        {
            string payload = "{\"iss\":\"https://sts.windows.net/tenant-id/\",\"aud\":\"api://client-id\"," +
                             "\"sub\":\"spn-object-id\",\"tid\":\"tenant-id\",\"appid\":\"client-id\",\"exp\":1893456000}";

            return "header." + Base64Url(payload) + ".signature";
        }

        private static string Base64Url(string value) =>
            Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(value))
                .TrimEnd('=')
                .Replace('+', '-')
                .Replace('/', '_');

        [Fact]
        public void ConnectionRenewsItsOwnTokenForServicePrincipalAuthentication()
        {
            Dictionary<string, string> properties = new Dictionary<string, string>
            {
                [BigQueryParameters.AuthenticationType] = BigQueryConstants.EntraServicePrincipalAuthenticationType,
                [BigQueryParameters.TenantId] = "tenant-id",
                [BigQueryParameters.ClientId] = "client-id",
                [BigQueryParameters.ClientSecret] = "client-secret",
                [BigQueryParameters.AudienceUri] = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/entra"
            };

            using BigQueryConnection connection = new BigQueryConnection(properties);

            Assert.NotNull(connection.UpdateToken);
        }

        [Fact]
        public void ConnectionDoesNotSelfRenewForCallerSuppliedTokens()
        {
            Dictionary<string, string> properties = new Dictionary<string, string>
            {
                [BigQueryParameters.AuthenticationType] = BigQueryConstants.EntraIdAuthenticationType,
                [BigQueryParameters.AccessToken] = "caller-supplied-token",
                [BigQueryParameters.AudienceUri] = "//iam.googleapis.com/locations/global/workforcePools/pool/providers/entra"
            };

            using BigQueryConnection connection = new BigQueryConnection(properties);

            Assert.Null(connection.UpdateToken);
        }

        private sealed class RecordedRequest
        {
            public string Uri { get; set; } = string.Empty;

            public string Body { get; set; } = string.Empty;

            public string? Authorization { get; set; }

            public Dictionary<string, string> Form { get; set; } = new Dictionary<string, string>();
        }

        private sealed class RecordingHandler : HttpMessageHandler
        {
            private readonly Dictionary<string, (string Body, HttpStatusCode Status)> _responses =
                new Dictionary<string, (string, HttpStatusCode)>(StringComparer.OrdinalIgnoreCase);

            public List<RecordedRequest> Requests { get; } = new List<RecordedRequest>();

            public void Respond(string uri, string body, HttpStatusCode status = HttpStatusCode.OK) =>
                _responses[uri] = (body, status);

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                string uri = request.RequestUri!.ToString();
                string body = request.Content == null
                    ? string.Empty
                    : await request.Content.ReadAsStringAsync().ConfigureAwait(false);

                Requests.Add(new RecordedRequest
                {
                    Uri = uri,
                    Body = body,
                    Authorization = request.Headers.Authorization?.ToString(),
                    Form = ParseForm(body)
                });

                if (!_responses.TryGetValue(uri, out (string Body, HttpStatusCode Status) response))
                {
                    return new HttpResponseMessage(HttpStatusCode.NotFound)
                    {
                        Content = new StringContent($"No stubbed response for {uri}")
                    };
                }

                return new HttpResponseMessage(response.Status)
                {
                    Content = new StringContent(response.Body)
                };
            }

            private static Dictionary<string, string> ParseForm(string body)
            {
                Dictionary<string, string> form = new Dictionary<string, string>();

                if (string.IsNullOrEmpty(body) || body.StartsWith("{", StringComparison.Ordinal))
                {
                    return form;
                }

                foreach (string pair in body.Split('&').Where(x => x.Length > 0))
                {
                    string[] parts = pair.Split(new[] { '=' }, 2);
                    form[Uri.UnescapeDataString(parts[0])] = parts.Length > 1
                        ? Uri.UnescapeDataString(parts[1].Replace("+", " "))
                        : string.Empty;
                }

                return form;
            }
        }
    }
}
