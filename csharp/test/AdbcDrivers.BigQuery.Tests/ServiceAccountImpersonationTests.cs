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
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Covers the request the driver sends to the Google IAM Service Account Credentials
    /// generateAccessToken endpoint, using a stub handler so no network call is made.
    /// </summary>
    public class ServiceAccountImpersonationTests
    {
        private const string ServiceAccount = "bq-wif@example-project.iam.gserviceaccount.com";
        private const string FederatedToken = "federated-token-value";

        [Fact]
        public void ImpersonationPostsToGenerateAccessTokenWithBearerCredential()
        {
            RecordingHandler handler = RecordingHandler.RespondWith("{\"accessToken\":\"impersonated\",\"expireTime\":\"2026-01-01T00:00:00Z\"}");
            using HttpClient httpClient = new HttpClient(handler);

            string token = WorkloadIdentityFederation.ImpersonateServiceAccount(
                httpClient, ServiceAccount, new[] { BigQueryConstants.EntraIdScope }, FederatedToken);

            Assert.Equal("impersonated", token);
            Assert.Equal(HttpMethod.Post, handler.Request!.Method);
            Assert.Equal(
                "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/" + Uri.EscapeDataString(ServiceAccount) + ":generateAccessToken",
                handler.Request.RequestUri!.ToString());
            Assert.Equal("Bearer", handler.Request.Headers.Authorization!.Scheme);
            Assert.Equal(FederatedToken, handler.Request.Headers.Authorization.Parameter);
        }

        /// <summary>
        /// generateAccessToken expects one array element per scope. Joining multiple scopes into a
        /// single element sends one malformed scope and the request is rejected.
        /// </summary>
        [Fact]
        public void ImpersonationSendsEachScopeAsASeparateArrayElement()
        {
            RecordingHandler handler = RecordingHandler.RespondWith("{\"accessToken\":\"impersonated\"}");
            using HttpClient httpClient = new HttpClient(handler);

            string[] scopes =
            {
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive"
            };

            WorkloadIdentityFederation.ImpersonateServiceAccount(httpClient, ServiceAccount, scopes, FederatedToken);

            using JsonDocument document = JsonDocument.Parse(handler.RequestBody!);
            JsonElement scope = document.RootElement.GetProperty("scope");

            Assert.Equal(JsonValueKind.Array, scope.ValueKind);
            Assert.Equal(scopes.Length, scope.GetArrayLength());
            Assert.Equal(scopes[0], scope[0].GetString());
            Assert.Equal(scopes[1], scope[1].GetString());
        }

        [Fact]
        public void ImpersonationFailureSurfacesTheResponseBody()
        {
            RecordingHandler handler = RecordingHandler.RespondWith(
                "{\"error\":{\"status\":\"PERMISSION_DENIED\",\"message\":\"caller does not have permission\"}}",
                HttpStatusCode.Forbidden);
            using HttpClient httpClient = new HttpClient(handler);

            Exception exception = Assert.ThrowsAny<Exception>(() =>
                WorkloadIdentityFederation.ImpersonateServiceAccount(
                    httpClient, ServiceAccount, new[] { BigQueryConstants.EntraIdScope }, FederatedToken));

            Assert.Contains("PERMISSION_DENIED", exception.Message);
        }

        [Fact]
        public void ImpersonationRejectsAnEmptyResponseToken()
        {
            RecordingHandler handler = RecordingHandler.RespondWith("{\"expireTime\":\"2026-01-01T00:00:00Z\"}");
            using HttpClient httpClient = new HttpClient(handler);

            AdbcException exception = Assert.Throws<AdbcException>(() =>
                WorkloadIdentityFederation.ImpersonateServiceAccount(
                    httpClient, ServiceAccount, new[] { BigQueryConstants.EntraIdScope }, FederatedToken));

            Assert.Equal(AdbcStatusCode.Unauthenticated, exception.Status);
        }

        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("no-at-sign")]
        [InlineData("two@at@signs.com")]
        [InlineData("trailing@")]
        [InlineData("@leading.com")]
        [InlineData("space in@name.com")]
        [InlineData("semicolon;@name.com")]
        [InlineData("slash@name.com/../evil")]
        [InlineData(" leading-space@name.com")]
        [InlineData("trailing-space@name.com ")]
        // $ matches before a trailing newline, so the pattern is anchored with \z instead.
        [InlineData("newline@name.com\n")]
        [InlineData("newline@name.com\r\n")]
        [InlineData("injection@name.com\nX-Evil: 1")]
        // The previous character-by-character check accepted all of the following.
        [InlineData("no-dot@localhost")]
        [InlineData("empty-label@.com")]
        [InlineData("double-dot@name..com")]
        [InlineData("trailing-dot@name.com.")]
        [InlineData("cyrilliс@name.com")]
        [InlineData("ｆｕｌｌｗｉｄｔｈ@name.com")]
        public void ImpersonationRejectsMalformedServiceAccountEmails(string email)
        {
            RecordingHandler handler = RecordingHandler.RespondWith("{\"accessToken\":\"unused\"}");
            using HttpClient httpClient = new HttpClient(handler);

            Assert.Throws<ArgumentException>(() =>
                WorkloadIdentityFederation.ImpersonateServiceAccount(
                    httpClient, email, new[] { BigQueryConstants.EntraIdScope }, FederatedToken));

            Assert.Null(handler.Request);
        }

        /// <summary>
        /// The default and service-agent accounts do not follow the user-managed naming rules, so
        /// the pattern must stay permissive enough for all of them.
        /// </summary>
        [Theory]
        [InlineData("bq-wif@example-project.iam.gserviceaccount.com")]
        [InlineData("123456789-compute@developer.gserviceaccount.com")]
        [InlineData("service-123456@gcp-sa-aiplatform.iam.gserviceaccount.com")]
        [InlineData("my-project@appspot.gserviceaccount.com")]
        [InlineData("123456789@cloudservices.gserviceaccount.com")]
        public void ImpersonationAcceptsRealGoogleServiceAccountFormats(string email)
        {
            RecordingHandler handler = RecordingHandler.RespondWith("{\"accessToken\":\"impersonated\"}");
            using HttpClient httpClient = new HttpClient(handler);

            string token = WorkloadIdentityFederation.ImpersonateServiceAccount(
                httpClient, email, new[] { BigQueryConstants.EntraIdScope }, FederatedToken);

            Assert.Equal("impersonated", token);
        }

        [Fact]
        public void ImpersonationRequiresAtLeastOneScope()
        {
            RecordingHandler handler = RecordingHandler.RespondWith("{\"accessToken\":\"unused\"}");
            using HttpClient httpClient = new HttpClient(handler);

            Assert.Throws<ArgumentException>(() =>
                WorkloadIdentityFederation.ImpersonateServiceAccount(
                    httpClient, ServiceAccount, Array.Empty<string>(), FederatedToken));

            Assert.Null(handler.Request);
        }

        /// <summary>
        /// Only an absent value opts out of impersonation. A whitespace value is a misconfiguration
        /// and must fail rather than silently running queries as the federated caller.
        /// </summary>
        [Theory]
        [InlineData(" ")]
        [InlineData("   ")]
        [InlineData("\t")]
        public void WhitespaceImpersonationEmailIsRejectedRatherThanIgnored(string configured)
        {
            Dictionary<string, string> properties = new Dictionary<string, string>
            {
                [BigQueryParameters.AuthenticationType] = BigQueryConstants.EntraIdAuthenticationType,
                [BigQueryParameters.ServiceAccountImpersonationEmail] = configured,
            };

            using BigQueryConnection connection = new BigQueryConnection(properties);

            Assert.Throws<ArgumentException>(() => connection.ImpersonateIfRequested("federated-token", null));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void AbsentImpersonationEmailReturnsTheFederatedToken(string? configured)
        {
            Dictionary<string, string> properties = new Dictionary<string, string>
            {
                [BigQueryParameters.AuthenticationType] = BigQueryConstants.EntraIdAuthenticationType,
            };

            if (configured != null)
            {
                properties[BigQueryParameters.ServiceAccountImpersonationEmail] = configured;
            }

            using BigQueryConnection connection = new BigQueryConnection(properties);

            Assert.Equal("federated-token", connection.ImpersonateIfRequested("federated-token", null));
        }

        private sealed class RecordingHandler : HttpMessageHandler        {
            private string responseBody = string.Empty;
            private HttpStatusCode statusCode = HttpStatusCode.OK;

            public HttpRequestMessage? Request { get; private set; }

            public string? RequestBody { get; private set; }

            public static RecordingHandler RespondWith(string body, HttpStatusCode statusCode = HttpStatusCode.OK)
            {
                return new RecordingHandler { responseBody = body, statusCode = statusCode };
            }

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                this.Request = request;

                if (request.Content != null)
                {
                    this.RequestBody = await request.Content.ReadAsStringAsync().ConfigureAwait(false);
                }

                return new HttpResponseMessage(this.statusCode)
                {
                    Content = new StringContent(this.responseBody, Encoding.UTF8, "application/json")
                };
            }
        }
    }
}
