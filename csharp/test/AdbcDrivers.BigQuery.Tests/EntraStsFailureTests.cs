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

using System.Net;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Tests for the diagnostics surfaced when the Google Security Token Service rejects a
    /// Microsoft Entra ID token during the "aad" federation flow.
    /// </summary>
    public class EntraStsFailureTests
    {
        [Fact]
        public void StsFailureMessageIncludesStatusCodeAndResponseBody()
        {
            const string body = "{\"error\":\"invalid_request\"}";

            string message = BigQueryConnection.BuildStsFailureMessage(HttpStatusCode.BadRequest, body);

            Assert.Contains("400", message);
            Assert.Contains("BadRequest", message);
            Assert.Contains(body, message);
        }

        /// <summary>
        /// Regression guard for the real failure seen when a provider mapped google.subject to
        /// assertion.oid but the connector federated an Entra id_token, which had no oid claim.
        /// The remediation was only discoverable from the response body.
        /// </summary>
        [Fact]
        public void StsFailureMessageSurfacesUnmappedSubjectReason()
        {
            const string body =
                "{\"error\":\"unauthorized_client\",\"error_description\":\"Could not obtain a value for google.subject from the given credential.\"}";

            string message = BigQueryConnection.BuildStsFailureMessage(HttpStatusCode.BadRequest, body);

            Assert.Contains("google.subject", message);
            Assert.Contains("unauthorized_client", message);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void StsFailureMessageHandlesMissingResponseBody(string? body)
        {
            string message = BigQueryConnection.BuildStsFailureMessage(HttpStatusCode.Forbidden, body);

            Assert.Contains("403", message);
            Assert.Contains("(no response body)", message);
        }

        [Fact]
        public void TokenFailureMessageIncludesStatusCodeAndResponseBody()
        {
            const string body = "{\"error\":\"invalid_grant\"}";

            string message = BigQueryConnection.BuildTokenFailureMessage(HttpStatusCode.Unauthorized, body);

            Assert.Contains("401", message);
            Assert.Contains("Unauthorized", message);
            Assert.Contains(body, message);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void TokenFailureMessageHandlesMissingResponseBody(string? body)
        {
            string message = BigQueryConnection.BuildTokenFailureMessage(HttpStatusCode.BadGateway, body);

            Assert.Contains("502", message);
            Assert.Contains("no response body", message);
        }

        /// <summary>
        /// The connector federates an Entra *access* token, so the subject token type must be the
        /// generic JWT type. Declaring it as an id_token misrepresents the credential to the
        /// Security Token Service.
        /// </summary>
        [Fact]
        public void StsRequestBodyDeclaresAccessTokenAsJwt()
        {
            string body = BigQueryConnection.CreateEntraStsRequestBody(
                "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/v",
                "an.entra.accesstoken",
                null);

            Assert.Contains(BigQueryConstants.AzureSubjectTokenType, body);
            Assert.DoesNotContain(BigQueryConstants.EntraSubjectTokenType, body);
        }

        private const string WorkforceAudience =
            "//iam.googleapis.com/locations/global/workforcePools/pool/providers/azuread";

        private const string WorkloadAudience =
            "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/pool/providers/entra";

        /// <summary>
        /// options.userProject is a workforce pool concept. A workload identity pool audience already
        /// names its project, and sending userProject there imposes a serviceusage.serviceUsageConsumer
        /// requirement the exchange would not otherwise have.
        /// </summary>
        [Fact]
        public void StsRequestBodyOmitsUserProjectForWorkloadPoolAudiences()
        {
            string body = CreateEntraStsRequestBody(WorkloadAudience, "token", "my-billing-project");

            Assert.DoesNotContain("options", body);
            Assert.DoesNotContain("userProject", body);
            Assert.DoesNotContain("my-billing-project", body);
        }

        [Fact]
        public void StsRequestBodyIncludesUserProjectForWorkforcePoolAudiences()
        {
            string body = CreateEntraStsRequestBody(WorkforceAudience, "token", "my-billing-project");

            Assert.Contains("userProject", body);
            Assert.Contains("my-billing-project", body);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void StsRequestBodyOmitsUserProjectWhenNoneSupplied(string? userProject)
        {
            string body = CreateEntraStsRequestBody(WorkforceAudience, "token", userProject);

            Assert.DoesNotContain("userProject", body);
        }

        [Theory]
        [InlineData(WorkforceAudience, true)]
        [InlineData(WorkloadAudience, false)]
        [InlineData("//iam.googleapis.com/locations/global/WORKFORCEPOOLS/p/providers/x", true)]
        [InlineData("", false)]
        [InlineData(null, false)]
        public void WorkforceAudienceDetectionMatchesThePoolKind(string? audience, bool expected)
        {
            Assert.Equal(expected, BigQueryConnection.IsWorkforcePoolAudience(audience));
        }

        private static string CreateEntraStsRequestBody(string audience, string token, string? userProject) =>
            BigQueryConnection.CreateEntraStsRequestBody(audience, token, userProject);
    }
}
