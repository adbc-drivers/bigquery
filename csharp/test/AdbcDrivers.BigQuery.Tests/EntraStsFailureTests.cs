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
        /// Regression guard for the real failure seen when a provider maps google.subject to
        /// assertion.oid but the connector federates an Entra id_token, which has no oid claim.
        /// The remediation is only discoverable from the response body.
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
    }
}
