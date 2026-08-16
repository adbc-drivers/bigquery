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

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Xunit;
using Xunit.Abstractions;
using AdbcTests = Apache.Arrow.Adbc.Tests;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Live tests for Workload Identity Federation. Unlike <see cref="WorkloadIdentityFederationTests"/>,
    /// which stubs every HTTP endpoint, these call Microsoft Entra ID, the Google Security Token
    /// Service and BigQuery for real.
    /// </summary>
    /// <remarks>
    /// Requires an environment with <c>authenticationType</c> of <c>aad_service_principal</c> in the
    /// file referenced by the BIGQUERY_TEST_CONFIG_FILE variable. Skipped otherwise.
    /// </remarks>
    public class WorkloadIdentityFederationLiveTests
    {
        private readonly BigQueryTestConfiguration? _testConfiguration;
        private readonly List<BigQueryTestEnvironment> _environments;
        private readonly ITestOutputHelper _outputHelper;

        public WorkloadIdentityFederationLiveTests(ITestOutputHelper outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            _testConfiguration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(_testConfiguration);
            _outputHelper = outputHelper;
        }

        /// <summary>
        /// Exercises the token exchange on its own so a failure identifies which hop rejected the
        /// request rather than surfacing as a generic BigQuery authentication error.
        /// </summary>
        [SkippableFact]
        public async Task CanExchangeServicePrincipalForGoogleToken()
        {
            BigQueryTestEnvironment environment = GetEnvironment();

            WorkloadIdentityFederationOptions options = new WorkloadIdentityFederationOptions
            {
                TenantId = environment.TenantId,
                ClientId = environment.ClientId,
                ClientSecret = environment.ClientSecret,
                AudienceUri = environment.Audience,
                EntraResourceUri = string.IsNullOrEmpty(environment.EntraResourceUri)
                    ? "api://" + environment.ClientId
                    : environment.EntraResourceUri,
                ServiceAccountImpersonationEmail = string.IsNullOrEmpty(environment.ServiceAccountImpersonationEmail)
                    ? null
                    : environment.ServiceAccountImpersonationEmail
            };

            using HttpClient httpClient = new HttpClient();
            using Activity activity = new Activity(nameof(CanExchangeServicePrincipalForGoogleToken)).Start();

            try
            {
                string token = await WorkloadIdentityFederation.GetGoogleAccessTokenAsync(httpClient, options, activity);

                Assert.False(string.IsNullOrEmpty(token), "The token exchange returned an empty access token.");
            }
            finally
            {
                activity.Stop();
                WriteDiagnostics(activity);
            }
        }

        /// <summary>
        /// Runs a real query end to end using only the service principal credential, which is the
        /// unattended path used by scheduled refresh.
        /// </summary>
        [SkippableFact]
        public void CanQueryBigQueryWithServicePrincipal()
        {
            BigQueryTestEnvironment environment = GetEnvironment();

            using BigQueryConnection? connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment) as BigQueryConnection;
            Assert.NotNull(connection);

            // No caller supplies a token on this path, so the driver must renew its own.
            Assert.NotNull(connection!.UpdateToken);

            AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = environment.Query;

            QueryResult queryResult = statement.ExecuteQuery();

            AdbcTests.DriverTests.CanExecuteQuery(queryResult, environment.ExpectedResultsCount, environment.Name);
        }

        private BigQueryTestEnvironment GetEnvironment()
        {
            BigQueryTestEnvironment? environment = _environments
                .FirstOrDefault(x => x.AuthenticationType.Equals(BigQueryConstants.EntraServicePrincipalAuthenticationType, System.StringComparison.OrdinalIgnoreCase));

            Skip.If(
                environment == null,
                $"No test environment with an authenticationType of '{BigQueryConstants.EntraServicePrincipalAuthenticationType}' was found.");

            return environment!;
        }

        /// <summary>
        /// Emits the federation tags so a failed run reports the issuer, audience and failing step.
        /// </summary>
        private void WriteDiagnostics(Activity activity)
        {
            foreach (KeyValuePair<string, object?> tag in activity.TagObjects.Where(t => t.Key.Contains("wif.")))
            {
                _outputHelper.WriteLine($"{tag.Key} = {tag.Value}");
            }
        }
    }
}
