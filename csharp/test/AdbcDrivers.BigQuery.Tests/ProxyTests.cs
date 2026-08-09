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
using System.Linq;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Live tests that route BigQuery traffic through a real HTTP proxy.
    /// Runs only for environments in the test configuration that set "proxyHost".
    /// </summary>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class ProxyTests
    {
        private readonly List<BigQueryTestEnvironment> _environments;
        private readonly ITestOutputHelper _outputHelper;

        public ProxyTests(ITestOutputHelper outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            BigQueryTestConfiguration testConfiguration =
                MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(testConfiguration);
            _outputHelper = outputHelper;
        }

        /// <summary>
        /// Executes each proxy-configured environment's query through its proxy and validates the results.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanExecuteQueryThroughProxy()
        {
            List<BigQueryTestEnvironment> environments = _environments
                .Where(environment => !string.IsNullOrEmpty(environment.ProxyHost))
                .ToList();

            Skip.If(environments.Count == 0, "No environment in the test configuration sets 'proxyHost'.");

            foreach (BigQueryTestEnvironment environment in environments)
            {
                _outputHelper.WriteLine($"Running query for environment '{environment.Name}' through proxy '{environment.ProxyHost}:{environment.ProxyPort}'.");

                using AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                using AdbcStatement statement = connection.CreateStatement();
                statement.SqlQuery = environment.Query;

                QueryResult queryResult = statement.ExecuteQuery();

                Apache.Arrow.Adbc.Tests.DriverTests.CanExecuteQuery(queryResult, environment.ExpectedResultsCount, environment.Name);
            }
        }
    }
}
