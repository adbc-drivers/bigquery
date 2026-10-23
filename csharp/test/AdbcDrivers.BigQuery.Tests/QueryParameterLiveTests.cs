/*
 * Copyright (c) 2026 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    [Trait("Category", "Live")]
    public class QueryParameterLiveTests
    {
        private readonly List<BigQueryTestEnvironment> _environments;

        public QueryParameterLiveTests()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE),
                "Set BIGQUERY_TEST_CONFIG_FILE to run live query parameter tests.");
            var configuration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(
                BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(configuration);
            Assert.NotEmpty(_environments);
        }

        [SkippableTheory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ScalarBindingsRoundTripAndCanBeRebound(bool useQueryEndpoint)
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                Dictionary<string, string> parameters = BigQueryTestingUtils.GetBigQueryParameters(environment);
                parameters[BigQueryParameters.AllowLargeResults] = "false";
                parameters[BigQueryParameters.CreateLargeResultsDataset] = "false";
                parameters[BigQueryParameters.IsMetadataCommand] = "false";
                parameters.Remove(BigQueryParameters.LargeResultsDestinationTable);
                using var driver = new BigQueryDriver();
                using AdbcDatabase database = driver.Open(parameters);
                using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
                using AdbcStatement statement = connection.CreateStatement();
                statement.SetOption(BigQueryParameters.UseLegacySQL, "false");
                statement.SetOption(BigQueryParameters.UseJobCreationMode, useQueryEndpoint ? "true" : "false");
                statement.SqlQuery = @"SELECT
                    ? AS integer_value,
                    ? AS boolean_value,
                    ? AS text_value,
                    TO_HEX(?) AS binary_value,
                    CAST(? AS STRING) AS decimal_value,
                    CAST(? AS STRING) AS wide_decimal_value,
                    ? AS null_value,
                    CAST(? AS STRING) AS date_value,
                    CAST(? AS STRING) AS time_value,
                    FORMAT_DATETIME('%F %H:%M:%E6S', ?) AS datetime_value,
                    FORMAT_TIMESTAMP('%F %H:%M:%E6S %Ez', ?) AS timestamp_value,
                    ? AS floating_value";

                for (int iteration = 0; iteration < 2; iteration++)
                {
                    long integer = 9007199254740993 + iteration;
                    bool flag = iteration != 0;
                    string text = iteration == 0 ? "O'Brien \"quoted\" ? @p \u00e9 e\u0301 \u4e2d\u6587 \ud83d\ude00\0\r\n\t" : "";
                    using RecordBatch binding = CreateBinding(integer, flag, text);
                    statement.Bind(binding, binding.Schema);
                    QueryResult result = statement.ExecuteQuery();
                    using IArrowArrayStream stream = result.Stream ?? throw new InvalidOperationException("The query returned no result stream.");
                    using RecordBatch? row = await stream.ReadNextRecordBatchAsync();
                    Assert.NotNull(row);
                    Assert.Equal(1, row.Length);
                    Assert.Equal(12, row.ColumnCount);
                    Assert.Equal(integer, Assert.IsType<Int64Array>(row.Column(0)).GetValue(0));
                    Assert.Equal(flag, Assert.IsType<BooleanArray>(row.Column(1)).GetValue(0));
                    Assert.Equal(text, Assert.IsType<StringArray>(row.Column(2)).GetString(0));
                    Assert.Equal("007fff", Assert.IsType<StringArray>(row.Column(3)).GetString(0));
                    Assert.Equal("123.4567", Assert.IsType<StringArray>(row.Column(4)).GetString(0));
                    Assert.Equal("0.1234567891", Assert.IsType<StringArray>(row.Column(5)).GetString(0));
                    Assert.True(Assert.IsType<Int64Array>(row.Column(6)).IsNull(0));
                    Assert.Equal("2026-10-23", Assert.IsType<StringArray>(row.Column(7)).GetString(0));
                    Assert.Equal("12:34:56.125", Assert.IsType<StringArray>(row.Column(8)).GetString(0));
                    Assert.Equal("2026-10-23 12:34:56.125000", Assert.IsType<StringArray>(row.Column(9)).GetString(0));
                    Assert.Equal("2026-10-23 07:04:56.125000 +00:00", Assert.IsType<StringArray>(row.Column(10)).GetString(0));
                    Assert.Equal(123.125, Assert.IsType<DoubleArray>(row.Column(11)).GetValue(0));
                    using RecordBatch? extraRow = await stream.ReadNextRecordBatchAsync();
                    Assert.Null(extraRow);
                }
            }
        }

        private static RecordBatch CreateBinding(long integer, bool flag, string text)
        {
            IArrowArray[] arrays =
            {
                new Int64Array.Builder().Append(integer).Build(),
                new BooleanArray.Builder().Append(flag).Build(),
                new StringArray.Builder().Append(text).Build(),
                new BinaryArray.Builder().Append(new byte[] { 0, 127, 255 }).Build(),
                new Decimal128Array.Builder(new Decimal128Type(29, 4)).Append(123.4567m).Build(),
                new Decimal256Array.Builder(new Decimal256Type(38, 10)).Append(0.1234567891m).Build(),
                new Int64Array.Builder().AppendNull().Build(),
                new Date32Array.Builder().Append(new DateTime(2026, 10, 23)).Build(),
                new Time64Array.Builder(TimeUnit.Microsecond).Append(45296125000).Build(),
                new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string?)null))
                    .Append(new DateTimeOffset(2026, 10, 23, 12, 34, 56, 125, TimeSpan.Zero)).Build(),
                new TimestampWithOffsetArray.Builder(TimeUnit.Microsecond)
                    .Append(new DateTimeOffset(2026, 10, 23, 12, 34, 56, 125, TimeSpan.FromHours(5.5))).Build(),
                new DoubleArray.Builder().Append(123.125).Build()
            };
            var schema = new Schema(arrays.Select((array, index) =>
                new Field(index.ToString(CultureInfo.InvariantCulture), array.Data.DataType, true)), null);
            return new RecordBatch(schema, arrays, 1);
        }
    }
}
