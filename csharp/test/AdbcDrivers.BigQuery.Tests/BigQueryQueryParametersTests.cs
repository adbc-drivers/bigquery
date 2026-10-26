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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Moq;
using Xunit;
#if NET8_0_OR_GREATER
using AdbcDrivers.BigQuery.MockServer;
#endif

namespace AdbcDrivers.BigQuery.Tests
{
    public class BigQueryQueryParametersTests
    {
        public static IEnumerable<object[]> ScalarParameters()
        {
            yield return Case(new BooleanArray.Builder().Append(false).Build(), BigQueryDbType.Bool, "FALSE");
            yield return Case(new BooleanArray.Builder().Append(true).Build(), BigQueryDbType.Bool, "TRUE");
            yield return Case(new Int8Array.Builder().Append(-5).Build(), BigQueryDbType.Int64, "-5");
            yield return Case(new Int16Array.Builder().Append(-500).Build(), BigQueryDbType.Int64, "-500");
            yield return Case(new Int32Array.Builder().Append(-5000123).Build(), BigQueryDbType.Int64, "-5000123");
            yield return Case(new Int64Array.Builder().Append(9007199254740993).Build(), BigQueryDbType.Int64, "9007199254740993");
            yield return Case(new Int64Array.Builder().Append(long.MinValue).Build(), BigQueryDbType.Int64, "-9223372036854775808");
            yield return Case(new UInt8Array.Builder().Append(255).Build(), BigQueryDbType.Int64, "255");
            yield return Case(new UInt16Array.Builder().Append(65535).Build(), BigQueryDbType.Int64, "65535");
            yield return Case(new UInt32Array.Builder().Append(uint.MaxValue).Build(), BigQueryDbType.Int64, "4294967295");
            yield return Case(new UInt64Array.Builder().Append((ulong)long.MaxValue).Build(), BigQueryDbType.Int64, "9223372036854775807");
            yield return Case(new DoubleArray.Builder().Append(123.125).Build(), BigQueryDbType.Float64, "123.125");
            yield return Case(new DoubleArray.Builder().Append(0.1).Build(), BigQueryDbType.Float64, "0.10000000000000001");
            yield return Case(new DoubleArray.Builder().Append(0.84551240822557006).Build(), BigQueryDbType.Float64, "0.84551240822557006");
            yield return Case(new DoubleArray.Builder().Append(double.MaxValue).Build(), BigQueryDbType.Float64, "1.7976931348623157E+308");
            yield return Case(new DoubleArray.Builder().Append(double.MinValue).Build(), BigQueryDbType.Float64, "-1.7976931348623157E+308");
            yield return Case(new DoubleArray.Builder().Append(double.Epsilon).Build(), BigQueryDbType.Float64, "4.9406564584124654E-324");
            yield return Case(new FloatArray.Builder().Append(1.5f).Build(), BigQueryDbType.Float64, "1.5");
            yield return Case(new FloatArray.Builder().Append(0.1f).Build(), BigQueryDbType.Float64, "0.100000001");
            yield return Case(new FloatArray.Builder().Append(float.MaxValue).Build(), BigQueryDbType.Float64, "3.40282347E+38");
            yield return Case(new FloatArray.Builder().Append(float.MinValue).Build(), BigQueryDbType.Float64, "-3.40282347E+38");
            yield return Case(new FloatArray.Builder().Append(float.Epsilon).Build(), BigQueryDbType.Float64, "1.40129846E-45");
            yield return Case(new FloatArray.Builder().Append(float.NaN).Build(), BigQueryDbType.Float64, "NaN");
            yield return Case(new FloatArray.Builder().Append(float.PositiveInfinity).Build(), BigQueryDbType.Float64, "+inf");
            yield return Case(new FloatArray.Builder().Append(float.NegativeInfinity).Build(), BigQueryDbType.Float64, "-inf");
            yield return Case(new DoubleArray.Builder().Append(double.NaN).Build(), BigQueryDbType.Float64, "NaN");
            yield return Case(new DoubleArray.Builder().Append(double.PositiveInfinity).Build(), BigQueryDbType.Float64, "+inf");
            yield return Case(new DoubleArray.Builder().Append(double.NegativeInfinity).Build(), BigQueryDbType.Float64, "-inf");
            yield return Case(new StringArray.Builder().Append("O'Brien ? @p; -- \u4e2d\u6587 \u00e9 e\u0301 \ud83d\ude00\0\n\t").Build(),
                BigQueryDbType.String, "O'Brien ? @p; -- \u4e2d\u6587 \u00e9 e\u0301 \ud83d\ude00\0\n\t");
            yield return Case(new StringArray.Builder().Append("").Build(), BigQueryDbType.String, "");
            yield return Case(new BinaryArray.Builder().Append(new byte[] { 0, 127, 255 }).Build(), BigQueryDbType.Bytes, "AH//");
            yield return Case(new Int64Array.Builder().AppendNull().Build(), BigQueryDbType.Int64, null);
            yield return Case(new StringArray.Builder().AppendNull().Build(), BigQueryDbType.String, null);
            yield return Case(new BooleanArray.Builder().AppendNull().Build(), BigQueryDbType.Bool, null);
            yield return Case(new Decimal128Array.Builder(new Decimal128Type(29, 4)).Append(123.4567m).Build(),
                BigQueryDbType.Numeric, "123.4567");
            yield return Case(new Decimal128Array.Builder(new Decimal128Type(29, 10)).Append(0.1234567891m).Build(),
                BigQueryDbType.BigNumeric, "0.1234567891");
            yield return Case(new Decimal256Array.Builder(new Decimal256Type(38, 10)).Append(0.1234567891m).Build(),
                BigQueryDbType.BigNumeric, "0.1234567891");
            yield return Case(new Date32Array.Builder().Append(new DateTime(2026, 10, 23)).Build(), BigQueryDbType.Date, "2026-10-23");
            yield return Case(new Date64Array.Builder().Append(new DateTime(2026, 10, 23)).Build(), BigQueryDbType.Date, "2026-10-23");
            yield return Case(new Time32Array.Builder(TimeUnit.Millisecond).Append(45296125).Build(), BigQueryDbType.Time, "12:34:56.125000");
            yield return Case(new Time32Array.Builder(TimeUnit.Second).Append(45296).Build(), BigQueryDbType.Time, "12:34:56.000000");
            yield return Case(new Time64Array.Builder(TimeUnit.Microsecond).Append(45296123456).Build(), BigQueryDbType.Time, "12:34:56.123456");
            yield return Case(new Time64Array.Builder(TimeUnit.Nanosecond).Append(45296123456000).Build(), BigQueryDbType.Time, "12:34:56.123456");
            yield return Case(Timestamp(1000000, TimeUnit.Nanosecond, "UTC"),
                BigQueryDbType.Timestamp, "1970-01-01 00:00:00.001000+00:00");
            yield return Case(Timestamp(1, TimeUnit.Millisecond, "UTC"),
                BigQueryDbType.Timestamp, "1970-01-01 00:00:00.001000+00:00");
            yield return Case(Timestamp(1, TimeUnit.Second, "UTC"),
                BigQueryDbType.Timestamp, "1970-01-01 00:00:01.000000+00:00");
            yield return Case(Timestamp(0, TimeUnit.Microsecond, "UTC"),
                BigQueryDbType.Timestamp, "1970-01-01 00:00:00.000000+00:00");
            yield return Case(Timestamp(0, TimeUnit.Microsecond, null),
                BigQueryDbType.DateTime, "1970-01-01 00:00:00.000000");
            yield return Case(Timestamp(-1, TimeUnit.Microsecond, "UTC"),
                BigQueryDbType.Timestamp, "1969-12-31 23:59:59.999999+00:00");
            yield return Case(new TimestampWithOffsetArray.Builder(TimeUnit.Microsecond)
                .Append(new DateTimeOffset(2026, 10, 23, 12, 34, 56, 125, TimeSpan.FromHours(5.5))).Build(),
                BigQueryDbType.Timestamp, "2026-10-23 07:04:56.125000+00:00");
            yield return Case(new TimestampWithOffsetArray.Builder(TimeUnit.Microsecond).AppendNull().Build(),
                BigQueryDbType.Timestamp, null);
        }

        private static object[] Case(IArrowArray array, BigQueryDbType type, string? value) => new object[] { array, type, value! };

        private static TimestampArray Timestamp(long value, TimeUnit unit, string? timezone) =>
            new(new TimestampType(unit, timezone), new ArrowBuffer.Builder<long>().Append(value).Build(), ArrowBuffer.Empty, 1, 0, 0);

        private static RecordBatch Batch(params IArrowArray[] arrays) =>
            new(new Schema(arrays.Select((a, i) => new Field(i.ToString(CultureInfo.InvariantCulture), a.Data.DataType, true)), null),
                arrays, arrays.Length == 0 ? 1 : arrays[0].Length);

        [Theory]
        [MemberData(nameof(ScalarParameters))]
        public void SerializesScalarIdenticallyForBothQueryPaths(IArrowArray array, BigQueryDbType type, string? value)
        {
            using RecordBatch batch = Batch(array);
            BigQueryQueryParameters parameters = BigQueryQueryParameters.FromBatch(batch);
            BigQueryParameter parameter = Assert.Single(parameters.Parameters);
            Assert.Null(parameter.Name);
            Assert.Equal(type, parameter.Type);
            Assert.Equal(value, parameter.Value);

            var options = new QueryOptions();
            parameters.ApplyTo(options);
            Assert.Equal(BigQueryParameterMode.Positional, options.ParameterMode);
            Assert.False(options.UseLegacySql);

            var request = new QueryRequest();
            parameters.ApplyTo(request);
            Assert.Equal("POSITIONAL", request.ParameterMode);
            Assert.False(request.UseLegacySql);
            QueryParameter restParameter = Assert.Single(request.QueryParameters);
            Assert.Null(restParameter.Name);
            Assert.Equal(type.ToString().ToUpperInvariant(), restParameter.ParameterType.Type);
            Assert.Equal(value, restParameter.ParameterValue.Value);
        }

        [Fact]
        public void PreservesOrderAndDoesNotRewriteSql()
        {
            using RecordBatch batch = Batch(new Int64Array.Builder().Append(42).Build(), new StringArray.Builder().Append("?").Build());
            var parameters = BigQueryQueryParameters.FromBatch(batch);
            var request = new QueryRequest { Query = "SELECT ?, ?" };
            parameters.ApplyTo(request);
            Assert.Equal("SELECT ?, ?", request.Query);
            Assert.Equal(new[] { "42", "?" }, request.QueryParameters.Select(p => p.ParameterValue.Value));
            Assert.Equal(new[] { "42", "?" }, parameters.Parameters.Select(p => p.Value));
        }

        [Fact]
        public void RequestProjectionsDoNotMutateTheSnapshot()
        {
            using RecordBatch batch = Batch(new Int64Array.Builder().Append(42).Build());
            var parameters = BigQueryQueryParameters.FromBatch(batch);
            Assert.Single(parameters.Parameters).Value = "changed";
            var firstRequest = new QueryRequest();
            parameters.ApplyTo(firstRequest);
            Assert.Single(firstRequest.QueryParameters).ParameterValue.Value = "changed";

            var secondRequest = new QueryRequest();
            parameters.ApplyTo(secondRequest);
            Assert.Equal("42", Assert.Single(secondRequest.QueryParameters).ParameterValue.Value);
            Assert.Equal("42", Assert.Single(parameters.Parameters).Value);
        }

        [Fact]
        public void RejectsLegacySql()
        {
            using RecordBatch batch = Batch(new Int64Array.Builder().Append(1).Build());
            var exception = Assert.Throws<AdbcException>(() =>
                BigQueryQueryParameters.FromBatch(batch).ApplyTo(new QueryOptions { UseLegacySql = true }));
            Assert.Equal(AdbcStatusCode.InvalidArgument, exception.Status);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(2)]
        public void RejectsMultipleOrMissingRows(int rows)
        {
            var builder = new Int64Array.Builder();
            for (int i = 0; i < rows; i++) builder.Append(i);
            using RecordBatch batch = Batch(builder.Build());
            Assert.Equal(AdbcStatusCode.InvalidArgument, Assert.Throws<AdbcException>(() => BigQueryQueryParameters.FromBatch(batch)).Status);
        }

        [Fact]
        public void RejectsUnsignedOverflow()
        {
            using RecordBatch batch = Batch(new UInt64Array.Builder().Append(ulong.MaxValue).Build());
            Assert.Equal(AdbcStatusCode.InvalidArgument, Assert.Throws<AdbcException>(() => BigQueryQueryParameters.FromBatch(batch)).Status);
        }

        [Fact]
        public void RejectsDurationInsteadOfTreatingItAsTimeOrNumber()
        {
            using RecordBatch batch = Batch(new DurationArray.Builder(DurationType.Nanosecond).Append(86400000000000).Build());
            var exception = Assert.Throws<AdbcException>(() => BigQueryQueryParameters.FromBatch(batch));
            Assert.Equal(AdbcStatusCode.NotImplemented, exception.Status);
            Assert.Contains("Duration", exception.Message);
        }

        [Fact]
        public void RejectsSubMicrosecondPrecision()
        {
            using RecordBatch batch = Batch(Timestamp(1, TimeUnit.Nanosecond, "UTC"));
            Assert.Equal(AdbcStatusCode.InvalidArgument, Assert.Throws<AdbcException>(() => BigQueryQueryParameters.FromBatch(batch)).Status);
        }

        [Theory]
        [InlineData(-62135596800000001)]
        [InlineData(253402300800000000)]
        [InlineData(long.MaxValue)]
        [InlineData(long.MinValue)]
        public void RejectsOutOfRangeTimestampsBeforeArrowTickArithmeticCanOverflow(long value)
        {
            using RecordBatch batch = Batch(Timestamp(value, TimeUnit.Microsecond, "UTC"));
            Assert.Equal(AdbcStatusCode.InvalidArgument, Assert.Throws<AdbcException>(() => BigQueryQueryParameters.FromBatch(batch)).Status);
        }

        [Theory]
        [InlineData(-62135596800000000, "0001-01-01 00:00:00.000000+00:00")]
        [InlineData(253402300799999999, "9999-12-31 23:59:59.999999+00:00")]
        public void SupportsTimestampRangeBoundaries(long value, string expected)
        {
            using RecordBatch batch = Batch(Timestamp(value, TimeUnit.Microsecond, "UTC"));
            Assert.Equal(expected, Assert.Single(BigQueryQueryParameters.FromBatch(batch).Parameters).Value);
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(86400000000)]
        public void RejectsTimeOutsideSingleDay(long microseconds)
        {
            using RecordBatch batch = Batch(new Time64Array.Builder(TimeUnit.Microsecond).Append(microseconds).Build());
            Assert.Equal(AdbcStatusCode.InvalidArgument, Assert.Throws<AdbcException>(() => BigQueryQueryParameters.FromBatch(batch)).Status);
        }

        [Fact]
        public async Task BindingSnapshotsCanBeReusedAndRebound()
        {
            using var connection = new BigQueryConnection(new Dictionary<string, string>());
            using var statement = new BigQueryStatement(connection);
            Assert.Null(await statement.GetQueryParametersAsync(CancellationToken.None));
            using RecordBatch first = Batch(new Int64Array.Builder().Append(1).Build());
            statement.Bind(first, first.Schema);
            var snapshot = await statement.GetQueryParametersAsync(CancellationToken.None);
            Assert.Same(snapshot, await statement.GetQueryParametersAsync(CancellationToken.None));
            using RecordBatch second = Batch(new Int64Array.Builder().Append(2).Build());
            statement.Bind(second, second.Schema);
            Assert.Equal("2", Assert.Single((await statement.GetQueryParametersAsync(CancellationToken.None))!.Parameters).Value);
            Assert.Equal("1", Assert.Single(snapshot!.Parameters).Value);
        }

        [Fact]
        public async Task StreamBindingIsConsumedOnceAndReplacesBatchBinding()
        {
            using var connection = new BigQueryConnection(new Dictionary<string, string>());
            using var statement = new BigQueryStatement(connection);
            using RecordBatch batch = Batch(new Int64Array.Builder().Append(1).Build());
            statement.Bind(batch, batch.Schema);
            var stream = new Mock<IArrowArrayStream>(MockBehavior.Strict);
            stream.SetupSequence(s => s.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<RecordBatch?>(Batch(new Int64Array.Builder().Append(2).Build())))
                .Returns(new ValueTask<RecordBatch?>((RecordBatch?)null));
            statement.BindStream(stream.Object);
            var parameters = await statement.GetQueryParametersAsync(CancellationToken.None);
            Assert.Equal("2", Assert.Single(parameters!.Parameters).Value);
            Assert.Same(parameters, await statement.GetQueryParametersAsync(CancellationToken.None));
            stream.Verify(s => s.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Fact]
        public async Task RejectsMultipleStreamRowsAcrossBatches()
        {
            var stream = new Mock<IArrowArrayStream>(MockBehavior.Strict);
            stream.SetupSequence(s => s.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<RecordBatch?>(Batch(new Int64Array.Builder().Append(1).Build())))
                .Returns(new ValueTask<RecordBatch?>(Batch(new Int64Array.Builder().Append(2).Build())));
            var exception = await Assert.ThrowsAsync<AdbcException>(() => BigQueryQueryParameters.FromStreamAsync(stream.Object, CancellationToken.None));
            Assert.Equal(AdbcStatusCode.InvalidArgument, exception.Status);
        }

        [Fact]
        public void SerializationIsCultureInvariant()
        {
            CultureInfo previous = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
                using RecordBatch batch = Batch(
                    new DoubleArray.Builder().Append(123.125).Build(),
                    new DoubleArray.Builder().Append(0.1).Build(),
                    new FloatArray.Builder().Append(0.1f).Build(),
                    new Decimal128Array.Builder(new Decimal128Type(29, 4)).Append(123.4567m).Build());
                var parameters = BigQueryQueryParameters.FromBatch(batch);
                var expected = new[] { "123.125", "0.10000000000000001", "0.100000001", "123.4567" };
                Assert.Equal(expected, parameters.Parameters.Select(p => p.Value));
                var request = new QueryRequest();
                parameters.ApplyTo(request);
                Assert.Equal(expected, request.QueryParameters.Select(p => p.ParameterValue.Value));
            }
            finally
            {
                CultureInfo.CurrentCulture = previous;
            }
        }

        [Fact]
        public async Task EmptyStreamIsRejected()
        {
            var stream = new Mock<IArrowArrayStream>(MockBehavior.Strict);
            stream.Setup(s => s.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<RecordBatch?>((RecordBatch?)null));
            var exception = await Assert.ThrowsAsync<AdbcException>(() => BigQueryQueryParameters.FromStreamAsync(stream.Object, CancellationToken.None));
            Assert.Equal(AdbcStatusCode.InvalidArgument, exception.Status);
        }

        [Fact]
        public async Task CancellationStopsReadingBindings()
        {
            var stream = new Mock<IArrowArrayStream>(MockBehavior.Strict);
            using var source = new CancellationTokenSource();
            source.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => BigQueryQueryParameters.FromStreamAsync(stream.Object, source.Token));
            stream.Verify(s => s.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()), Times.Never);
        }

#if NET8_0_OR_GREATER
        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public void FailedStreamValidationCannotSubmitRemainingRows(bool executeUpdate, bool rebindStream)
        {
            using var server = new BigQueryMockServer();
            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(new Dictionary<string, string>
            {
                [BigQueryParameters.ProjectId] = "mock-project",
                [BigQueryParameters.AuthenticationType] = BigQueryConstants.MockAuthenticationType,
                [BigQueryParameters.TestRestEndpoint] = server.RestEndpoint,
                [BigQueryParameters.TestStorageEndpoint] = server.GrpcEndpoint
            });
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = executeUpdate ? "UPDATE t SET value = ?" : "SELECT ? AS value";
            var stream = new Mock<IArrowArrayStream>(MockBehavior.Strict);
            stream.SetupSequence(s => s.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .Returns(() => new ValueTask<RecordBatch?>(Batch(new Int64Array.Builder().Append(1).Build())))
                .Returns(() => new ValueTask<RecordBatch?>(Batch(new Int64Array.Builder().Append(2).Build())))
                .Returns(() => new ValueTask<RecordBatch?>(Batch(new Int64Array.Builder().Append(3).Build())))
                .Returns(new ValueTask<RecordBatch?>((RecordBatch?)null));
            statement.BindStream(stream.Object);

            void Execute()
            {
                if (executeUpdate)
                {
                    statement.ExecuteUpdate();
                }
                else
                {
                    statement.ExecuteQuery().Stream?.Dispose();
                }
            }

            AdbcException failure = Assert.Throws<AdbcException>(Execute);
            Assert.Equal(AdbcStatusCode.InvalidArgument, failure.Status);
            Assert.Same(failure, Assert.Throws<AdbcException>(Execute));
            Assert.Empty(server.ExecutedQueries);
            stream.Verify(s => s.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));

            using RecordBatch replacementBatch = Batch(new Int64Array.Builder().Append(4).Build());
            if (rebindStream)
            {
                var replacement = new Mock<IArrowArrayStream>(MockBehavior.Strict);
                replacement.SetupSequence(s => s.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                    .Returns(() => new ValueTask<RecordBatch?>(Batch(new Int64Array.Builder().Append(4).Build())))
                    .Returns(new ValueTask<RecordBatch?>((RecordBatch?)null));
                statement.BindStream(replacement.Object);
            }
            else
            {
                statement.Bind(replacementBatch, replacementBatch.Schema);
            }

            Execute();
            Execute();
            Assert.Equal(2, server.ExecutedQueries.Count);
            Assert.All(server.ExecutedQueryParameters, parameters =>
                Assert.Equal("4", Assert.Single(parameters).ParameterValue.Value));
            stream.Verify(s => s.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void QuerySubmissionActuallySendsBindings(bool useQueryEndpoint)
        {
            using var server = new BigQueryMockServer();
            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(new Dictionary<string, string>
            {
                [BigQueryParameters.ProjectId] = "mock-project",
                [BigQueryParameters.AuthenticationType] = BigQueryConstants.MockAuthenticationType,
                [BigQueryParameters.TestRestEndpoint] = server.RestEndpoint,
                [BigQueryParameters.TestStorageEndpoint] = server.GrpcEndpoint
            });
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SetOption(BigQueryParameters.UseJobCreationMode, useQueryEndpoint ? "true" : "false");
            statement.SqlQuery = "SELECT ? AS value, ? AS label";
            const string label = "O'Brien ? \u4e2d\u6587 \ud83d\ude00\0";
            using RecordBatch batch = Batch(
                new Int64Array.Builder().Append(9007199254740993).Build(),
                new StringArray.Builder().Append(label).Build());
            statement.Bind(batch, batch.Schema);

            statement.ExecuteQuery().Stream?.Dispose();

            Assert.Equal(statement.SqlQuery, Assert.Single(server.ExecutedQueries));
            var parameters = Assert.Single(server.ExecutedQueryParameters);
            Assert.Equal(new[] { "9007199254740993", label }, parameters.Select(p => p.ParameterValue.Value));
            Assert.Equal(new[] { useQueryEndpoint ? "INT64" : "INTEGER", "STRING" }, parameters.Select(p => p.ParameterType.Type));
            Assert.All(parameters, p => Assert.True(string.IsNullOrEmpty(p.Name)));
            if (useQueryEndpoint)
            {
                Assert.Single(server.RequestsOfKind(MockRequestKind.JobQuery));
                Assert.Empty(server.RequestsOfKind(MockRequestKind.JobInsert));
                Assert.Equal("POSITIONAL", server.LastQueryRequest!.ParameterMode);
                Assert.False(server.LastQueryRequest.UseLegacySql);
            }
            else
            {
                Assert.Single(server.RequestsOfKind(MockRequestKind.JobInsert));
                Assert.Empty(server.RequestsOfKind(MockRequestKind.JobQuery));
                Assert.Equal("positional", server.LastInsertedJob!.Configuration.Query.ParameterMode);
                Assert.False(server.LastInsertedJob.Configuration.Query.UseLegacySql);
            }
        }

        [Fact]
        public void UpdateSubmissionActuallySendsBindings()
        {
            using var server = new BigQueryMockServer();
            using var driver = new BigQueryDriver();
            using AdbcDatabase database = driver.Open(new Dictionary<string, string>
            {
                [BigQueryParameters.ProjectId] = "mock-project",
                [BigQueryParameters.AuthenticationType] = BigQueryConstants.MockAuthenticationType,
                [BigQueryParameters.TestRestEndpoint] = server.RestEndpoint,
                [BigQueryParameters.TestStorageEndpoint] = server.GrpcEndpoint
            });
            using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "UPDATE t SET value = ?";
            using RecordBatch batch = Batch(new BooleanArray.Builder().Append(false).Build());
            statement.Bind(batch, batch.Schema);

            statement.ExecuteUpdate();

            QueryParameter parameter = Assert.Single(Assert.Single(server.ExecutedQueryParameters));
            Assert.Equal("FALSE", parameter.ParameterValue.Value);
            Assert.Equal("BOOL", parameter.ParameterType.Type);
            Assert.Equal("positional", server.LastInsertedJob!.Configuration.Query.ParameterMode);
        }
#endif
    }
}
