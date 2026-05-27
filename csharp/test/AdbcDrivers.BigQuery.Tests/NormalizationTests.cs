/*
* Copyright (c) 2025 ADBC Drivers Contributors
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
using Apache.Arrow;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Unit tests for Arrow type normalization in BigQueryBulkIngest.
    /// BigQuery requires microsecond precision for all time/timestamp types.
    /// </summary>
    public class NormalizationTests
    {
        #region NormalizeSchema tests

        [Fact]
        public void NormalizeSchema_NoTemporalFields_ReturnsSameSchema()
        {
            var schema = new Schema(new[]
            {
                new Field("id", Int64Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: true),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            Assert.Same(schema, result);
        }

        [Fact]
        public void NormalizeSchema_Time32Second_ConvertedToTime64Microsecond()
        {
            var schema = new Schema(new[]
            {
                new Field("t", new Time32Type(TimeUnit.Second), nullable: true),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            Assert.IsType<Time64Type>(result.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, ((Time64Type)result.FieldsList[0].DataType).Unit);
        }

        [Fact]
        public void NormalizeSchema_Time32Millisecond_ConvertedToTime64Microsecond()
        {
            var schema = new Schema(new[]
            {
                new Field("t", new Time32Type(TimeUnit.Millisecond), nullable: true),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            Assert.IsType<Time64Type>(result.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, ((Time64Type)result.FieldsList[0].DataType).Unit);
        }

        [Fact]
        public void NormalizeSchema_Time64Nanosecond_ConvertedToMicrosecond()
        {
            var schema = new Schema(new[]
            {
                new Field("t", new Time64Type(TimeUnit.Nanosecond), nullable: true),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            Assert.IsType<Time64Type>(result.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, ((Time64Type)result.FieldsList[0].DataType).Unit);
        }

        [Fact]
        public void NormalizeSchema_Time64Microsecond_Unchanged()
        {
            var schema = new Schema(new[]
            {
                new Field("t", new Time64Type(TimeUnit.Microsecond), nullable: true),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            Assert.Same(schema, result);
        }

        [Fact]
        public void NormalizeSchema_TimestampNanosecond_ConvertedToMicrosecond()
        {
            var schema = new Schema(new[]
            {
                new Field("ts", new TimestampType(TimeUnit.Nanosecond, "UTC"), nullable: true),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            var tsType = Assert.IsType<TimestampType>(result.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, tsType.Unit);
            Assert.Equal("UTC", tsType.Timezone);
        }

        [Fact]
        public void NormalizeSchema_TimestampSecond_ConvertedToMicrosecond()
        {
            var schema = new Schema(new[]
            {
                new Field("ts", new TimestampType(TimeUnit.Second, "America/Los_Angeles"), nullable: false),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            var tsType = Assert.IsType<TimestampType>(result.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, tsType.Unit);
            Assert.Equal("America/Los_Angeles", tsType.Timezone);
        }

        [Fact]
        public void NormalizeSchema_TimestampMicrosecond_Unchanged()
        {
            var schema = new Schema(new[]
            {
                new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            Assert.Same(schema, result);
        }

        [Fact]
        public void NormalizeSchema_MixedFields_OnlyTemporalFieldsNormalized()
        {
            var schema = new Schema(new[]
            {
                new Field("id", Int64Type.Default, nullable: false),
                new Field("t", new Time32Type(TimeUnit.Second), nullable: true),
                new Field("name", StringType.Default, nullable: true),
                new Field("ts", new TimestampType(TimeUnit.Nanosecond, "UTC"), nullable: true),
                new Field("value", DoubleType.Default, nullable: true),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            Assert.IsType<Int64Type>(result.FieldsList[0].DataType);
            Assert.IsType<Time64Type>(result.FieldsList[1].DataType);
            Assert.Equal(TimeUnit.Microsecond, ((Time64Type)result.FieldsList[1].DataType).Unit);
            Assert.IsType<StringType>(result.FieldsList[2].DataType);
            var tsType = Assert.IsType<TimestampType>(result.FieldsList[3].DataType);
            Assert.Equal(TimeUnit.Microsecond, tsType.Unit);
            Assert.IsType<DoubleType>(result.FieldsList[4].DataType);
        }

        [Fact]
        public void NormalizeSchema_PreservesNullabilityAndMetadata()
        {
            var metadata = new Dictionary<string, string> { { "key", "value" } };
            var schema = new Schema(new[]
            {
                new Field("t", new Time32Type(TimeUnit.Second), nullable: false, metadata),
            }, null);

            Schema result = BigQueryBulkIngest.NormalizeSchema(schema);

            Assert.False(result.FieldsList[0].IsNullable);
            Assert.Equal("value", result.FieldsList[0].Metadata["key"]);
        }

        #endregion

        #region NormalizeBatch tests

        [Fact]
        public void NormalizeBatch_NoTemporalFields_ReturnsSameBatch()
        {
            var batch = CreateBatchWithTypes(
                new Field("id", Int64Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: true));

            RecordBatch result = BigQueryBulkIngest.NormalizeBatch(batch);

            Assert.Same(batch, result);
        }

        [Fact]
        public void NormalizeBatch_Time32Second_ConvertedToMicroseconds()
        {
            int secondsValue = 3661; // 1h 1m 1s
            var builder = new Time32Array.Builder(TimeUnit.Second);
            builder.Append(secondsValue);
            builder.AppendNull();

            var schema = new Schema(new[] { new Field("t", new Time32Type(TimeUnit.Second), nullable: true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { builder.Build() }, 2);

            RecordBatch result = BigQueryBulkIngest.NormalizeBatch(batch);

            Assert.IsType<Time64Type>(result.Schema.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, ((Time64Type)result.Schema.FieldsList[0].DataType).Unit);

            var array = (Time64Array)result.Column(0);
            Assert.Equal(secondsValue * 1_000_000L, array.GetValue(0));
            Assert.True(array.IsNull(1));
        }

        [Fact]
        public void NormalizeBatch_Time32Millisecond_ConvertedToMicroseconds()
        {
            int msValue = 3661000; // 1h 1m 1s in ms
            var builder = new Time32Array.Builder(TimeUnit.Millisecond);
            builder.Append(msValue);

            var schema = new Schema(new[] { new Field("t", new Time32Type(TimeUnit.Millisecond), nullable: true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { builder.Build() }, 1);

            RecordBatch result = BigQueryBulkIngest.NormalizeBatch(batch);

            var array = (Time64Array)result.Column(0);
            Assert.Equal(msValue * 1_000L, array.GetValue(0));
        }

        [Fact]
        public void NormalizeBatch_Time64Nanosecond_ConvertedToMicroseconds()
        {
            long nsValue = 3_661_000_000_000L; // 1h 1m 1s in ns
            var builder = new Time64Array.Builder(TimeUnit.Nanosecond);
            builder.Append(nsValue);

            var schema = new Schema(new[] { new Field("t", new Time64Type(TimeUnit.Nanosecond), nullable: true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { builder.Build() }, 1);

            RecordBatch result = BigQueryBulkIngest.NormalizeBatch(batch);

            var array = (Time64Array)result.Column(0);
            Assert.Equal(nsValue / 1_000L, array.GetValue(0));
        }

        [Fact]
        public void NormalizeBatch_TimestampNanosecond_ConvertedToMicroseconds()
        {
            var ts = new DateTimeOffset(2024, 6, 15, 12, 30, 45, TimeSpan.Zero);
            var builder = new TimestampArray.Builder(new TimestampType(TimeUnit.Nanosecond, "UTC"));
            builder.Append(ts);
            builder.AppendNull();

            var schema = new Schema(new[] { new Field("ts", new TimestampType(TimeUnit.Nanosecond, "UTC"), nullable: true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { builder.Build() }, 2);

            RecordBatch result = BigQueryBulkIngest.NormalizeBatch(batch);

            var resultType = Assert.IsType<TimestampType>(result.Schema.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, resultType.Unit);
            Assert.Equal("UTC", resultType.Timezone);

            var array = (TimestampArray)result.Column(0);
            // Verify the timestamp round-trips correctly
            Assert.Equal(ts, array.GetTimestamp(0));
            Assert.True(array.IsNull(1));
        }

        [Fact]
        public void NormalizeBatch_TimestampWithoutTimezone_PreservesNullTimezone()
        {
            var ts = new DateTimeOffset(2024, 6, 15, 12, 30, 45, TimeSpan.Zero);
            var builder = new TimestampArray.Builder(new TimestampType(TimeUnit.Nanosecond, (string?)null));
            builder.Append(ts);

            var schema = new Schema(new[] { new Field("ts", new TimestampType(TimeUnit.Nanosecond, (string?)null), nullable: true) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { builder.Build() }, 1);

            RecordBatch result = BigQueryBulkIngest.NormalizeBatch(batch);

            var resultType = Assert.IsType<TimestampType>(result.Schema.FieldsList[0].DataType);
            Assert.Equal(TimeUnit.Microsecond, resultType.Unit);
            Assert.Null(resultType.Timezone);
        }

        [Fact]
        public void NormalizeBatch_PreservesRowCount()
        {
            var builder = new Time32Array.Builder(TimeUnit.Second);
            builder.Append(1);
            builder.Append(2);
            builder.Append(3);

            var schema = new Schema(new[] { new Field("t", new Time32Type(TimeUnit.Second), nullable: false) }, null);
            var batch = new RecordBatch(schema, new IArrowArray[] { builder.Build() }, 3);

            RecordBatch result = BigQueryBulkIngest.NormalizeBatch(batch);

            Assert.Equal(3, result.Length);
        }

        #endregion

        #region Helpers

        private static RecordBatch CreateBatchWithTypes(params Field[] fields)
        {
            var schema = new Schema(fields, null);
            var arrays = new IArrowArray[fields.Length];

            for (int i = 0; i < fields.Length; i++)
            {
                arrays[i] = fields[i].DataType switch
                {
                    Int64Type => new Int64Array.Builder().Append(1).Build(),
                    StringType => new StringArray.Builder().Append("test").Build(),
                    _ => throw new NotSupportedException($"Test helper does not support {fields[i].DataType}")
                };
            }

            return new RecordBatch(schema, arrays, 1);
        }

        #endregion
    }
}
