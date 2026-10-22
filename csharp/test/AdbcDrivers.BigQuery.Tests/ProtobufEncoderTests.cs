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
using System.Globalization;
using System.Linq;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    public class ProtobufEncoderTests
    {
        [Fact]
        public void EncodesNonNullDefaultsButOmitsNulls()
        {
            using RecordBatch batch = Batch(
                new Int64Array.Builder().Append(-1).Append(0).AppendNull().Build(),
                new BooleanArray.Builder().Append(true).Append(false).AppendNull().Build(),
                new StringArray.Builder().Append("text").Append("").AppendNull().Build(),
                new BinaryArray.Builder().Append(new byte[] { 0, 255 }).Append(System.Array.Empty<byte>()).AppendNull().Build(),
                new FloatArray.Builder().Append(1.5f).Append(0).AppendNull().Build(),
                new DoubleArray.Builder().Append(-1.25).Append(0).AppendNull().Build());
            ProtoRows rows = new ProtobufEncoder(batch.Schema).Encode(batch);
            Assert.Equal(3, rows.SerializedRows.Count);
            for (int row = 0; row < 2; row++)
            {
                using var input = rows.SerializedRows[row].CreateCodedInput();
                Assert.Equal(8U, input.ReadTag());
                Assert.Equal(row == 0 ? -1L : 0L, input.ReadInt64());
                Assert.Equal(16U, input.ReadTag());
                Assert.Equal(row == 0, input.ReadBool());
                Assert.Equal(26U, input.ReadTag());
                Assert.Equal(row == 0 ? "text" : "", input.ReadString());
                Assert.Equal(34U, input.ReadTag());
                Assert.Equal(row == 0 ? ByteString.CopyFrom(new byte[] { 0, 255 }) : ByteString.Empty, input.ReadBytes());
                Assert.Equal(45U, input.ReadTag());
                Assert.Equal(row == 0 ? 1.5f : 0, input.ReadFloat());
                Assert.Equal(49U, input.ReadTag());
                Assert.Equal(row == 0 ? -1.25 : 0, input.ReadDouble());
                Assert.True(input.IsAtEnd);
            }
            Assert.Equal(ByteString.Empty, rows.SerializedRows[2]);
        }

        [Fact]
        public void PreservesFullDecimalPrecisionIndependentlyOfCulture()
        {
            const string numeric = "-99999999999999999999999999999.999999999";
            const string bigNumeric = "99999999999999999999999999999999999999.99999999999999999999999999999999999999";
            using RecordBatch batch = Batch(
                new Decimal128Array.Builder(new Decimal128Type(38, 9)).Append(numeric).Build(),
                new Decimal256Array.Builder(new Decimal256Type(76, 38)).Append(bigNumeric).Build());
            CultureInfo original = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
                using var input = new ProtobufEncoder(batch.Schema).Encode(batch).SerializedRows[0].CreateCodedInput();
                Assert.Equal(10U, input.ReadTag());
                Assert.Equal(numeric, input.ReadString());
                Assert.Equal(18U, input.ReadTag());
                Assert.Equal(bigNumeric, input.ReadString());
                Assert.True(input.IsAtEnd);
            }
            finally
            {
                CultureInfo.CurrentCulture = original;
            }
        }

        [Theory]
        [InlineData(TimeUnit.Second, -1L, 3661L, 0L)]
        [InlineData(TimeUnit.Millisecond, -877L, 3661123L, 123000L)]
        [InlineData(TimeUnit.Microsecond, -876544L, 3661123456L, 123456L)]
        [InlineData(TimeUnit.Nanosecond, -876544000L, 3661123456789L, 123456L)]
        public void EncodesTemporalUnitsAndPreEpochCivilDateTime(TimeUnit unit, long timestampValue, long timeValue, long fraction)
        {
            var instant = new DateTimeOffset(1969, 12, 31, 23, 59, 59, TimeSpan.Zero).AddTicks(fraction * 10);
            IArrowArray time = unit == TimeUnit.Second || unit == TimeUnit.Millisecond
                ? new Time32Array.Builder(unit).Append(checked((int)timeValue)).Build()
                : new Time64Array.Builder(unit).Append(timeValue).Build();
            using RecordBatch batch = Batch(
                new TimestampArray.Builder(new TimestampType(unit, "UTC")).Append(instant).Build(),
                new TimestampArray.Builder(new TimestampType(unit, (string?)null)).Append(instant).Build(),
                time,
                new Date32Array.Builder().Append(new DateTime(1969, 12, 31)).Build(),
                new Date64Array.Builder().Append(new DateTime(1969, 12, 31)).Build());
            using var input = new ProtobufEncoder(batch.Schema).Encode(batch).SerializedRows[0].CreateCodedInput();
            Assert.Equal(8U, input.ReadTag());
            long expectedMicros = unit == TimeUnit.Second ? timestampValue * 1_000_000
                : unit == TimeUnit.Millisecond ? timestampValue * 1_000
                : unit == TimeUnit.Nanosecond ? timestampValue / 1_000 : timestampValue;
            Assert.Equal(expectedMicros, input.ReadInt64());
            Assert.Equal(16U, input.ReadTag());
            Assert.Equal((1969L << 46) | (12L << 42) | (31L << 37) | (23L << 32) | (59L << 26) | (59L << 20) | fraction,
                input.ReadInt64());
            Assert.Equal(24U, input.ReadTag());
            Assert.Equal((1L << 32) | (1L << 26) | (1L << 20) | fraction, input.ReadInt64());
            Assert.Equal(32U, input.ReadTag());
            Assert.Equal(-1, input.ReadInt32());
            Assert.Equal(40U, input.ReadTag());
            Assert.Equal(-1, input.ReadInt32());
            Assert.True(input.IsAtEnd);
        }

        [Fact]
        public void AnnotatesNestedNamesAndRespectsSlicedStructAndListOffsets()
        {
            var recordType = new StructType(new[] { new Field("date accessed", Int64Type.Default, true) });
            var records = new StructArray(recordType, 4, new IArrowArray[]
            {
                new Int64Array.Builder().Append(99).Append(7).Append(8).Append(9).Build(),
            }, new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Append(false).Append(true).Build(), 1);
            var list = new ListArray(new ListType(Int64Type.Default), 4,
                new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(3).Append(3).Append(4).Build(),
                new Int64Array.Builder().Append(99).Append(10).Append(11).Append(12).Build(), ArrowBuffer.Empty);
            using RecordBatch original = Batch(records, list);
            using RecordBatch batch = original.Slice(1, 2);
            var encoder = new ProtobufEncoder(batch.Schema);
            DescriptorProto descriptor = encoder.WriterSchema.ProtoDescriptor;
            Assert.Equal("column 0", descriptor.Field[0].Options.GetExtension(AnnotationsExtensions.ColumnName));
            Assert.Equal("date accessed", descriptor.NestedType[0].Field[0].Options.GetExtension(AnnotationsExtensions.ColumnName));
            Assert.Equal(FieldDescriptorProto.Types.Label.Repeated, descriptor.Field[1].Label);
            Assert.Equal(FieldDescriptorProto.Types.Type.Message, descriptor.Field[0].Type);
            ProtoRows rows = encoder.Encode(batch);
            using var input = rows.SerializedRows[0].CreateCodedInput();
            Assert.Equal(10U, input.ReadTag());
            using var child = input.ReadBytes().CreateCodedInput();
            Assert.Equal(8U, child.ReadTag());
            Assert.Equal(7, child.ReadInt64());
            Assert.True(child.IsAtEnd);
            Assert.Equal(16U, input.ReadTag());
            Assert.Equal(10, input.ReadInt64());
            Assert.Equal(16U, input.ReadTag());
            Assert.Equal(11, input.ReadInt64());
            Assert.True(input.IsAtEnd);
            Assert.Equal(ByteString.Empty, rows.SerializedRows[1]);
        }

        [Fact]
        public void RejectsNullListElementsRatherThanSilentlyDroppingThem()
        {
            var builder = new ListArray.Builder(Int64Type.Default);
            builder.Append();
            ((Int64Array.Builder)builder.ValueBuilder).Append(1).AppendNull().Append(2);
            using RecordBatch batch = Batch(builder.Build());
            AdbcException error = Assert.Throws<AdbcException>(() => new ProtobufEncoder(batch.Schema).Encode(batch));
            Assert.Equal(AdbcStatusCode.InvalidArgument, error.Status);
            Assert.Contains("NULL elements", error.Message);
        }

        [Fact]
        public void EncodesListsInsideNestedStructs()
        {
            var list = new ListArray.Builder(Int64Type.Default);
            list.Append();
            ((Int64Array.Builder)list.ValueBuilder).Append(0).Append(-1);
            ListArray values = list.Build();
            var innerType = new StructType(new[] { new Field("list field", values.Data.DataType, true) });
            var inner = new StructArray(innerType, 1, new IArrowArray[] { values }, ArrowBuffer.Empty);
            var outerType = new StructType(new[] { new Field("inner field", innerType, true) });
            using RecordBatch batch = Batch(new StructArray(outerType, 1, new IArrowArray[] { inner }, ArrowBuffer.Empty));
            var encoder = new ProtobufEncoder(batch.Schema);
            DescriptorProto nested = encoder.WriterSchema.ProtoDescriptor.NestedType[0].NestedType[0];
            Assert.Equal("list field", nested.Field[0].Options.GetExtension(AnnotationsExtensions.ColumnName));
            using var input = encoder.Encode(batch).SerializedRows[0].CreateCodedInput();
            Assert.Equal(10U, input.ReadTag());
            using var outer = input.ReadBytes().CreateCodedInput();
            Assert.Equal(10U, outer.ReadTag());
            using var child = outer.ReadBytes().CreateCodedInput();
            Assert.Equal(8U, child.ReadTag());
            Assert.Equal(0, child.ReadInt64());
            Assert.Equal(8U, child.ReadTag());
            Assert.Equal(-1, child.ReadInt64());
            Assert.True(child.IsAtEnd);
            Assert.True(outer.IsAtEnd);
            Assert.True(input.IsAtEnd);
        }

        [Theory]
        [InlineData(1, 1, 1, 0, 0, 0, 0)]
        [InlineData(9999, 12, 31, 23, 59, 59, 999999)]
        public void PreservesCivilDateTimeBoundaries(int year, int month, int day, int hour, int minute, int second, int fraction)
        {
            var value = new DateTimeOffset(year, month, day, hour, minute, second, TimeSpan.Zero).AddTicks(fraction * 10L);
            using RecordBatch batch = Batch(new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string?)null)).Append(value).Build());
            using var input = new ProtobufEncoder(batch.Schema).Encode(batch).SerializedRows[0].CreateCodedInput();
            Assert.Equal(8U, input.ReadTag());
            Assert.Equal(((long)year << 46) | ((long)month << 42) | ((long)day << 37) | ((long)hour << 32)
                | ((long)minute << 26) | ((long)second << 20) | (long)fraction, input.ReadInt64());
            Assert.True(input.IsAtEnd);
        }

        [Theory]
        [InlineData(TimeUnit.Microsecond, -1L)]
        [InlineData(TimeUnit.Microsecond, 86_400_000_000L)]
        [InlineData(TimeUnit.Nanosecond, -1L)]
        [InlineData(TimeUnit.Nanosecond, 86_400_000_000_000L)]
        public void RejectsTimeOutsideSingleDay(TimeUnit unit, long value)
        {
            using RecordBatch batch = Batch(new Time64Array.Builder(unit).Append(value).Build());
            AdbcException error = Assert.Throws<AdbcException>(() => new ProtobufEncoder(batch.Schema).Encode(batch));
            Assert.Equal(AdbcStatusCode.InvalidArgument, error.Status);
        }

        [Fact]
        public void RejectsOutOfRangeUnsignedIntegers()
        {
            using RecordBatch valid = Batch(new UInt64Array.Builder().Append(long.MaxValue).Build());
            using var input = new ProtobufEncoder(valid.Schema).Encode(valid).SerializedRows[0].CreateCodedInput();
            Assert.Equal(8U, input.ReadTag());
            Assert.Equal(long.MaxValue, input.ReadInt64());
            using RecordBatch invalid = Batch(new UInt64Array.Builder().Append(ulong.MaxValue).Build());
            AdbcException error = Assert.Throws<AdbcException>(() => new ProtobufEncoder(invalid.Schema).Encode(invalid));
            Assert.Equal(AdbcStatusCode.InvalidArgument, error.Status);
        }

        [Fact]
        public void RejectsUnsupportedTypesAndNestedListsBeforeEncoding()
        {
            foreach (IArrowType type in new IArrowType[] { NullType.Default, new ListType(new ListType(Int64Type.Default)) })
            {
                var schema = new Schema(new[] { new Field("unsupported", type, true) }, null);
                AdbcException error = Assert.Throws<AdbcException>(() => new ProtobufEncoder(schema));
                Assert.Equal(AdbcStatusCode.NotImplemented, error.Status);
            }
        }

        [Fact]
        public void ObservesCancellationWhileEncodingRows()
        {
            using RecordBatch batch = Batch(new Int64Array.Builder().Append(1).Build());
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            Assert.ThrowsAny<OperationCanceledException>(() => new ProtobufEncoder(batch.Schema).Encode(batch, cancellation.Token));
        }

        private static RecordBatch Batch(params IArrowArray[] arrays)
        {
            var schema = new Schema(arrays.Select((a, i) => new Field($"column {i}", a.Data.DataType, true)), null);
            return new RecordBatch(schema, arrays, arrays[0].Length);
        }
    }
}
