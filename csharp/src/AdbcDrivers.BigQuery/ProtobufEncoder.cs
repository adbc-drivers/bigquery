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
using System.IO;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace AdbcDrivers.BigQuery
{
    internal sealed class ProtobufEncoder
    {
        public ProtoSchema WriterSchema { get; }

        public ProtobufEncoder(Schema schema)
        {
            WriterSchema = new ProtoSchema { ProtoDescriptor = CreateDescriptor("Row", schema.FieldsList) };
        }

        private static DescriptorProto CreateDescriptor(string name, IReadOnlyList<Field> fields)
        {
            var descriptor = new DescriptorProto { Name = name };
            for (int i = 0; i < fields.Count; i++)
            {
                Field field = fields[i];
                IArrowType type = field.DataType;
                bool repeated = type is ListType;
                if (type is ListType list)
                    type = list.ValueDataType;

                var protoField = new FieldDescriptorProto
                {
                    Name = $"field_{i}",
                    Number = i < 18999 ? i + 1 : i + 1001, // Protobuf reserves 19000-19999.
                    Label = repeated ? FieldDescriptorProto.Types.Label.Repeated : FieldDescriptorProto.Types.Label.Optional,
                    Options = new FieldOptions(),
                };
                protoField.Options.SetExtension(AnnotationsExtensions.ColumnName, field.Name);
                if (type is StructType record)
                {
                    var nested = CreateDescriptor($"Record_{i}", record.Fields);
                    descriptor.NestedType.Add(nested);
                    protoField.Type = FieldDescriptorProto.Types.Type.Message;
                    protoField.TypeName = nested.Name;
                }
                else
                {
                    protoField.Type = GetProtoType(type);
                }
                descriptor.Field.Add(protoField);
            }
            return descriptor;
        }

        private static FieldDescriptorProto.Types.Type GetProtoType(IArrowType type) => type switch
        {
            Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type or UInt64Type
                or Time32Type or Time64Type or TimestampType => FieldDescriptorProto.Types.Type.Int64,
            Date32Type or Date64Type => FieldDescriptorProto.Types.Type.Int32,
            BooleanType => FieldDescriptorProto.Types.Type.Bool,
            FloatType => FieldDescriptorProto.Types.Type.Float,
            DoubleType => FieldDescriptorProto.Types.Type.Double,
            StringType or Decimal128Type or Decimal256Type => FieldDescriptorProto.Types.Type.String,
            BinaryType or FixedSizeBinaryType => FieldDescriptorProto.Types.Type.Bytes,
            _ => throw new AdbcException($"Unsupported Arrow type for BigQuery Protobuf ingest: {type}", AdbcStatusCode.NotImplemented),
        };

        public ProtoRows Encode(RecordBatch batch, CancellationToken cancellationToken = default)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < arrays.Length; i++)
                arrays[i] = batch.Column(i);
            using var writer = new MessageWriter(arrays, WriterSchema.ProtoDescriptor);
            var rows = new ProtoRows();
            for (int row = 0; row < batch.Length; row++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                rows.SerializedRows.Add(writer.EncodeRow(row));
            }
            return rows;
        }

        private sealed class MessageWriter : IDisposable
        {
            private readonly MemoryStream _buffer = new();
            private readonly CodedOutputStream _output;
            private readonly Action<CodedOutputStream, int>[] _columns;
            private readonly List<MessageWriter> _children = new();

            public MessageWriter(IReadOnlyList<IArrowArray> arrays, DescriptorProto descriptor)
            {
                _output = new CodedOutputStream(_buffer, leaveOpen: true);
                _columns = new Action<CodedOutputStream, int>[arrays.Count];
                try
                {
                    for (int i = 0; i < arrays.Count; i++)
                        _columns[i] = CreateFieldWriter(arrays[i], descriptor.Field[i], descriptor);
                }
                catch
                {
                    Dispose();
                    throw;
                }
            }

            public ByteString EncodeRow(int row)
            {
                _buffer.Position = 0;
                _buffer.SetLength(0);
                foreach (var column in _columns)
                    column(_output, row);
                _output.Flush();
                return ByteString.CopyFrom(_buffer.GetBuffer(), 0, checked((int)_buffer.Length));
            }

            private Action<CodedOutputStream, int> CreateFieldWriter(IArrowArray array, FieldDescriptorProto field, DescriptorProto parent)
            {
                if (array is ListArray list)
                {
                    Action<CodedOutputStream, int> element = CreateValueWriter(list.Values, field, parent);
                    return (output, row) =>
                    {
                        if (list.IsNull(row)) return;
                        for (int i = list.ValueOffsets[row]; i < list.ValueOffsets[row + 1]; i++)
                        {
                            if (list.Values.IsNull(i))
                                throw new AdbcException("BigQuery arrays cannot contain NULL elements.", AdbcStatusCode.InvalidArgument);
                            element(output, i);
                        }
                    };
                }

                Action<CodedOutputStream, int> value = CreateValueWriter(array, field, parent);
                return (output, row) =>
                {
                    // Omission represents NULL, but non-null defaults must be encoded explicitly.
                    if (!array.IsNull(row))
                        value(output, row);
                };
            }

            private Action<CodedOutputStream, int> CreateValueWriter(IArrowArray array, FieldDescriptorProto field, DescriptorProto parent)
            {
                Action<CodedOutputStream, int> writeValue;
                if (array is StructArray record)
                {
                    DescriptorProto nested = parent.NestedType[GetNestedIndex(parent, field.TypeName)];
                    var child = new MessageWriter(record.Fields, nested);
                    _children.Add(child);
                    writeValue = (w, i) => w.WriteBytes(child.EncodeRow(i));
                }
                else
                {
                    writeValue = array switch
                    {
                        Int8Array a => (w, i) => w.WriteInt64(a.GetValue(i)!.Value),
                        Int16Array a => (w, i) => w.WriteInt64(a.GetValue(i)!.Value),
                        Int32Array a => (w, i) => w.WriteInt64(a.GetValue(i)!.Value),
                        Int64Array a => (w, i) => w.WriteInt64(a.GetValue(i)!.Value),
                        UInt8Array a => (w, i) => w.WriteInt64(a.GetValue(i)!.Value),
                        UInt16Array a => (w, i) => w.WriteInt64(a.GetValue(i)!.Value),
                        UInt32Array a => (w, i) => w.WriteInt64(a.GetValue(i)!.Value),
                        UInt64Array a => (w, i) => w.WriteInt64(ToInt64(a.GetValue(i)!.Value)),
                        BooleanArray a => (w, i) => w.WriteBool(a.GetValue(i)!.Value),
                        FloatArray a => (w, i) => w.WriteFloat(a.GetValue(i)!.Value),
                        DoubleArray a => (w, i) => w.WriteDouble(a.GetValue(i)!.Value),
                        StringArray a => (w, i) => w.WriteString(a.GetString(i)),
                        BinaryArray a => (w, i) => w.WriteBytes(ByteString.CopyFrom(a.GetBytes(i))),
                        Decimal128Array a => (w, i) => w.WriteString(a.GetString(i)),
                        Decimal256Array a => (w, i) => w.WriteString(a.GetString(i)),
                        FixedSizeBinaryArray a => (w, i) => w.WriteBytes(ByteString.CopyFrom(a.GetBytes(i))),
                        Date32Array a => (w, i) => w.WriteInt32(a.GetValue(i)!.Value),
                        Date64Array a => (w, i) => w.WriteInt32(Date64Days(a.GetValue(i)!.Value)),
                        Time32Array a => (w, i) => w.WriteInt64(PackTime(a.GetValue(i)!.Value, ((Time32Type)a.Data.DataType).Unit)),
                        Time64Array a => (w, i) => w.WriteInt64(PackTime(a.GetValue(i)!.Value, ((Time64Type)a.Data.DataType).Unit)),
                        TimestampArray a when string.IsNullOrEmpty(((TimestampType)a.Data.DataType).Timezone)
                            => (w, i) => w.WriteInt64(PackDateTime(ToMicroseconds(a.Values[i], ((TimestampType)a.Data.DataType).Unit))),
                        TimestampArray a => (w, i) => w.WriteInt64(ToMicroseconds(a.Values[i], ((TimestampType)a.Data.DataType).Unit)),
                        _ => throw new AdbcException($"Unsupported Arrow array for BigQuery Protobuf ingest: {array.GetType().Name}", AdbcStatusCode.NotImplemented),
                    };
                }
                WireFormat.WireType wireType = field.Type switch
                {
                    FieldDescriptorProto.Types.Type.Double => WireFormat.WireType.Fixed64,
                    FieldDescriptorProto.Types.Type.Float => WireFormat.WireType.Fixed32,
                    FieldDescriptorProto.Types.Type.String or FieldDescriptorProto.Types.Type.Bytes
                        or FieldDescriptorProto.Types.Type.Message => WireFormat.WireType.LengthDelimited,
                    _ => WireFormat.WireType.Varint,
                };
                uint tag = WireFormat.MakeTag(field.Number, wireType);
                return (output, row) =>
                {
                    output.WriteTag(tag);
                    writeValue(output, row);
                };
            }

            private static int GetNestedIndex(DescriptorProto parent, string name)
            {
                for (int i = 0; i < parent.NestedType.Count; i++)
                    if (parent.NestedType[i].Name == name) return i;
                throw new InvalidOperationException($"Missing nested Protobuf descriptor {name}.");
            }

            public void Dispose()
            {
                foreach (var child in _children)
                    child.Dispose();
                _output.Dispose();
                _buffer.Dispose();
            }
        }

        private static long ToInt64(ulong value)
        {
            if (value > long.MaxValue)
                throw new AdbcException("UInt64 value exceeds the BigQuery INT64 range.", AdbcStatusCode.InvalidArgument);
            return (long)value;
        }

        private static int Date64Days(long milliseconds)
        {
            if (milliseconds % 86_400_000 != 0)
                throw new AdbcException("Arrow Date64 values must represent whole days.", AdbcStatusCode.InvalidArgument);
            return checked((int)(milliseconds / 86_400_000));
        }

        private static long ToMicroseconds(long value, TimeUnit unit) => unit switch
        {
            TimeUnit.Second => checked(value * 1_000_000),
            TimeUnit.Millisecond => checked(value * 1_000),
            TimeUnit.Microsecond => value,
            TimeUnit.Nanosecond => value / 1_000,
            _ => throw new AdbcException($"Unsupported temporal unit: {unit}", AdbcStatusCode.NotImplemented),
        };

        // Storage Write uses packed civil components for TIME/DATETIME, not epoch microseconds.
        private static long PackTime(long value, TimeUnit unit = TimeUnit.Microsecond)
        {
            if (value < 0)
                throw new AdbcException("TIME value must be within a single day.", AdbcStatusCode.InvalidArgument);
            long microseconds = ToMicroseconds(value, unit);
            if (microseconds >= 86_400_000_000L)
                throw new AdbcException("TIME value must be within a single day.", AdbcStatusCode.InvalidArgument);
            return ((microseconds / 3_600_000_000L) << 32)
                | ((microseconds / 60_000_000L % 60) << 26)
                | ((microseconds / 1_000_000L % 60) << 20)
                | (microseconds % 1_000_000L);
        }

        private static long PackDateTime(long microseconds)
        {
            var value = new DateTime(checked(621355968000000000L + microseconds * 10));
            return ((long)value.Year << 46) | ((long)value.Month << 42) | ((long)value.Day << 37)
                | PackTime(value.TimeOfDay.Ticks / 10);
        }
    }
}
