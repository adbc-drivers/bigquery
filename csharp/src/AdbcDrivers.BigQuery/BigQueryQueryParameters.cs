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

namespace AdbcDrivers.BigQuery
{
    internal sealed class BigQueryQueryParameters
    {
        private readonly List<(BigQueryDbType Type, string? Value)> parameters = new();

        public IEnumerable<BigQueryParameter> Parameters =>
            parameters.Select(parameter => new BigQueryParameter { Type = parameter.Type, Value = parameter.Value });

        internal static BigQueryQueryParameters FromBatch(RecordBatch batch)
        {
            if (batch.Length != 1)
            {
                throw new AdbcException("Query parameter bindings must contain exactly one row.", AdbcStatusCode.InvalidArgument);
            }

            var result = new BigQueryQueryParameters();
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                IArrowArray array = batch.Column(i);
                BigQueryDbType type = GetParameterType(array.Data.DataType);
                string? value = array.IsNull(0) ? null : GetParameterValue(array);
                result.parameters.Add((type, value));
            }
            return result;
        }

        internal static async Task<BigQueryQueryParameters> FromStreamAsync(IArrowArrayStream stream, CancellationToken cancellationToken)
        {
            BigQueryQueryParameters? result = null;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                using RecordBatch? batch = await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
                if (batch == null)
                {
                    return result ?? throw new AdbcException("Query parameter bindings must contain exactly one row.", AdbcStatusCode.InvalidArgument);
                }
                if (batch.Length == 0)
                {
                    continue;
                }
                if (result != null)
                {
                    throw new AdbcException("Query parameter bindings must contain exactly one row.", AdbcStatusCode.InvalidArgument);
                }
                result = FromBatch(batch);
            }
        }

        internal void ApplyTo(QueryOptions options)
        {
            if (options.UseLegacySql == true)
            {
                throw new AdbcException("Query parameters require GoogleSQL, not legacy SQL.", AdbcStatusCode.InvalidArgument);
            }
            options.UseLegacySql = false;
            options.ParameterMode = BigQueryParameterMode.Positional;
        }

        internal void ApplyTo(QueryRequest request)
        {
            request.UseLegacySql = false;
            request.ParameterMode = "POSITIONAL";
            request.QueryParameters = parameters.Select(parameter => new QueryParameter
            {
                ParameterType = new QueryParameterType { Type = parameter.Type.ToString().ToUpperInvariant() },
                ParameterValue = new QueryParameterValue { Value = parameter.Value }
            }).ToList();
        }

        private static BigQueryDbType GetParameterType(IArrowType type)
        {
            return type.TypeId switch
            {
                ArrowTypeId.Boolean => BigQueryDbType.Bool,
                ArrowTypeId.Int8 or ArrowTypeId.Int16 or ArrowTypeId.Int32 or ArrowTypeId.Int64 or
                    ArrowTypeId.UInt8 or ArrowTypeId.UInt16 or ArrowTypeId.UInt32 or ArrowTypeId.UInt64 => BigQueryDbType.Int64,
                ArrowTypeId.Float or ArrowTypeId.Double => BigQueryDbType.Float64,
                ArrowTypeId.String => BigQueryDbType.String,
                ArrowTypeId.Binary => BigQueryDbType.Bytes,
                ArrowTypeId.Date32 or ArrowTypeId.Date64 => BigQueryDbType.Date,
                ArrowTypeId.Time32 or ArrowTypeId.Time64 => BigQueryDbType.Time,
                ArrowTypeId.Timestamp => string.IsNullOrEmpty(((TimestampType)type).Timezone) ? BigQueryDbType.DateTime : BigQueryDbType.Timestamp,
                ArrowTypeId.Extension when type is TimestampWithOffsetType => BigQueryDbType.Timestamp,
                ArrowTypeId.Decimal128 => DecimalType(((Decimal128Type)type).Precision, ((Decimal128Type)type).Scale),
                ArrowTypeId.Decimal256 => DecimalType(((Decimal256Type)type).Precision, ((Decimal256Type)type).Scale),
                _ => throw new AdbcException($"Query parameter type {type.TypeId} is not supported.", AdbcStatusCode.NotImplemented)
            };
        }

        private static BigQueryDbType DecimalType(int precision, int scale)
        {
            if (scale >= 0 && scale <= 9 && precision - scale <= 29)
            {
                return BigQueryDbType.Numeric;
            }
            if (scale >= 0 && scale <= 38 && precision - scale <= 38)
            {
                return BigQueryDbType.BigNumeric;
            }
            throw new AdbcException("Decimal parameter precision or scale exceeds BigQuery's supported range.", AdbcStatusCode.InvalidArgument);
        }

        private static string? GetParameterValue(IArrowArray array)
        {
            IFormatProvider culture = CultureInfo.InvariantCulture;
            return array switch
            {
                BooleanArray a => a.GetValue(0)!.Value ? "TRUE" : "FALSE",
                Int8Array a => a.GetValue(0)!.Value.ToString(culture),
                Int16Array a => a.GetValue(0)!.Value.ToString(culture),
                Int32Array a => a.GetValue(0)!.Value.ToString(culture),
                Int64Array a => a.GetValue(0)!.Value.ToString(culture),
                UInt8Array a => a.GetValue(0)!.Value.ToString(culture),
                UInt16Array a => a.GetValue(0)!.Value.ToString(culture),
                UInt32Array a => a.GetValue(0)!.Value.ToString(culture),
                UInt64Array a => a.GetValue(0)!.Value <= long.MaxValue ? a.GetValue(0)!.Value.ToString(culture) :
                    throw new AdbcException("Unsigned parameter exceeds BigQuery INT64 range.", AdbcStatusCode.InvalidArgument),
                FloatArray a => FloatValue(a.GetValue(0)!.Value),
                DoubleArray a => FloatValue(a.GetValue(0)!.Value),
                StringArray a => a.GetString(0),
                BinaryArray a => Convert.ToBase64String(a.GetBytes(0).ToArray()),
                Date32Array a => a.GetDateTime(0)!.Value.ToString("yyyy-MM-dd", culture),
                Date64Array a => a.GetDateTime(0)!.Value.ToString("yyyy-MM-dd", culture),
                Time32Array a => TimeValue(a.GetValue(0)!.Value, ((Time32Type)a.Data.DataType).Unit),
                Time64Array a => TimeValue(a.GetValue(0)!.Value, ((Time64Type)a.Data.DataType).Unit),
                TimestampArray a => TimestampValue(a),
                TimestampWithOffsetArray a => TimestampValue((TimestampArray)((StructArray)a.Storage).Fields[0]),
                Decimal128Array a => a.GetString(0),
                Decimal256Array a => a.GetString(0),
                _ => throw new AdbcException($"Query parameter type {array.Data.DataType.TypeId} is not supported.", AdbcStatusCode.NotImplemented)
            };
        }

        private static string FloatValue(float value) =>
            float.IsNaN(value) ? "NaN" :
            float.IsPositiveInfinity(value) ? "+inf" :
            float.IsNegativeInfinity(value) ? "-inf" :
            value.ToString("G9", CultureInfo.InvariantCulture);

        private static string FloatValue(double value) =>
            double.IsNaN(value) ? "NaN" :
            double.IsPositiveInfinity(value) ? "+inf" :
            double.IsNegativeInfinity(value) ? "-inf" :
            value.ToString("G17", CultureInfo.InvariantCulture);

        private static long Microseconds(long value, TimeUnit unit)
        {
            if (unit == TimeUnit.Nanosecond && value % 1000 != 0)
            {
                throw new AdbcException("BigQuery temporal parameters support microsecond precision.", AdbcStatusCode.InvalidArgument);
            }
            try
            {
                return unit switch
                {
                    TimeUnit.Second => checked(value * 1000000),
                    TimeUnit.Millisecond => checked(value * 1000),
                    TimeUnit.Microsecond => value,
                    TimeUnit.Nanosecond => value / 1000,
                    _ => throw new AdbcException("Unsupported temporal parameter unit.", AdbcStatusCode.InvalidArgument)
                };
            }
            catch (OverflowException exception)
            {
                throw new AdbcException("Temporal parameter exceeds BigQuery's supported range.", AdbcStatusCode.InvalidArgument, exception);
            }
        }

        private static string TimeValue(long value, TimeUnit unit)
        {
            long microseconds = Microseconds(value, unit);
            if (microseconds < 0 || microseconds >= 86400000000)
            {
                throw new AdbcException("TIME parameters must be within a single day.", AdbcStatusCode.InvalidArgument);
            }
            return new DateTime(microseconds * 10).ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
        }

        private static string TimestampValue(TimestampArray array)
        {
            var type = (TimestampType)array.Data.DataType;
            long microseconds = Microseconds(array.GetValue(0)!.Value, type.Unit);
            if (microseconds < -62135596800000000 || microseconds > 253402300799999999)
            {
                throw new AdbcException("Timestamp parameters must be within years 0001 through 9999.", AdbcStatusCode.InvalidArgument);
            }
            return array.GetTimestamp(0)!.Value.ToString(
                string.IsNullOrEmpty(type.Timezone) ? "yyyy-MM-dd HH:mm:ss.ffffff" : "yyyy-MM-dd HH:mm:ss.ffffffzzz",
                CultureInfo.InvariantCulture);
        }
    }
}
