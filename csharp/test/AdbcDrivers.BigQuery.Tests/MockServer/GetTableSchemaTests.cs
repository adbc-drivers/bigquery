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

#if NET8_0_OR_GREATER

using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using AdbcDrivers.BigQuery.MockServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests.MockServer
{
    /// <summary>
    /// GetTableSchema builds its Arrow types from the BigQuery tables.get REST API. The Storage
    /// Read API returns TIMESTAMP and DATETIME values in microseconds, so these types must match
    /// the ones BigQueryStatement declares for query results.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class GetTableSchemaTests
    {
        private const string ProjectId = "mock-project";
        private const string DbSchema = "mock_dataset";

        [Fact]
        public void TimestampColumnIsMicrosecondUtc()
        {
            Schema schema = GetTableSchema(("ts", "TIMESTAMP"));

            TimestampType type = Assert.IsType<TimestampType>(schema.GetFieldByName("ts").DataType);
            Assert.Equal(TimeUnit.Microsecond, type.Unit);
            Assert.Equal("UTC", type.Timezone);
        }

        [Fact]
        public void DatetimeColumnIsMicrosecondWithoutTimezone()
        {
            Schema schema = GetTableSchema(("dt", "DATETIME"));

            TimestampType type = Assert.IsType<TimestampType>(schema.GetFieldByName("dt").DataType);
            Assert.Equal(TimeUnit.Microsecond, type.Unit);
            Assert.Null(type.Timezone);
        }

        [Fact]
        public void StructFieldsUseMicrosecondTimestamps()
        {
            Schema schema = GetTableSchemaFromFields(new TableFieldSchema
            {
                Name = "rec",
                Type = "RECORD",
                Mode = "NULLABLE",
                Fields = new[]
                {
                    new TableFieldSchema { Name = "ts", Type = "TIMESTAMP", Mode = "NULLABLE" },
                    new TableFieldSchema { Name = "dt", Type = "DATETIME", Mode = "REQUIRED" },
                    new TableFieldSchema
                    {
                        Name = "nested",
                        Type = "RECORD",
                        Mode = "REQUIRED",
                        Fields = new[]
                        {
                            new TableFieldSchema { Name = "inner_ts", Type = "TIMESTAMP", Mode = "REQUIRED" },
                            new TableFieldSchema { Name = "inner_dt", Type = "DATETIME", Mode = "NULLABLE" },
                        },
                    },
                },
            });

            Field recField = schema.GetFieldByName("rec");
            Assert.True(recField.IsNullable);
            StructType structType = Assert.IsType<StructType>(recField.DataType);

            Field tsField = structType.GetFieldByName("ts");
            Assert.True(tsField.IsNullable);
            TimestampType ts = Assert.IsType<TimestampType>(tsField.DataType);
            Assert.Equal(TimeUnit.Microsecond, ts.Unit);
            Assert.Equal("UTC", ts.Timezone);

            Field dtField = structType.GetFieldByName("dt");
            Assert.False(dtField.IsNullable);
            TimestampType dt = Assert.IsType<TimestampType>(dtField.DataType);
            Assert.Equal(TimeUnit.Microsecond, dt.Unit);
            Assert.Null(dt.Timezone);

            Field nestedField = structType.GetFieldByName("nested");
            Assert.False(nestedField.IsNullable);
            StructType nestedStruct = Assert.IsType<StructType>(nestedField.DataType);

            Field innerTsField = nestedStruct.GetFieldByName("inner_ts");
            Assert.False(innerTsField.IsNullable);
            TimestampType innerTs = Assert.IsType<TimestampType>(innerTsField.DataType);
            Assert.Equal(TimeUnit.Microsecond, innerTs.Unit);
            Assert.Equal("UTC", innerTs.Timezone);

            Field innerDtField = nestedStruct.GetFieldByName("inner_dt");
            Assert.True(innerDtField.IsNullable);
            TimestampType innerDt = Assert.IsType<TimestampType>(innerDtField.DataType);
            Assert.Equal(TimeUnit.Microsecond, innerDt.Unit);
            Assert.Null(innerDt.Timezone);
        }

        [Fact]
        public void RepeatedFieldsAreMappedAsNonNullableLists()
        {
            Schema schema = GetTableSchemaFromFields(
                new TableFieldSchema { Name = "int_array", Type = "INTEGER", Mode = "REPEATED" },
                new TableFieldSchema
                {
                    Name = "dec_array",
                    Type = "NUMERIC",
                    Mode = "REPEATED",
                    Precision = 10,
                    Scale = 2,
                },
                new TableFieldSchema
                {
                    Name = "rec_array",
                    Type = "RECORD",
                    Mode = "REPEATED",
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
                        new TableFieldSchema { Name = "tags", Type = "STRING", Mode = "REPEATED" },
                    },
                });

            Field intArrayField = schema.GetFieldByName("int_array");
            Assert.False(intArrayField.IsNullable);
            ListType intListType = Assert.IsType<ListType>(intArrayField.DataType);
            Assert.IsType<Int64Type>(intListType.ValueDataType);

            Field decArrayField = schema.GetFieldByName("dec_array");
            Assert.False(decArrayField.IsNullable);
            ListType decListType = Assert.IsType<ListType>(decArrayField.DataType);
            Decimal128Type decElement = Assert.IsType<Decimal128Type>(decListType.ValueDataType);
            Assert.Equal(10, decElement.Precision);
            Assert.Equal(2, decElement.Scale);

            Field recArrayField = schema.GetFieldByName("rec_array");
            Assert.False(recArrayField.IsNullable);
            ListType recListType = Assert.IsType<ListType>(recArrayField.DataType);
            StructType recElement = Assert.IsType<StructType>(recListType.ValueDataType);

            Field idField = recElement.GetFieldByName("id");
            Assert.False(idField.IsNullable);
            Assert.IsType<Int64Type>(idField.DataType);

            Field tagsField = recElement.GetFieldByName("tags");
            Assert.False(tagsField.IsNullable);
            ListType tagsListType = Assert.IsType<ListType>(tagsField.DataType);
            Assert.IsType<StringType>(tagsListType.ValueDataType);
        }

        [Fact]
        public void NumericAndBigNumericPrecisionAndScaleDefaults()
        {
            Schema schema = GetTableSchemaFromFields(
                new TableFieldSchema { Name = "num_default", Type = "NUMERIC", Mode = "NULLABLE" },
                new TableFieldSchema { Name = "num_prec_only", Type = "NUMERIC", Mode = "NULLABLE", Precision = 12 },
                new TableFieldSchema { Name = "num_small_prec", Type = "NUMERIC", Mode = "NULLABLE", Precision = 5 },
                new TableFieldSchema { Name = "num_param", Type = "NUMERIC", Mode = "NULLABLE", Precision = 10, Scale = 2 },
                new TableFieldSchema { Name = "bignum_default", Type = "BIGNUMERIC", Mode = "NULLABLE" },
                new TableFieldSchema { Name = "bignum_prec_only", Type = "BIGNUMERIC", Mode = "NULLABLE", Precision = 20 },
                new TableFieldSchema { Name = "bignum_param", Type = "BIGNUMERIC", Mode = "NULLABLE", Precision = 50, Scale = 10 });

            Decimal128Type numDefault = Assert.IsType<Decimal128Type>(schema.GetFieldByName("num_default").DataType);
            Assert.Equal(38, numDefault.Precision);
            Assert.Equal(9, numDefault.Scale);

            Decimal128Type numPrecOnly = Assert.IsType<Decimal128Type>(schema.GetFieldByName("num_prec_only").DataType);
            Assert.Equal(12, numPrecOnly.Precision);
            Assert.Equal(0, numPrecOnly.Scale);

            Decimal128Type numSmallPrec = Assert.IsType<Decimal128Type>(schema.GetFieldByName("num_small_prec").DataType);
            Assert.Equal(5, numSmallPrec.Precision);
            Assert.Equal(0, numSmallPrec.Scale);

            Decimal128Type numParam = Assert.IsType<Decimal128Type>(schema.GetFieldByName("num_param").DataType);
            Assert.Equal(10, numParam.Precision);
            Assert.Equal(2, numParam.Scale);

            Decimal256Type bigNumDefault = Assert.IsType<Decimal256Type>(schema.GetFieldByName("bignum_default").DataType);
            Assert.Equal(76, bigNumDefault.Precision);
            Assert.Equal(38, bigNumDefault.Scale);

            Decimal256Type bigNumPrecOnly = Assert.IsType<Decimal256Type>(schema.GetFieldByName("bignum_prec_only").DataType);
            Assert.Equal(20, bigNumPrecOnly.Precision);
            Assert.Equal(0, bigNumPrecOnly.Scale);

            Decimal256Type bigNumParam = Assert.IsType<Decimal256Type>(schema.GetFieldByName("bignum_param").DataType);
            Assert.Equal(50, bigNumParam.Precision);
            Assert.Equal(10, bigNumParam.Scale);
        }

        [Fact]
        public void UnauthorizedErrorMapsToUnauthorizedStatus()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.QueueError(MockRequestKind.TableGet, HttpStatusCode.Unauthorized, "authError");
            mockServer.QueueError(MockRequestKind.TableGet, HttpStatusCode.Unauthorized, "authError");

            using AdbcConnection connection = Connect(mockServer, new Dictionary<string, string>
            {
                { BigQueryParameters.MaximumRetryAttempts, "1" },
                { BigQueryParameters.RetryDelayMs, "1" },
            });

            AdbcException ex = Assert.Throws<AdbcException>(
                () => connection.GetTableSchema(ProjectId, DbSchema, "my_table"));

            Assert.Equal(AdbcStatusCode.Unauthorized, ex.Status);
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.TableGet));
        }

        [Fact]
        public void UnauthorizedErrorRefreshesTokenAndRetries()
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Google.Apis.Bigquery.v2.Data.Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "auth_table",
                },
                Schema = new TableSchema
                {
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
                    },
                },
            });

            mockServer.QueueError(MockRequestKind.TableGet, HttpStatusCode.Unauthorized, "authError");

            using AdbcConnection connection = Connect(mockServer, new Dictionary<string, string>
            {
                { BigQueryParameters.RetryDelayMs, "1" },
            });

            bool tokenRefreshed = false;
            ((BigQueryConnection)connection).UpdateToken = () =>
            {
                tokenRefreshed = true;
                ((BigQueryConnection)connection).UpdateClientToken();
                return Task.CompletedTask;
            };

            Schema schema = connection.GetTableSchema(ProjectId, DbSchema, "auth_table");

            Assert.True(tokenRefreshed);
            Assert.Single(schema.FieldsList);
            Assert.Equal(2, mockServer.CountOfKind(MockRequestKind.TableGet));
        }

        [Fact]
        public void MissingTableReturnsEmptySchema()
        {
            using var mockServer = new BigQueryMockServer();
            using AdbcConnection connection = Connect(mockServer);

            Schema schema = connection.GetTableSchema(ProjectId, DbSchema, "nonexistent_table");

            Assert.Empty(schema.FieldsList);
            Assert.Single(mockServer.RequestsOfKind(MockRequestKind.TableGet));
        }

        [Fact]
        public void PcntDatasetTableSchemaSucceeds()
        {
            const string pcntDataset = "my_catalog.my_namespace";
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Google.Apis.Bigquery.v2.Data.Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = pcntDataset,
                    TableId = "my_table",
                },
                Schema = new TableSchema
                {
                    Fields = new[]
                    {
                        new TableFieldSchema { Name = "id", Type = "INTEGER", Mode = "REQUIRED" },
                        new TableFieldSchema { Name = "name", Type = "STRING", Mode = "NULLABLE" },
                    },
                },
            });

            using AdbcConnection connection = Connect(mockServer);
            Schema schema = connection.GetTableSchema(ProjectId, pcntDataset, "my_table");

            Assert.Equal(2, schema.FieldsList.Count);
            Assert.Equal(Int64Type.Default.TypeId, schema.GetFieldByName("id").DataType.TypeId);
            Assert.False(schema.GetFieldByName("id").IsNullable);
            Assert.Equal(StringType.Default.TypeId, schema.GetFieldByName("name").DataType.TypeId);
            Assert.True(schema.GetFieldByName("name").IsNullable);
            Assert.Empty(mockServer.ExecutedQueries);
        }

        private static Schema GetTableSchema(params (string Name, string DataType)[] columns)
        {
            var fields = new TableFieldSchema[columns.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                fields[i] = new TableFieldSchema
                {
                    Name = columns[i].Name,
                    Type = columns[i].DataType,
                    Mode = "NULLABLE",
                };
            }

            return GetTableSchemaFromFields(fields);
        }

        private static Schema GetTableSchemaFromFields(params TableFieldSchema[] fields)
        {
            using var mockServer = new BigQueryMockServer();
            mockServer.AddTable(new Google.Apis.Bigquery.v2.Data.Table
            {
                TableReference = new TableReference
                {
                    ProjectId = ProjectId,
                    DatasetId = DbSchema,
                    TableId = "my_table",
                },
                Schema = new TableSchema { Fields = fields },
            });

            using AdbcConnection connection = Connect(mockServer);
            Schema schema = connection.GetTableSchema(ProjectId, DbSchema, "my_table");

            Assert.Equal(fields.Length, schema.FieldsList.Count);
            return schema;
        }

        private static AdbcConnection Connect(BigQueryMockServer mockServer, IDictionary<string, string>? extraParameters = null)
        {
            var parameters = new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, ProjectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            };

            if (extraParameters != null)
            {
                foreach (var kvp in extraParameters)
                {
                    parameters[kvp.Key] = kvp.Value;
                }
            }

            var driver = new BigQueryDriver();
            AdbcDatabase database = driver.Open(parameters);

            return database.Connect(new Dictionary<string, string>());
        }
    }
}

#endif
