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
using AdbcDrivers.BigQuery.MockServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Google.Apis.Bigquery.v2.Data;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests.MockServer
{
    /// <summary>
    /// GetTableSchema builds its Arrow types from INFORMATION_SCHEMA.COLUMNS. The Storage Read API
    /// returns TIMESTAMP and DATETIME values in microseconds, so these types must match the ones
    /// BigQueryStatement declares for query results.
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
            Schema schema = GetTableSchema(("rec", "STRUCT<ts TIMESTAMP, dt DATETIME>"));

            StructType structType = Assert.IsType<StructType>(schema.GetFieldByName("rec").DataType);

            TimestampType ts = Assert.IsType<TimestampType>(structType.GetFieldByName("ts").DataType);
            Assert.Equal(TimeUnit.Microsecond, ts.Unit);
            Assert.Equal("UTC", ts.Timezone);

            TimestampType dt = Assert.IsType<TimestampType>(structType.GetFieldByName("dt").DataType);
            Assert.Equal(TimeUnit.Microsecond, dt.Unit);
            Assert.Null(dt.Timezone);
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
            using var mockServer = new BigQueryMockServer();
            var fields = new List<TableFieldSchema>();
            for (int i = 0; i < columns.Length; i++)
            {
                fields.Add(new TableFieldSchema
                {
                    Name = columns[i].Name,
                    Type = columns[i].DataType,
                    Mode = "NULLABLE",
                });
            }

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

            Assert.Equal(columns.Length, schema.FieldsList.Count);
            return schema;
        }

        private static AdbcConnection Connect(BigQueryMockServer mockServer)
        {
            var driver = new BigQueryDriver();
            AdbcDatabase database = driver.Open(new Dictionary<string, string>
            {
                { BigQueryParameters.ProjectId, ProjectId },
                { BigQueryParameters.AuthenticationType, BigQueryConstants.MockAuthenticationType },
                { BigQueryParameters.TestRestEndpoint, mockServer.RestEndpoint },
                { BigQueryParameters.TestStorageEndpoint, mockServer.GrpcEndpoint },
            });

            return database.Connect(new Dictionary<string, string>());
        }
    }
}

#endif
