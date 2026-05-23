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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Cloud.BigQuery.V2;
using Google.Protobuf;
using ArrowRecordBatch = Google.Cloud.BigQuery.Storage.V1.ArrowRecordBatch;
using ArrowSchema = Google.Cloud.BigQuery.Storage.V1.ArrowSchema;
using TableFieldSchema = Google.Apis.Bigquery.v2.Data.TableFieldSchema;

namespace AdbcDrivers.BigQuery
{
    /// <summary>
    /// Implements bulk ingestion using the BigQuery Storage Write API with Arrow IPC format.
    /// Data flows columnar end-to-end: Arrow RecordBatch → IPC serialize → gRPC → BigQuery.
    /// Uses a PENDING write stream for atomic commit.
    /// </summary>
    internal sealed class BigQueryBulkIngest
    {
        private readonly BigQueryClient _restClient;
        private readonly BigQueryWriteClient _writeClient;
        private readonly string _projectId;

        public BigQueryBulkIngest(BigQueryClient restClient, BigQueryWriteClient writeClient, string projectId)
        {
            _restClient = restClient ?? throw new ArgumentNullException(nameof(restClient));
            _writeClient = writeClient ?? throw new ArgumentNullException(nameof(writeClient));
            _projectId = projectId ?? throw new ArgumentNullException(nameof(projectId));
        }

        /// <summary>
        /// Executes bulk ingestion of the bound data into the target table.
        /// </summary>
        public async Task<UpdateResult> ExecuteAsync(
            string? targetCatalog,
            string? targetDbSchema,
            string targetTable,
            BulkIngestMode mode,
            RecordBatch? boundBatch,
            IArrowArrayStream? boundStream,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(targetTable))
                throw new AdbcException("Target table name is required for bulk ingest", AdbcStatusCode.InvalidArgument);

            if (boundBatch == null && boundStream == null)
                throw new AdbcException("No data bound for bulk ingest. Call Bind() or BindStream() before ExecuteUpdate().", AdbcStatusCode.InvalidState);

            string catalog = targetCatalog ?? _projectId;
            if (string.IsNullOrEmpty(targetDbSchema))
                throw new AdbcException("Target dataset (schema) is required for bulk ingest", AdbcStatusCode.InvalidArgument);

            string tableReference = $"projects/{catalog}/datasets/{targetDbSchema}/tables/{targetTable}";

            // Determine the Arrow schema from the bound data
            Schema arrowSchema;
            if (boundBatch != null)
            {
                arrowSchema = boundBatch.Schema;
            }
            else
            {
                arrowSchema = boundStream!.Schema;
            }

            // Handle table lifecycle based on BulkIngestMode
            await HandleTableLifecycleAsync(catalog, targetDbSchema!, targetTable, mode, arrowSchema, cancellationToken).ConfigureAwait(false);

            // Create a PENDING write stream for atomic commit
            WriteStream writeStream = await _writeClient.CreateWriteStreamAsync(
                new CreateWriteStreamRequest
                {
                    Parent = tableReference,
                    WriteStream = new WriteStream { Type = WriteStream.Types.Type.Pending }
                },
                cancellationToken).ConfigureAwait(false);

            long totalRows = 0;

            try
            {
                // Open the bidirectional streaming call
                using BigQueryWriteClient.AppendRowsStream appendStream = _writeClient.AppendRows();

                // Serialize the Arrow schema (sent with first request)
                byte[] schemaBytes = SerializeSchema(arrowSchema);

                if (boundBatch != null)
                {
                    totalRows = await AppendBatchAsync(appendStream, writeStream.Name, schemaBytes, boundBatch, isFirst: true, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    bool isFirst = true;
                    while (true)
                    {
                        RecordBatch? batch = await boundStream!.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
                        if (batch == null) break;

                        totalRows += await AppendBatchAsync(appendStream, writeStream.Name, schemaBytes, batch, isFirst, cancellationToken).ConfigureAwait(false);
                        isFirst = false;
                    }
                }

                // Signal end of writes
                await appendStream.WriteCompleteAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not AdbcException)
            {
                throw new AdbcException($"Failed to append rows: {ex.Message}", AdbcStatusCode.IOError, ex);
            }

            // Finalize the write stream
            await _writeClient.FinalizeWriteStreamAsync(
                new FinalizeWriteStreamRequest { Name = writeStream.Name },
                cancellationToken).ConfigureAwait(false);

            // Atomically commit
            await _writeClient.BatchCommitWriteStreamsAsync(
                new BatchCommitWriteStreamsRequest
                {
                    Parent = tableReference,
                    WriteStreams = { writeStream.Name }
                },
                cancellationToken).ConfigureAwait(false);

            return new UpdateResult(totalRows);
        }

        private async Task<long> AppendBatchAsync(
            BigQueryWriteClient.AppendRowsStream appendStream,
            string writeStreamName,
            byte[] schemaBytes,
            RecordBatch batch,
            bool isFirst,
            CancellationToken cancellationToken)
        {
            // Normalize types BigQuery requires (microsecond precision)
            batch = NormalizeBatch(batch);

            byte[] batchBytes = SerializeRecordBatch(batch);

            var request = new AppendRowsRequest
            {
                WriteStream = writeStreamName,
                ArrowRows = new AppendRowsRequest.Types.ArrowData
                {
                    Rows = new ArrowRecordBatch
                    {
                        SerializedRecordBatch = ByteString.CopyFrom(batchBytes)
                    }
                }
            };

            // Include schema on first request
            if (isFirst)
            {
                request.ArrowRows.WriterSchema = new ArrowSchema
                {
                    SerializedSchema = ByteString.CopyFrom(schemaBytes)
                };
            }

            await appendStream.WriteAsync(request).ConfigureAwait(false);

            // Read and check response
            var responseStream = appendStream.GetResponseStream();
            if (await responseStream.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                var response = responseStream.Current;
                if (response.Error != null && response.Error.Code != 0)
                {
                    throw new AdbcException(
                        $"BigQuery append failed: {response.Error.Message}",
                        AdbcStatusCode.IOError);
                }
            }

            return batch.Length;
        }

        private async Task HandleTableLifecycleAsync(
            string projectId,
            string datasetId,
            string tableId,
            BulkIngestMode mode,
            Schema arrowSchema,
            CancellationToken cancellationToken)
        {
            var tableRef = _restClient.GetTableReference(projectId, datasetId, tableId);
            BigQueryTable? existingTable = null;
            bool tableExists;

            try
            {
                existingTable = await _restClient.GetTableAsync(tableRef, cancellationToken: cancellationToken).ConfigureAwait(false);
                tableExists = true;
            }
            catch (GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                tableExists = false;
            }

            switch (mode)
            {
                case BulkIngestMode.Create:
                    if (tableExists)
                        throw new AdbcException($"Table {tableId} already exists", AdbcStatusCode.AlreadyExists);
                    await CreateTableAsync(projectId, datasetId, tableId, arrowSchema, cancellationToken).ConfigureAwait(false);
                    break;

                case BulkIngestMode.Append:
                    if (!tableExists)
                        throw new AdbcException($"Table {tableId} does not exist", AdbcStatusCode.NotFound);
                    break;

                case BulkIngestMode.Replace:
                    if (tableExists)
                        await _restClient.DeleteTableAsync(tableRef, cancellationToken: cancellationToken).ConfigureAwait(false);
                    await CreateTableAsync(projectId, datasetId, tableId, arrowSchema, cancellationToken).ConfigureAwait(false);
                    break;

                case BulkIngestMode.CreateAppend:
                    if (!tableExists)
                        await CreateTableAsync(projectId, datasetId, tableId, arrowSchema, cancellationToken).ConfigureAwait(false);
                    break;

                default:
                    throw new AdbcException($"Unsupported bulk ingest mode: {mode}", AdbcStatusCode.NotImplemented);
            }
        }

        private async Task CreateTableAsync(
            string projectId,
            string datasetId,
            string tableId,
            Schema arrowSchema,
            CancellationToken cancellationToken)
        {
            var bqSchema = ArrowSchemaToBigQuerySchema(arrowSchema);
            var tableRef = _restClient.GetTableReference(projectId, datasetId, tableId);
            await _restClient.CreateTableAsync(tableRef, bqSchema, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        #region Arrow type normalization

        /// <summary>
        /// Normalizes Arrow types to those BigQuery accepts.
        /// BigQuery assumes microsecond precision for all time/timestamp values.
        /// </summary>
        internal static RecordBatch NormalizeBatch(RecordBatch batch)
        {
            bool needsNormalization = false;
            foreach (var field in batch.Schema.FieldsList)
            {
                if (NeedsNormalization(field.DataType))
                {
                    needsNormalization = true;
                    break;
                }
            }

            if (!needsNormalization)
                return batch;

            var newFields = new List<Field>(batch.Schema.FieldsList.Count);
            var newArrays = new IArrowArray[batch.ColumnCount];

            for (int i = 0; i < batch.ColumnCount; i++)
            {
                var field = batch.Schema.FieldsList[i];
                var array = batch.Column(i);

                if (NeedsNormalization(field.DataType))
                {
                    var (newType, newArray) = NormalizeArray(field.DataType, array);
                    newFields.Add(new Field(field.Name, newType, field.IsNullable, field.Metadata));
                    newArrays[i] = newArray;
                }
                else
                {
                    newFields.Add(field);
                    newArrays[i] = array;
                }
            }

            var newSchema = new Schema(newFields, batch.Schema.Metadata);
            return new RecordBatch(newSchema, newArrays, batch.Length);
        }

        private static bool NeedsNormalization(IArrowType type)
        {
            return type switch
            {
                Time32Type => true,
                Time64Type t => t.Unit != TimeUnit.Microsecond,
                TimestampType t => t.Unit != TimeUnit.Microsecond,
                _ => false
            };
        }

        private static (IArrowType, IArrowArray) NormalizeArray(IArrowType type, IArrowArray array)
        {
            switch (type)
            {
                case Time32Type t32:
                {
                    var source = (Time32Array)array;
                    var builder = new Time64Array.Builder(TimeUnit.Microsecond);
                    long multiplier = t32.Unit == TimeUnit.Second ? 1_000_000L : 1_000L;
                    for (int i = 0; i < source.Length; i++)
                    {
                        if (source.IsNull(i))
                            builder.AppendNull();
                        else
                            builder.Append(source.GetValue(i)!.Value * multiplier);
                    }
                    return (new Time64Type(TimeUnit.Microsecond), builder.Build());
                }
                case Time64Type t64:
                {
                    var source = (Time64Array)array;
                    var builder = new Time64Array.Builder(TimeUnit.Microsecond);
                    // t64.Unit must be Nanosecond (Microsecond is handled by NeedsNormalization)
                    for (int i = 0; i < source.Length; i++)
                    {
                        if (source.IsNull(i))
                            builder.AppendNull();
                        else
                            builder.Append(source.GetValue(i)!.Value / 1_000L);
                    }
                    return (new Time64Type(TimeUnit.Microsecond), builder.Build());
                }
                case TimestampType ts:
                {
                    var source = (TimestampArray)array;
                    var targetType = new TimestampType(TimeUnit.Microsecond, ts.Timezone);
                    var builder = new TimestampArray.Builder(targetType);
                    for (int i = 0; i < source.Length; i++)
                    {
                        if (source.IsNull(i))
                            builder.AppendNull();
                        else
                            builder.Append(source.GetTimestamp(i)!.Value);
                    }
                    return (targetType, builder.Build());
                }
                default:
                    throw new InvalidOperationException($"Unexpected type for normalization: {type}");
            }
        }

        #endregion

        #region Arrow schema to BigQuery schema conversion

        /// <summary>
        /// Converts an Arrow schema to a BigQuery table schema for table creation.
        /// </summary>
        internal static Google.Apis.Bigquery.v2.Data.TableSchema ArrowSchemaToBigQuerySchema(Schema arrowSchema)
        {
            var fields = new List<TableFieldSchema>(arrowSchema.FieldsList.Count);
            foreach (var field in arrowSchema.FieldsList)
            {
                fields.Add(ArrowFieldToBigQueryField(field));
            }
            return new Google.Apis.Bigquery.v2.Data.TableSchema { Fields = fields };
        }

        private static TableFieldSchema ArrowFieldToBigQueryField(Field field)
        {
            var bqField = new TableFieldSchema
            {
                Name = field.Name,
                Mode = field.IsNullable ? "NULLABLE" : "REQUIRED",
            };

            switch (field.DataType)
            {
                case BinaryType:
                    bqField.Type = "BYTES";
                    break;
                case Decimal128Type d128:
                    bqField.Type = "NUMERIC";
                    bqField.Precision = d128.Precision;
                    bqField.Scale = d128.Scale;
                    break;
                case Decimal256Type d256:
                    bqField.Type = "BIGNUMERIC";
                    bqField.Precision = d256.Precision;
                    bqField.Scale = d256.Scale;
                    break;
                case FixedSizeBinaryType:
                    bqField.Type = "BYTES";
                    break;
                case BooleanType:
                    bqField.Type = "BOOLEAN";
                    break;
                case Date32Type:
                case Date64Type:
                    bqField.Type = "DATE";
                    break;
                case FloatType:
                case DoubleType:
                    bqField.Type = "FLOAT";
                    break;
                case Int8Type:
                case Int16Type:
                case Int32Type:
                case Int64Type:
                case UInt8Type:
                case UInt16Type:
                case UInt32Type:
                case UInt64Type:
                    bqField.Type = "INTEGER";
                    break;
                case StringType:
                    bqField.Type = "STRING";
                    break;
                case Time32Type:
                case Time64Type:
                    bqField.Type = "TIME";
                    break;
                case TimestampType ts:
                    bqField.Type = string.IsNullOrEmpty(ts.Timezone) ? "DATETIME" : "TIMESTAMP";
                    break;
                case StructType structType:
                    bqField.Type = "RECORD";
                    var nestedFields = new List<TableFieldSchema>();
                    foreach (var nestedField in structType.Fields)
                    {
                        nestedFields.Add(ArrowFieldToBigQueryField(nestedField));
                    }
                    bqField.Fields = nestedFields;
                    break;
                case ListType listType:
                    var elementField = ArrowFieldToBigQueryField(listType.ValueField);
                    bqField.Type = elementField.Type;
                    bqField.Mode = "REPEATED";
                    bqField.Fields = elementField.Fields;
                    bqField.Precision = elementField.Precision;
                    bqField.Scale = elementField.Scale;
                    break;
                default:
                    throw new AdbcException($"Unsupported Arrow type for BigQuery ingest: {field.DataType}", AdbcStatusCode.NotImplemented);
            }

            return bqField;
        }

        #endregion

        #region Arrow IPC serialization

        private static byte[] SerializeSchema(Schema schema)
        {
            return ArrowSerializationHelpers.SerializeSchema(schema);
        }

        private static byte[] SerializeRecordBatch(RecordBatch batch)
        {
            return ArrowSerializationHelpers.SerializeRecordBatch(batch);
        }

        #endregion
    }
}
