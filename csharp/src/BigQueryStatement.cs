/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google;
using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Cloud.BigQuery.V2;
using TableFieldSchema = Google.Apis.Bigquery.v2.Data.TableFieldSchema;
using TableSchema = Google.Apis.Bigquery.v2.Data.TableSchema;

namespace AdbcDrivers.BigQuery
{
    /// <summary>
    /// BigQuery-specific implementation of <see cref="AdbcStatement"/>
    /// </summary>
    class BigQueryStatement : TracingStatement, ITokenProtectedResource, IDisposable
    {
        readonly BigQueryConnection bigQueryConnection;
        readonly CancellationRegistry cancellationRegistry;

        bool isMetadataCommand = false;
        string? catalogName = null;
        string? schemaName = null;
        string? tableName = null;

        public BigQueryStatement(BigQueryConnection bigQueryConnection) : base(bigQueryConnection)
        {
            if (bigQueryConnection == null) { throw new AdbcException($"{nameof(bigQueryConnection)} cannot be null", AdbcStatusCode.InvalidArgument); }

            // pass on the handler since this isn't accessible publicly
            UpdateToken = bigQueryConnection.UpdateToken;

            this.bigQueryConnection = bigQueryConnection;
            this.cancellationRegistry = new CancellationRegistry();
        }

        public Func<Task>? UpdateToken { get; set; }

        internal Dictionary<string, string>? Options { get; set; }

        private BigQueryClient Client => this.bigQueryConnection.Client ?? throw new AdbcException("Client cannot be null");

        private GoogleCredential Credential => this.bigQueryConnection.Credential ?? throw new AdbcException("Credential cannot be null");

        private int MaxRetryAttempts => this.bigQueryConnection.MaxRetryAttempts;

        private int RetryDelayMs => this.bigQueryConnection.RetryDelayMs;

        public override string AssemblyVersion => BigQueryUtils.BigQueryAssemblyVersion;

        public override string AssemblyName => BigQueryUtils.BigQueryAssemblyName;

        public override void SetOption(string key, string value)
        {
            if (Options == null)
            {
                Options = new Dictionary<string, string>();
            }

            Options[key] = value;
        }

        public override QueryResult ExecuteQuery()
        {
            try
            {
                return ExecuteQueryInternalAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex) when (BigQueryConnection.IsUnauthorizedException(ex, out GoogleApiException? googleEx))
            {
                throw new AdbcException(googleEx!.Message, AdbcStatusCode.Unauthorized, ex);
            }
        }

        private async Task<QueryResult> ExecuteQueryInternalAsync()
        {
            return await this.TraceActivityAsync(async activity =>
            {
                QueryOptions queryOptions = ValidateOptions(activity);

                activity?.AddConditionalTag(SemanticConventions.Db.Query.Text, SqlQuery, this.bigQueryConnection.IsSafeToTrace);

                if (isMetadataCommand)
                {
                    return await ExecuteMetadataCommandQuery(activity);
                }

                BigQueryJob job = await Client.CreateQueryJobAsync(SqlQuery, null, queryOptions);
                JobReference jobReference = job.Reference;
                GetQueryResultsOptions getQueryResultsOptions = new GetQueryResultsOptions();

                if (Options?.TryGetValue(BigQueryParameters.GetQueryResultsOptionsTimeout, out string? timeoutSeconds) == true &&
                    int.TryParse(timeoutSeconds, out int seconds) &&
                    seconds >= 0)
                {
                    getQueryResultsOptions.Timeout = TimeSpan.FromSeconds(seconds);
                    activity?.AddBigQueryParameterTag(BigQueryParameters.GetQueryResultsOptionsTimeout, seconds);
                }

                using JobCancellationContext cancellationContext = new JobCancellationContext(cancellationRegistry, job);

                // We can't checkJobStatus, Otherwise, the timeout in QueryResultsOptions is meaningless.
                // When encountering a long-running job, it should be controlled by the timeout in the Google SDK instead of blocking in a while loop.
                Func<Task<BigQueryResults>> getJobResults = async () =>
                {
                    return await ExecuteCancellableJobAsync(cancellationContext, activity, async (context) =>
                    {
                        // if the authentication token was reset, then we need a new job with the latest token
                        context.Job = await Client.GetJobAsync(jobReference, cancellationToken: context.CancellationToken).ConfigureAwait(false);
                        return await context.Job.GetQueryResultsAsync(getQueryResultsOptions, cancellationToken: context.CancellationToken).ConfigureAwait(false);
                    }).ConfigureAwait(false);
                };

                BigQueryResults results = await ExecuteWithRetriesAsync(getJobResults, activity, cancellationContext.CancellationToken).ConfigureAwait(false);

                TokenProtectedReadClientManger clientMgr = new TokenProtectedReadClientManger(Credential);
                clientMgr.UpdateToken = () => Task.Run(() =>
                {
                    this.bigQueryConnection.SetCredential();
                    clientMgr.UpdateCredential(Credential);
                });

                // For multi-statement queries, StatementType == "SCRIPT"
                if (results.TableReference == null || job.Statistics.Query.StatementType.Equals("SCRIPT", StringComparison.OrdinalIgnoreCase))
                {
                    string statementType = string.Empty;
                    if (Options?.TryGetValue(BigQueryParameters.StatementType, out string? statementTypeString) == true)
                    {
                        statementType = statementTypeString;
                    }
                    int statementIndex = 1;
                    if (Options?.TryGetValue(BigQueryParameters.StatementIndex, out string? statementIndexString) == true &&
                        int.TryParse(statementIndexString, out int statementIndexInt) &&
                        statementIndexInt > 0)
                    {
                        statementIndex = statementIndexInt;
                    }
                    string evaluationKind = string.Empty;
                    if (Options?.TryGetValue(BigQueryParameters.EvaluationKind, out string? evaluationKindString) == true)
                    {
                        evaluationKind = evaluationKindString;
                    }

                    Func<Task<BigQueryResults>> getMultiJobResults = async () =>
                    {
                        // To get the results of all statements in a multi-statement query, enumerate the child jobs. Related public docs: https://cloud.google.com/bigquery/docs/multi-statement-queries#get_all_executed_statements.
                        // Can filter by StatementType and EvaluationKind. Related public docs: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobstatistics2, https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#evaluationkind
                        ListJobsOptions listJobsOptions = new ListJobsOptions();
                        listJobsOptions.ParentJobId = results.JobReference.JobId;
                        var joblist = Client.ListJobs(listJobsOptions)
                            .Select(job => Client.GetJob(job.Reference))
                            .Where(job => string.IsNullOrEmpty(evaluationKind) || job.Statistics.ScriptStatistics.EvaluationKind.Equals(evaluationKind, StringComparison.OrdinalIgnoreCase))
                            .Where(job => string.IsNullOrEmpty(statementType) || job.Statistics.Query.StatementType.Equals(statementType, StringComparison.OrdinalIgnoreCase))
                            .OrderBy(job => job.Resource.Statistics.CreationTime)
                            .ToList();

                        if (joblist.Count > 0)
                        {
                            if (statementIndex < 1 || statementIndex > joblist.Count)
                            {
                                throw new ArgumentOutOfRangeException($"The specified index {statementIndex} is out of range. There are {joblist.Count} jobs available.");
                            }
                            BigQueryJob indexedJob = joblist[statementIndex - 1];
                            cancellationContext.Job = indexedJob;
                            return await ExecuteCancellableJobAsync(cancellationContext, activity, async (context) =>
                            {
                                return await indexedJob.GetQueryResultsAsync(getQueryResultsOptions, cancellationToken: context.CancellationToken).ConfigureAwait(false);
                            }).ConfigureAwait(false);
                        }

                        throw new AdbcException($"Unable to obtain result from statement [{statementIndex}]", AdbcStatusCode.InvalidData);
                    };

                    results = await ExecuteWithRetriesAsync(getMultiJobResults, activity, cancellationContext.CancellationToken).ConfigureAwait(false);
                }

                if (results?.TableReference == null)
                {
                    throw new AdbcException("There is no query statement");
                }

                string table = $"projects/{results.TableReference.ProjectId}/datasets/{results.TableReference.DatasetId}/tables/{results.TableReference.TableId}";

                int maxStreamCount = 1;

                if (Options?.TryGetValue(BigQueryParameters.MaxFetchConcurrency, out string? maxStreamCountString) == true)
                {
                    if (int.TryParse(maxStreamCountString, out int count))
                    {
                        if (count >= 0)
                        {
                            maxStreamCount = count;
                        }
                    }
                }

                long totalRows = results.TotalRows == null ? -1L : (long)results.TotalRows.Value;

                Func<Task<IEnumerable<IArrowReader>>> getArrowReadersFunc = async () =>
                {
                    return await ExecuteCancellableJobAsync(cancellationContext, activity, async (context) =>
                    {
                        // Cancelling this step may leave the server with unread streams.
                        return await GetArrowReaders(clientMgr, table, results.TableReference.ProjectId, maxStreamCount, activity, context.CancellationToken).ConfigureAwait(false);
                    }).ConfigureAwait(false);
                };
                IEnumerable<IArrowReader> readers = await ExecuteWithRetriesAsync(getArrowReadersFunc, activity, cancellationContext.CancellationToken).ConfigureAwait(false);

                // Note: MultiArrowReader must dispose the cancellationContext.
                IArrowArrayStream stream = new MultiArrowReader(this, TranslateSchema(results.Schema), readers, new CancellationContext(cancellationRegistry));
                activity?.AddTag(SemanticConventions.Db.Response.ReturnedRows, totalRows);
                return new QueryResult(totalRows, stream);
            });
        }

        private Task<QueryResult> ExecuteMetadataCommandQuery(Activity? activity)
        {
            const string SupportedMetadataCommands = "GetCatalogs,GetSchemas,GetTables,GetColumns,GetPrimaryKeys";
            return SqlQuery?.ToLowerInvariant() switch
            {
                "getcatalogs" => GetCatalogs(activity),
                "getschemas" => GetSchemas(activity),
                "gettables" => GetTables(activity),
                "getcolumns" => GetTableSchema(activity),
                "getprimarykeys" => GetPrimaryKeys(activity),
                null or "" => throw new ArgumentNullException(nameof(SqlQuery), $"Metadata command for property 'SqlQuery' must not be empty or null. Supported metadata commands: {SupportedMetadataCommands}"),
                _ => throw new NotSupportedException($"Metadata command '{SqlQuery}' is not supported. Supported metadata commands: {SupportedMetadataCommands}"),
            };
        }

        protected Task<QueryResult> GetTables(Activity? activity)
        {
            StringArray.Builder tableNameBuilder = new StringArray.Builder();
            StringArray.Builder tableTypeBuilder = new StringArray.Builder();
            Func<Task<PagedEnumerable<TableList, BigQueryTable>?>> func = () => Task.Run(() =>
            {
                return Client?.ListTables(this.catalogName, this.schemaName);
            });
            PagedEnumerable<TableList, BigQueryTable>? tables;
            tables = ExecuteWithRetriesAsync<PagedEnumerable<TableList, BigQueryTable>?>(func, activity).GetAwaiter().GetResult();
            if (tables != null)
            {
                // Sort tables by name for consistency
                var sortedTables = tables.OrderBy(x => x.Reference.TableId).ToList();

                foreach (var table in sortedTables)
                {
                    tableNameBuilder.Append(table.Reference.TableId);

                    // Map BigQuery types to ADBC standard types
                    string tableType = table.Resource.Type switch
                    {
                        "TABLE" => "BASE TABLE",  // ADBC standard name
                        "VIEW" => "VIEW",
                        "EXTERNAL" => "EXTERNAL TABLE",
                        "SNAPSHOT" => "SNAPSHOT",
                        "CLONE" => "CLONE",
                        _ => table.Resource.Type  // fallback to original
                    };
                    tableTypeBuilder.Append(tableType);
                }
            }
            IArrowArray[] dataArrays = new IArrowArray[]
            {
                tableNameBuilder.Build(),
                tableTypeBuilder.Build()
            };

            // Create schema with both columns
            Schema schema = new Schema(
                new Field[]
                {
                    new Field("table_name", StringType.Default, false),
                    new Field("table_type", StringType.Default, false)
                },
                metadata: null
            );

            schema.Validate(dataArrays);

            IArrowArrayStream stream = new BigQueryInfoArrowStream(schema, dataArrays);

            return Task.FromResult(new QueryResult(dataArrays[0].Length, stream));
        }

        protected Task<QueryResult> GetPrimaryKeys(Activity? activity)
        {
            StringArray.Builder columnNameBuilder = new StringArray.Builder();

            Func<Task<BigQueryTable?>> func = () => Task.Run(() =>
            {
                return Client?.GetTable(this.catalogName, this.schemaName, this.tableName);
            });

            BigQueryTable? table = ExecuteWithRetriesAsync<BigQueryTable?>(func, activity).GetAwaiter().GetResult();

            if (table?.Resource?.TableConstraints?.PrimaryKey?.Columns != null)
            {
                List<string> primaryKeyColumns = new List<string>();
                primaryKeyColumns = table.Resource.TableConstraints.PrimaryKey.Columns.ToList();
                foreach (string columnName in primaryKeyColumns)
                {
                    columnNameBuilder.Append(columnName);
                }
            }

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                columnNameBuilder.Build()
            };

            Schema schema = new Schema(
                new Field[] { new Field("column_name", StringType.Default, false) },
                metadata: null
            );

            schema.Validate(dataArrays);

            IArrowArrayStream stream = new BigQueryInfoArrowStream(schema, dataArrays);

            return Task.FromResult(new QueryResult(dataArrays[0].Length, stream));
        }

        /*
         * Retrieves the schema information for a BigQuery table, including all nested fields.
         * 
         * This method flattens nested STRUCT/RECORD types into individual rows, preserving all field properties
         * at every nesting level. Each field is represented by a separate row with its full qualified name
         * using dot notation (e.g., "address.city.name").
         * 
         * Output columns include:
         * - column_name: Full qualified field name with dot notation for nested fields
         * - column_type: BigQuery data type (STRING, INT64, STRUCT, etc.)
         * - column_mode: Field mode (NULLABLE, REQUIRED, REPEATED)
         * - column_description: Field description
         * - column_max_length: Maximum length for STRING/BYTES types
         * - column_precision: Precision for NUMERIC types
         * - column_scale: Scale for NUMERIC types
         * - column_default_value_expression: Default value expression
         * - column_collation: Collation specification
         * - column_policy_tags: List of policy tags
         * - column_rounding_mode: Rounding mode for NUMERIC types
         * - column_range_element_type: Element type for RANGE types
         * - column_depth: Nesting level (0 for top-level, 1+ for nested)
         * - column_parent_name: Full qualified parent field name (null for top-level)
         * 
         * Example:
         * For a BigQuery table with the following schema:
         * 
         *   CREATE TABLE example (
         *     id INT64,
         *     name STRING,
         *     address STRUCT<
         *       street STRING,
         *       city STRUCT<
         *         name STRING,
         *         zipcode INT64
         *       >
         *     >
         *   )
         * 
         * The output will contain these rows:
         * 
         *   | column_name          | column_type | column_depth | column_parent_name |
         *   |----------------------|-------------|--------------|-------------------|
         *   | id                   | INT64       | 0            | null              |
         *   | name                 | STRING      | 0            | null              |
         *   | address              | STRUCT      | 0            | null              |
         *   | address.street       | STRING      | 1            | address           |
         *   | address.city         | STRUCT      | 1            | address           |
         *   | address.city.name    | STRING      | 2            | address.city      |
         *   | address.city.zipcode | INT64       | 2            | address.city      |
         */
        protected Task<QueryResult> GetTableSchema(Activity? activity)
        {
            StringArray.Builder columnNameBuilder = new StringArray.Builder();
            StringArray.Builder columnTypeBuilder = new StringArray.Builder();
            StringArray.Builder columnModeBuilder = new StringArray.Builder();
            StringArray.Builder columnDescriptionBuilder = new StringArray.Builder();
            Int64Array.Builder columnMaxLengthBuilder = new Int64Array.Builder();
            Int64Array.Builder columnPrecisionBuilder = new Int64Array.Builder();
            Int64Array.Builder columnScaleBuilder = new Int64Array.Builder();
            StringArray.Builder columnDefaultValueExpressionBuilder = new StringArray.Builder();
            StringArray.Builder columnCollationBuilder = new StringArray.Builder();
            ListArray.Builder columnPolicyTagsBuilder = new ListArray.Builder(StringType.Default);
            StringArray.Builder columnRoundingModeBuilder = new StringArray.Builder();
            StringArray.Builder columnRangeElementTypeBuilder = new StringArray.Builder();
            Int64Array.Builder columnDepthBuilder = new Int64Array.Builder();
            StringArray.Builder columnParentNameBuilder = new StringArray.Builder();

            Func<Task<BigQueryTable?>> func = () => Task.Run(() =>
            {
                return Client?.GetTable(this.catalogName, this.schemaName, this.tableName);
            });
            BigQueryTable? table = ExecuteWithRetriesAsync<BigQueryTable?>(func, activity).GetAwaiter().GetResult();

            if (table != null)
            {
                FlattenFields(table.Schema.Fields, string.Empty, string.Empty, 0,
                    columnNameBuilder, columnTypeBuilder, columnModeBuilder, columnDescriptionBuilder,
                    columnMaxLengthBuilder, columnPrecisionBuilder, columnScaleBuilder,
                    columnDefaultValueExpressionBuilder, columnCollationBuilder,
                    columnPolicyTagsBuilder, columnRoundingModeBuilder, columnRangeElementTypeBuilder,
                    columnDepthBuilder, columnParentNameBuilder);
            }

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                columnNameBuilder.Build(),
                columnTypeBuilder.Build(),
                columnModeBuilder.Build(),
                columnDescriptionBuilder.Build(),
                columnMaxLengthBuilder.Build(),
                columnPrecisionBuilder.Build(),
                columnScaleBuilder.Build(),
                columnDefaultValueExpressionBuilder.Build(),
                columnCollationBuilder.Build(),
                columnPolicyTagsBuilder.Build(),
                columnRoundingModeBuilder.Build(),
                columnRangeElementTypeBuilder.Build(),
                columnDepthBuilder.Build(),
                columnParentNameBuilder.Build()
            };

            Schema schema = new Schema(
                new Field[]
                {
                    new Field("column_name", StringType.Default, false),
                    new Field("column_type", StringType.Default, false),
                    new Field("column_mode", StringType.Default, true),
                    new Field("column_description", StringType.Default, true),
                    new Field("column_max_length", Int64Type.Default, true),
                    new Field("column_precision", Int64Type.Default, true),
                    new Field("column_scale", Int64Type.Default, true),
                    new Field("column_default_value_expression", StringType.Default, true),
                    new Field("column_collation", StringType.Default, true),
                    new Field("column_policy_tags", new ListType(StringType.Default), true),
                    new Field("column_rounding_mode", StringType.Default, true),
                    new Field("column_range_element_type", StringType.Default, true),
                    new Field("column_depth", Int64Type.Default, false),
                    new Field("column_parent_name", StringType.Default, true)
                },
                metadata: null
            );

            schema.Validate(dataArrays);

            IArrowArrayStream stream = new BigQueryInfoArrowStream(schema, dataArrays);

            return Task.FromResult(new QueryResult(dataArrays[0].Length, stream));
        }

        /*
        * Recursively flattens nested BigQuery table fields into a flat structure with dot notation.
        * 
        * This method processes each field in the collection and recursively handles nested STRUCT/RECORD types.
        * For each field, it:
        * 1. Constructs the full qualified name using dot notation
        * 2. Appends all field properties to their respective builders
        * 3. Records the depth level and parent field name
        * 4. Recursively processes nested fields if present
        * 
        * Example:
        * Given a field structure:
        * 
        *   address STRUCT<
        *     street STRING,
        *     location STRUCT<
        *       lat FLOAT64,
        *       lon FLOAT64
        *     >
        *   >
        * 
        * The method will generate these entries:
        * 
        *   Depth 0: "address" (parent: null)
        *   Depth 1: "address.street" (parent: "address")
        *   Depth 1: "address.location" (parent: "address")
        *   Depth 2: "address.location.lat" (parent: "address.location")
        *   Depth 2: "address.location.lon" (parent: "address.location")
        */
        private void FlattenFields(
            IList<TableFieldSchema> fields,
            string prefix,
            string parentName,
            int depth,
            StringArray.Builder columnNameBuilder,
            StringArray.Builder columnTypeBuilder,
            StringArray.Builder columnModeBuilder,
            StringArray.Builder columnDescriptionBuilder,
            Int64Array.Builder columnMaxLengthBuilder,
            Int64Array.Builder columnPrecisionBuilder,
            Int64Array.Builder columnScaleBuilder,
            StringArray.Builder columnDefaultValueExpressionBuilder,
            StringArray.Builder columnCollationBuilder,
            ListArray.Builder columnPolicyTagsBuilder,
            StringArray.Builder columnRoundingModeBuilder,
            StringArray.Builder columnRangeElementTypeBuilder,
            Int64Array.Builder columnDepthBuilder,
            StringArray.Builder columnParentNameBuilder)
        {
            foreach (TableFieldSchema field in fields)
            {
                string fieldName = string.IsNullOrEmpty(prefix) ? field.Name : $"{prefix}.{field.Name}";

                columnNameBuilder.Append(fieldName);
                columnTypeBuilder.Append(field.Type);
                columnModeBuilder.Append(field.Mode);
                columnDescriptionBuilder.Append(field.Description);

                if (field.MaxLength.HasValue)
                    columnMaxLengthBuilder.Append(field.MaxLength.Value);
                else
                    columnMaxLengthBuilder.AppendNull();

                if (field.Precision.HasValue)
                    columnPrecisionBuilder.Append(field.Precision.Value);
                else
                    columnPrecisionBuilder.AppendNull();

                if (field.Scale.HasValue)
                    columnScaleBuilder.Append(field.Scale.Value);
                else
                    columnScaleBuilder.AppendNull();

                columnDefaultValueExpressionBuilder.Append(field.DefaultValueExpression);
                columnCollationBuilder.Append(field.Collation);
                if (field.PolicyTags?.Names != null && field.PolicyTags.Names.Count > 0)
                {
                    StringArray.Builder valueBuilder = (StringArray.Builder)columnPolicyTagsBuilder.ValueBuilder;
                    foreach (string tag in field.PolicyTags.Names)
                    {
                        valueBuilder.Append(tag);
                    }
                    columnPolicyTagsBuilder.Append();
                }
                else
                {
                    columnPolicyTagsBuilder.AppendNull();
                }
                columnRoundingModeBuilder.Append(field.RoundingMode);
                columnRangeElementTypeBuilder.Append(field.RangeElementType?.Type);

                // Add depth information
                columnDepthBuilder.Append(depth);

                // Add parent name (null for top-level fields)
                columnParentNameBuilder.Append(string.IsNullOrEmpty(parentName) ? null : parentName);

                // Recursively flatten nested fields (for STRUCT/RECORD types)
                if (field.Fields != null && field.Fields.Count > 0)
                {
                    FlattenFields(field.Fields, fieldName, fieldName, depth + 1,
                        columnNameBuilder, columnTypeBuilder, columnModeBuilder, columnDescriptionBuilder,
                        columnMaxLengthBuilder, columnPrecisionBuilder, columnScaleBuilder,
                        columnDefaultValueExpressionBuilder, columnCollationBuilder,
                        columnPolicyTagsBuilder, columnRoundingModeBuilder, columnRangeElementTypeBuilder,
                        columnDepthBuilder, columnParentNameBuilder);
                }
            }
        }

        protected Task<QueryResult> GetSchemas(Activity? activity)
        {
            StringArray.Builder schemaNameBuilder = new StringArray.Builder();
            List<string> datasetIds = new List<string>();
            Func<Task<PagedEnumerable<DatasetList, BigQueryDataset>?>> func = () => Task.Run(() =>
            {
                // stick with this call because PagedAsyncEnumerable has different behaviors for selecting items
                return Client?.ListDatasets(this.catalogName);
            });
            PagedEnumerable<DatasetList, BigQueryDataset>? datasets;
            datasets = ExecuteWithRetriesAsync<PagedEnumerable<DatasetList, BigQueryDataset>?>(func, activity).GetAwaiter().GetResult();
            if (datasets != null)
            {
                datasetIds = datasets.Select(x => x.Reference.DatasetId).ToList();
                datasetIds.Sort();
                foreach (string datasetId in datasetIds)
                {
                    schemaNameBuilder.Append(datasetId);
                }
            }

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                schemaNameBuilder.Build()
            };

            Schema schema = GetObjectSchema("SchemaName");
            schema.Validate(dataArrays);

            IArrowArrayStream stream = new BigQueryInfoArrowStream(schema, dataArrays);

            return Task.FromResult(new QueryResult(dataArrays[0].Length, stream));
        }

        protected Task<QueryResult> GetCatalogs(Activity? activity)
        {
            StringArray.Builder catalogNameBuilder = new StringArray.Builder();
            List<string> projectIds = new List<string>();
            Func<Task<PagedEnumerable<ProjectList, CloudProject>?>> func = () => Task.Run(() =>
            {
                // stick with this call because PagedAsyncEnumerable has different behaviors for selecting items
                return Client?.ListProjects();
            });
            PagedEnumerable<ProjectList, CloudProject>? catalogs;
            catalogs = ExecuteWithRetriesAsync<PagedEnumerable<ProjectList, CloudProject>?>(func, activity).GetAwaiter().GetResult();
            if (catalogs != null)
            {
                projectIds = catalogs.Select(x => x.ProjectId).ToList();
            }

            if (this.bigQueryConnection.IncludePublicProjectIds && !projectIds.Contains(BigQueryConstants.PublicProjectId))
            {
                projectIds.Add(BigQueryConstants.PublicProjectId);
            }

            projectIds.Sort();
            foreach (string projectId in projectIds)
            {
                catalogNameBuilder.Append(projectId);
            }

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                catalogNameBuilder.Build()
            };

            Schema schema = GetObjectSchema("CatalogName");
            schema.Validate(dataArrays);

            IArrowArrayStream stream = new BigQueryInfoArrowStream(schema, dataArrays);

            return Task.FromResult(new QueryResult(dataArrays[0].Length, stream));
        }

        private async Task<IEnumerable<IArrowReader>> GetArrowReaders(
            TokenProtectedReadClientManger clientMgr,
            string table,
            string projectId,
            int maxStreamCount,
            Activity? activity,
            CancellationToken cancellationToken = default)
        {
            ReadSession rs = new ReadSession { Table = table, DataFormat = DataFormat.Arrow };
            BigQueryReadClient bigQueryReadClient = clientMgr.ReadClient;
            ReadSession rrs = await bigQueryReadClient.CreateReadSessionAsync("projects/" + projectId, rs, maxStreamCount);

            var readers = rrs.Streams
                             .Select(s => ReadChunk(bigQueryReadClient, s.Name, activity, this.bigQueryConnection.IsSafeToTrace, cancellationToken))
                             .Where(chunk => chunk != null)
                             .Cast<IArrowReader>();

            return readers;
        }

        public override UpdateResult ExecuteUpdate()
        {
            try
            {
                return ExecuteUpdateInternalAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex) when (BigQueryConnection.IsUnauthorizedException(ex, out GoogleApiException? googleEx))
            {
                throw new AdbcException(googleEx!.Message, AdbcStatusCode.Unauthorized, ex);
            }
        }

        public override void Cancel()
        {
            this.TraceActivity(_ =>
            {
                this.cancellationRegistry.CancelAll();
            });
        }

        public override void Dispose()
        {
            this.cancellationRegistry.Dispose();
            base.Dispose();
        }

        private Schema GetObjectSchema(string objectName)
        {
            return new Schema(
                new Field[]
                {
                    new Field(objectName, StringType.Default, false)
                },
                metadata: null);
        }

        private async Task<UpdateResult> ExecuteUpdateInternalAsync()
        {
            return await this.TraceActivityAsync(async activity =>
            {
                GetQueryResultsOptions getQueryResultsOptions = new GetQueryResultsOptions();

                if (Options?.TryGetValue(BigQueryParameters.GetQueryResultsOptionsTimeout, out string? timeoutSeconds) == true &&
                    int.TryParse(timeoutSeconds, out int seconds) &&
                    seconds >= 0)
                {
                    getQueryResultsOptions.Timeout = TimeSpan.FromSeconds(seconds);
                    activity?.AddBigQueryParameterTag(BigQueryParameters.GetQueryResultsOptionsTimeout, seconds);
                }

                activity?.AddConditionalTag(SemanticConventions.Db.Query.Text, SqlQuery, this.bigQueryConnection.IsSafeToTrace);

                using JobCancellationContext context = new(cancellationRegistry);
                // Cannot set destination table in jobs with DDL statements, otherwise an error will be prompted
                Func<Task<BigQueryResults?>> getQueryResultsAsyncFunc = async () =>
                {
                    return await ExecuteCancellableJobAsync(context, activity, async (context) =>
                    {
                        context.Job = await this.Client.CreateQueryJobAsync(SqlQuery, null, null, context.CancellationToken).ConfigureAwait(false);
                        return await context.Job.GetQueryResultsAsync(getQueryResultsOptions, context.CancellationToken).ConfigureAwait(false);
                    }).ConfigureAwait(false);
                };
                BigQueryResults? result = await ExecuteWithRetriesAsync(getQueryResultsAsyncFunc, activity, context.CancellationToken);
                long updatedRows = result?.NumDmlAffectedRows.HasValue == true ? result.NumDmlAffectedRows.Value : -1L;

                activity?.AddTag(SemanticConventions.Db.Response.ReturnedRows, updatedRows);
                return new UpdateResult(updatedRows);
            });
        }

        private Schema TranslateSchema(TableSchema schema)
        {
            return new Schema(schema.Fields.Select(TranslateField), null);
        }

        private Field TranslateField(TableFieldSchema field)
        {
            List<KeyValuePair<string, string>> metadata = new List<KeyValuePair<string, string>>()
            {
                new KeyValuePair<string, string>("BIGQUERY_TYPE", field.Type),
                new KeyValuePair<string, string>("BIGQUERY_MODE", field.Mode)
            };

            return new Field(field.Name, TranslateType(field), field.Mode == "NULLABLE", metadata);
        }

        private IArrowType TranslateType(TableFieldSchema field)
        {
            // per https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableFieldSchema.html#getType--

            switch (field.Type)
            {
                case "INTEGER" or "INT64":
                    return GetType(field, Int64Type.Default);
                case "FLOAT" or "FLOAT64":
                    return GetType(field, DoubleType.Default);
                case "BOOL" or "BOOLEAN":
                    return GetType(field, BooleanType.Default);
                case "STRING":
                    return GetType(field, StringType.Default);
                case "BYTES":
                    return GetType(field, BinaryType.Default);
                case "DATETIME":
                    return GetType(field, TimestampType.Default);
                case "TIMESTAMP":
                    return GetType(field, TimestampType.Default);
                case "TIME":
                    return GetType(field, Time64Type.Microsecond);
                case "DATE":
                    return GetType(field, Date32Type.Default);
                case "RECORD" or "STRUCT":
                    return GetType(field, BuildStructType(field));

                // treat these values as strings
                case "GEOGRAPHY" or "JSON":
                    return GetType(field, StringType.Default);

                // get schema cannot get precision and scale for NUMERIC or BIGNUMERIC types
                // instead, the max values are returned from BigQuery
                // see 'precision' on https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
                // and discussion in https://github.com/apache/arrow-adbc/pull/1192#discussion_r1365987279

                case "NUMERIC" or "DECIMAL":
                    return GetType(field, new Decimal128Type(38, 9));

                case "BIGNUMERIC" or "BIGDECIMAL":
                    if (Options != null)
                        return bool.Parse(Options[BigQueryParameters.LargeDecimalsAsString]) ? GetType(field, StringType.Default) : GetType(field, new Decimal256Type(76, 38));
                    else
                        return GetType(field, StringType.Default);

                default: throw new InvalidOperationException($"{field.Type} cannot be translated");
            }
        }

        private StructType BuildStructType(TableFieldSchema field)
        {
            List<Field> arrowFields = new List<Field>();

            foreach (TableFieldSchema subfield in field.Fields)
            {
                Field arrowField = TranslateField(subfield);
                arrowFields.Add(arrowField);
            }

            return new StructType(arrowFields.AsReadOnly());
        }

        private IArrowType GetType(TableFieldSchema field, IArrowType type)
        {
            if (field.Mode == "REPEATED")
                return new ListType(type);

            return type;
        }

        private static IArrowReader? ReadChunk(BigQueryReadClient client, string streamName, Activity? activity, bool isSafeToTrace, CancellationToken cancellationToken = default)
        {
            // Ideally we wouldn't need to indirect through a stream, but the necessary APIs in Arrow
            // are internal. (TODO: consider changing Arrow).
            activity?.AddConditionalBigQueryTag("read_stream", streamName, isSafeToTrace);
            BigQueryReadClient.ReadRowsStream readRowsStream = client.ReadRows(new ReadRowsRequest { ReadStream = streamName });
            IAsyncEnumerator<ReadRowsResponse> enumerator = readRowsStream.GetResponseStream().GetAsyncEnumerator(cancellationToken);

            ReadRowsStream stream = new ReadRowsStream(enumerator);
            activity?.AddBigQueryTag("read_stream.has_rows", stream.HasRows);

            return stream.HasRows ? stream : null;
        }

        private QueryOptions ValidateOptions(Activity? activity)
        {
            QueryOptions options = new QueryOptions();

            if (Client.ProjectId == BigQueryConstants.DetectProjectId)
            {
                activity?.AddBigQueryTag("client_project_id", BigQueryConstants.DetectProjectId);

                // An error occurs when calling CreateQueryJob without the ID set,
                // so use the first one that is found. This does not prevent from calling
                // to other 'project IDs' (catalogs) with a query.
                Func<Task<PagedEnumerable<ProjectList, CloudProject>?>> func = () => Task.Run(() =>
                {
                    return Client?.ListProjects();
                });

                PagedEnumerable<ProjectList, CloudProject>? projects = ExecuteWithRetriesAsync<PagedEnumerable<ProjectList, CloudProject>?>(func, activity).GetAwaiter().GetResult();

                if (projects != null)
                {
                    string? firstProjectId = projects.Select(x => x.ProjectId).FirstOrDefault();

                    if (firstProjectId != null)
                    {
                        options.ProjectId = firstProjectId;
                        activity?.AddBigQueryTag("detected_client_project_id", firstProjectId);
                        // need to reopen the Client with the projectId specified
                        this.bigQueryConnection.Open(firstProjectId);
                    }
                }
            }

            if (Options == null || Options.Count == 0)
                return options;

            string largeResultDatasetId = BigQueryConstants.DefaultLargeDatasetId;

            foreach (KeyValuePair<string, string> keyValuePair in Options)
            {
                switch (keyValuePair.Key)
                {
                    case BigQueryParameters.AllowLargeResults:
                        options.AllowLargeResults = true ? keyValuePair.Value.Equals("true", StringComparison.OrdinalIgnoreCase) : false;
                        activity?.AddBigQueryParameterTag(BigQueryParameters.AllowLargeResults, options.AllowLargeResults);
                        break;
                    case BigQueryParameters.LargeResultsDataset:
                        largeResultDatasetId = keyValuePair.Value;
                        activity?.AddBigQueryParameterTag(BigQueryParameters.LargeResultsDataset, largeResultDatasetId);
                        break;
                    case BigQueryParameters.LargeResultsDestinationTable:
                        string destinationTable = keyValuePair.Value;

                        if (!destinationTable.Contains("."))
                        {
                            throw new InvalidOperationException($"{BigQueryParameters.LargeResultsDestinationTable} is invalid");
                        }

                        string projectId = string.Empty;
                        string datasetId = string.Empty;
                        string tableId = string.Empty;

                        string[] segments = destinationTable.Split('.');

                        if (segments.Length != 3)
                            throw new InvalidOperationException($"{BigQueryParameters.LargeResultsDestinationTable} cannot be parsed");

                        projectId = segments[0];
                        datasetId = segments[1];
                        tableId = segments[2];

                        if (string.IsNullOrEmpty(projectId.Trim()) || string.IsNullOrEmpty(datasetId.Trim()) || string.IsNullOrEmpty(tableId.Trim()))
                            throw new InvalidOperationException($"{BigQueryParameters.LargeResultsDestinationTable} contains invalid values");

                        options.DestinationTable = new TableReference()
                        {
                            ProjectId = projectId,
                            DatasetId = datasetId,
                            TableId = tableId
                        };
                        activity?.AddBigQueryParameterTag(BigQueryParameters.LargeResultsDestinationTable, destinationTable);
                        break;
                    case BigQueryParameters.UseLegacySQL:
                        options.UseLegacySql = true ? keyValuePair.Value.Equals("true", StringComparison.OrdinalIgnoreCase) : false;
                        activity?.AddBigQueryParameterTag(BigQueryParameters.UseLegacySQL, options.UseLegacySql);
                        break;
                    case BigQueryParameters.IsMetadataCommand:
                        isMetadataCommand = keyValuePair.Value.Equals("true", StringComparison.OrdinalIgnoreCase);
                        activity?.AddBigQueryParameterTag(BigQueryParameters.IsMetadataCommand, isMetadataCommand);
                        break;
                    case BigQueryParameters.CatalogName:
                        catalogName = keyValuePair.Value;
                        break;
                    case BigQueryParameters.SchemaName:
                        schemaName = keyValuePair.Value;
                        break;
                    case BigQueryParameters.TableName:
                        tableName = keyValuePair.Value;
                        break;
                }
            }

            if (options.AllowLargeResults == true && options.DestinationTable == null)
            {
                options.DestinationTable = TryGetLargeDestinationTableReference(largeResultDatasetId, activity);
            }

            return options;
        }

        /// <summary>
        /// Attempts to retrieve or create the specified dataset.
        /// </summary>
        /// <param name="datasetId">The name of the dataset.</param>
        /// <param name="activity">The current activity for tracing.</param>
        /// <returns>A <see cref="TableReference"/> to a randomly generated table name in the specified dataset.</returns>
        private TableReference TryGetLargeDestinationTableReference(string datasetId, Activity? activity)
        {
            BigQueryDataset? dataset = null;

            try
            {
                activity?.AddBigQueryTag("large_results.dataset.try_find", datasetId);
                dataset = this.Client.GetDataset(datasetId);
                activity?.AddBigQueryTag("large_results.dataset.found", datasetId);
            }
            catch (GoogleApiException gaEx)
            {
                if (gaEx.HttpStatusCode != System.Net.HttpStatusCode.NotFound)
                {
                    activity?.AddException(gaEx);
                    throw new AdbcException($"Failure trying to retrieve dataset {datasetId}", gaEx);
                }
            }

            if (dataset == null && bigQueryConnection.CreateLargeResultsDataset)
            {
                try
                {
                    activity?.AddBigQueryTag("large_results.dataset.try_create", datasetId);
                    activity?.AddBigQueryTag("large_results.dataset.try_create_region", this.Client.DefaultLocation);
                    DatasetReference reference = this.Client.GetDatasetReference(datasetId);

                    // The location is not set here because it will use the DefaultLocation from the client.
                    // Similar to the DefaultLocation for the client, if the caller attempts to use a public
                    // dataset from a multi-region but set the destination to a specific location,
                    // a similar permission error is thrown.
                    BigQueryDataset bigQueryDataset = new BigQueryDataset(this.Client, new Dataset()
                    {
                        DatasetReference = reference,
                        DefaultTableExpirationMs = (long)TimeSpan.FromDays(1).TotalMilliseconds
                    });

                    dataset = this.Client.CreateDataset(datasetId, bigQueryDataset.Resource);
                    activity?.AddBigQueryTag("large_results.dataset.created", datasetId);
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex);
                    throw new AdbcException($"Could not create dataset {datasetId} in {this.Client.DefaultLocation}", ex);
                }
            }

            if (dataset == null)
            {
                throw new AdbcException($"Could not find dataset {datasetId}", AdbcStatusCode.NotFound);
            }
            else
            {
                TableReference reference = new TableReference()
                {
                    ProjectId = this.Client.ProjectId,
                    DatasetId = datasetId,
                    TableId = "lg_" + Guid.NewGuid().ToString().Replace("-", "")
                };

                activity?.AddBigQueryTag("large_results.table_reference", reference.ToString());

                return reference;
            }
        }

        public bool TokenRequiresUpdate(Exception ex) => BigQueryUtils.TokenRequiresUpdate(ex);

        private async Task<T> ExecuteWithRetriesAsync<T>(Func<Task<T>> action, Activity? activity, CancellationToken cancellationToken = default) =>
            await RetryManager.ExecuteWithRetriesAsync<T>(this, action, activity, MaxRetryAttempts, RetryDelayMs, cancellationToken);

        private async Task<T> ExecuteCancellableJobAsync<T>(
            JobCancellationContext context,
            Activity? activity,
            Func<JobCancellationContext, Task<T>> func)
        {
            try
            {
                return await func(context).ConfigureAwait(false);
            }
            catch (Exception ex)
                when (context.CancellationToken.IsCancellationRequested &&
                    BigQueryUtils.ContainsException(ex, out OperationCanceledException? cancelledEx))
            {
                // Note: OperationCanceledException could be thrown from the call,
                // but we only want to handle when the cancellation was requested from the context.
                activity?.AddException(cancelledEx!);
                try
                {
                    if (context.Job != null)
                    {
                        activity?.AddBigQueryTag("job.cancel", context.Job.Reference.JobId);
                        await context.Job.CancelAsync().ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    activity?.AddException(e);
                }
                throw;
            }
            finally
            {
                // Job is no longer in context after completion or cancellation
                context.Job = null;
            }
        }

        private class CancellationContext : IDisposable
        {
            private readonly CancellationRegistry cancellationRegistry;
            private readonly CancellationTokenSource cancellationTokenSource;
            private bool disposed;

            public CancellationContext(CancellationRegistry cancellationRegistry)
            {
                cancellationTokenSource = new CancellationTokenSource();
                this.cancellationRegistry = cancellationRegistry;
                this.cancellationRegistry.Register(this);
            }

            public CancellationToken CancellationToken => cancellationTokenSource.Token;

            public void Cancel()
            {
                cancellationTokenSource.Cancel();
            }

            public virtual void Dispose()
            {
                if (!disposed)
                {
                    cancellationRegistry.Unregister(this);
                    cancellationTokenSource.Dispose();
                    disposed = true;
                }
            }
        }

        private class JobCancellationContext : CancellationContext
        {
            public JobCancellationContext(CancellationRegistry cancellationRegistry, BigQueryJob? job = default)
                : base(cancellationRegistry)
            {
                Job = job;
            }

            public BigQueryJob? Job { get; set; }
        }

        private sealed class CancellationRegistry : IDisposable
        {
            private readonly ConcurrentDictionary<CancellationContext, byte> contexts = new();
            private bool disposed;

            public CancellationContext Register(CancellationContext context)
            {
                if (disposed) throw new ObjectDisposedException(nameof(CancellationRegistry));

                contexts.TryAdd(context, 0);
                return context;
            }

            public bool Unregister(CancellationContext context)
            {
                if (disposed) return false;

                return contexts.TryRemove(context, out _);
            }

            public void CancelAll()
            {
                if (disposed) throw new ObjectDisposedException(nameof(CancellationRegistry));

                foreach (CancellationContext context in contexts.Keys)
                {
                    context.Cancel();
                }
            }

            public void Dispose()
            {
                if (!disposed)
                {
                    contexts.Clear();
                    disposed = true;
                }
            }
        }

        private class MultiArrowReader : TracingReader
        {
            private static readonly string s_assemblyName = BigQueryUtils.GetAssemblyName(typeof(BigQueryStatement));
            private static readonly string s_assemblyVersion = BigQueryUtils.GetAssemblyVersion(typeof(BigQueryStatement));

            readonly Schema schema;
            readonly CancellationContext cancellationContext;
            IEnumerator<IArrowReader>? readers;
            IArrowReader? reader;
            bool disposed;

            public MultiArrowReader(BigQueryStatement statement, Schema schema, IEnumerable<IArrowReader> readers, CancellationContext cancellationContext) : base(statement)
            {
                this.schema = schema;
                this.readers = readers.GetEnumerator();
                this.cancellationContext = cancellationContext;
            }

            public override Schema Schema { get { return this.schema; } }

            public override string AssemblyVersion => s_assemblyVersion;

            public override string AssemblyName => s_assemblyName;

            public override async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                return await this.TraceActivityAsync(async activity =>
                {
                    if (this.readers == null)
                    {
                        return null;
                    }

                    using CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.cancellationContext.CancellationToken);

                    while (true)
                    {
                        linkedCts.Token.ThrowIfCancellationRequested();
                        if (this.reader == null)
                        {
                            if (!this.readers.MoveNext())
                            {
                                this.readers.Dispose();
                                this.readers = null;
                                return null;
                            }
                            this.reader = this.readers.Current;
                        }

                        RecordBatch result = await this.reader.ReadNextRecordBatchAsync(linkedCts.Token).ConfigureAwait(false);

                        if (result != null)
                        {
                            return result;
                        }

                        this.reader = null;
                    }
                });
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    if (!this.disposed)
                    {
                        if (this.readers != null)
                        {
                            this.readers.Dispose();
                            this.readers = null;
                        }
                        this.cancellationContext.Dispose();
                        this.disposed = true;
                    }
                }

                base.Dispose(disposing);
            }
        }

        sealed class ReadRowsStream : IArrowArrayStream
        {
            readonly Schema? schema;
            readonly IAsyncEnumerator<ReadRowsResponse> response;
            bool first;
            bool disposed;

            public ReadRowsStream(IAsyncEnumerator<ReadRowsResponse> response)
            {
                try
                {
                    if (response.MoveNextAsync().Result && response.Current != null)
                    {
                        this.schema = ArrowSerializationHelpers.DeserializeSchema(response.Current.ArrowSchema.SerializedSchema.Memory);
                    }
                }
                catch (InvalidOperationException)
                {
                }

                this.response = response;
                this.first = true;
            }

            public Schema Schema => this.schema ?? throw new InvalidOperationException("Stream has no rows");
            public bool HasRows => this.schema != null;

            public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
            {
                if (this.first)
                {
                    this.first = false;
                }
                else if (this.disposed || !await this.response.MoveNextAsync())
                {
                    return null;
                }

                return ArrowSerializationHelpers.DeserializeRecordBatch(this.schema, this.response.Current.ArrowRecordBatch.SerializedRecordBatch.Memory);
            }

            public void Dispose()
            {
                if (!this.disposed)
                {
                    this.response.DisposeAsync().GetAwaiter().GetResult();
                    this.disposed = true;
                }
            }
        }
    }
}
