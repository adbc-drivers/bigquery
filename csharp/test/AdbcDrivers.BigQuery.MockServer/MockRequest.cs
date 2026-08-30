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

namespace AdbcDrivers.BigQuery.MockServer
{
    /// <summary>
    /// The kind of REST request recorded by <see cref="BigQueryMockServer"/>.
    /// </summary>
    public enum MockRequestKind
    {
        /// <summary>POST projects/{projectId}/jobs</summary>
        JobInsert,

        /// <summary>GET projects/{projectId}/jobs/{jobId}</summary>
        JobGet,

        /// <summary>POST projects/{projectId}/jobs/{jobId}/cancel</summary>
        JobCancel,

        /// <summary>GET projects/{projectId}/queries/{jobId}</summary>
        QueryResults,

        /// <summary>GET projects/{projectId}/datasets/{datasetId}/tables/{tableId}</summary>
        TableGet,

        /// <summary>POST projects/{projectId}/datasets/{datasetId}/tables</summary>
        TableInsert,

        /// <summary>DELETE projects/{projectId}/datasets/{datasetId}/tables/{tableId}</summary>
        TableDelete,
    }

    /// <summary>
    /// A single REST request observed by <see cref="BigQueryMockServer"/>, recorded in arrival order
    /// so tests can assert on the sequence of RPCs the driver issued.
    /// </summary>
    public sealed class MockRequest
    {
        internal MockRequest(MockRequestKind kind, string? jobId, int sequence)
        {
            Kind = kind;
            JobId = jobId;
            Sequence = sequence;
            TimestampUtc = DateTimeOffset.UtcNow;
        }

        /// <summary>The kind of request.</summary>
        public MockRequestKind Kind { get; }

        /// <summary>
        /// The job the request targeted, or null for requests that are not job-scoped (table
        /// operations) and for a job insert that failed before a job was created. A successful
        /// job insert carries the id the mock assigned to the new job.
        /// </summary>
        public string? JobId { get; }

        /// <summary>The zero-based arrival order of this request across all recorded kinds.</summary>
        public int Sequence { get; }

        /// <summary>When the request was recorded.</summary>
        public DateTimeOffset TimestampUtc { get; }

        public override string ToString() =>
            JobId == null ? $"{Sequence}:{Kind}" : $"{Sequence}:{Kind}({JobId})";
    }
}
