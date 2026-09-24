// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigquery

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/apierror"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Helpers to manage BigQuery Jobs and query APIs.

func ipcReaderFromArrowIterator(arrowIterator bigquery.ArrowIterator, schemaEnhancer schemaEnhancer, jobID string, alloc memory.Allocator) (*ipc.Reader, *arrow.Schema, error) {
	arrowItReader := bigquery.NewArrowIteratorReader(arrowIterator)
	rdr, err := ipc.NewReader(arrowItReader, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, nil, err
	}

	fields := make([]arrow.Field, len(arrowIterator.Schema()))
	for i, field := range arrowIterator.Schema() {
		fields[i], err = buildField(field, 0)
		if err != nil {
			return nil, nil, err
		}
	}

	if len(fields) != rdr.Schema().NumFields() {
		// XXX: BigQuery doesn't always populate the schema in responses; if so, fall back to the Arrow schema
		fields = rdr.Schema().Fields()
	}

	metadata := make(map[string]string)
	if schemaEnhancer != nil {
		err = schemaEnhancer.GetMetadata(metadata)
		if err != nil {
			return nil, nil, err
		}
	}
	return rdr, arrow.NewSchema(fields, new(arrow.MetadataFrom(metadata))), nil
}

// runQuery executes the given query and returns an iterator over the results (it may be empty/a trivial iterator if the query did not return a result set).
//
// post-condition: ArrowIterator and schemaEnhancer are both non-nil if err is nil; other fields may be populated if possible
func runQuery(ctx context.Context, logger *slog.Logger, client *bigquery.Client, query *bigquery.Query, executeUpdate bool, st *statement) (bigquery.ArrowIterator, schemaEnhancer, string, int64, error) {
	// Parameterized execution reuses the query struct, including its job ID policy, so reset it
	jobIDConfig := query.JobIDConfig
	defer func() { query.JobIDConfig = jobIDConfig }()

	var job *bigquery.Job
	var enhancer schemaEnhancer
	var err error
	// If we use the optional job creation mode and it returns a job, we can and should skip creating a read session; instead we directly read from the default stream
	var readRowsFastPath bool

	if !query.DryRun && query.JobCreationMode != nil && *query.JobCreationMode == bigquery.JobCreationModeOptional {
		// The behavior here is very muddled; the public API documentation doesn't really actually document much. Reference used instead:
		// https://github.com/googleapis/google-cloud-python/blob/1857302d5c602087b9a782c62694c33013eb77ea/packages/google-cloud-bigquery/google/cloud/bigquery/table.py#L2306
		resp, err := query.TryRead(ctx)
		if err != nil {
			return nil, nil, "", -1, errToAdbcErr(adbc.StatusInternal, err, "read query")
		}

		enhancer = &queryResponseSchemaEnhancer{resp: resp}
		if jr := resp.JobReference; jr != nil {
			// query created a job. we need to materialize the job from the API; the info in the response isn't enough to construct the job object ourselves
			job, err = client.JobFromProject(ctx, jr.ProjectId, jr.JobId, jr.Location)
			if err != nil {
				return nil, nil, "", -1, errToAdbcErr(adbc.StatusInternal, err, "get job from query response")
			}
			// TODO(lidavidm): it is possible to get an inline response here - we could return it to optimize time-to-first-row
			readRowsFastPath = true
		} else if resp != nil {
			if !resp.JobComplete {
				// TODO: is it possible to get here? It would mean no job was created, but the query is incomplete.
				// The Python SDK above does not handle this - it only uses the Storage Read API to fetch remaining results
				return nil, nil, "", -1, adbc.Error{
					Code: adbc.StatusInternal,
					Msg:  "[bq] no job but query is not complete (Google backend error?)",
				}
			} else if resp.ArrowSchema == nil || resp.ArrowRecordBatch == nil {
				// TODO(adbc-drivers/bigquery#280): handle the "struct_encoding" case
				return nil, nil, "", -1, adbc.Error{
					Code: adbc.StatusInternal,
					Msg:  "[bq] no job but query is complete but no results",
				}
			}

			schema := bigquery.BqToSchema(resp.Schema)
			it, err := newInlineArrowIterator(schema, resp.ArrowSchema, resp.ArrowRecordBatch)
			if err != nil {
				return nil, nil, "", -1, errToAdbcErr(adbc.StatusInternal, err, "create inline Arrow iterator")
			}
			return it, enhancer, "", int64(resp.TotalRows), nil
		}
		// no API response => fast path was not attempted, create a job below
	}

	// N.B. this may assign a job ID. We can't do this above as assigning our own job ID disables the fast path
	activeJob := st.beginJob(st.cnxn.client, &query.JobIDConfig)
	defer st.finishJob(ctx, logger, activeJob)

	if job == nil {
		// we may already have a job if the fast path was attempted above
		job, err = query.Run(ctx)
		if err != nil {
			return nil, nil, "", -1, errToAdbcErr(adbc.StatusInternal, err, "run query")
		}
		activeJob.setJob(job)
	}
	jobID := job.ID()

	// The project id, location, and job id are all URL-safe:
	// - Project id and job id can only contain URL-safe characters:
	//   https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
	// - Locations are also URL-safe, listed here:
	//   https://cloud.google.com/bigquery/docs/locations
	jobLink := fmt.Sprintf(
		"https://console.cloud.google.com/bigquery?project=%s&j=bq:%s:%s&page=queryresults",
		job.ProjectID(), job.Location(), job.ID(),
	)
	wrap := func(err error) error {
		if err == nil {
			return err
		}
		if adbcErr, ok := errors.AsType[adbc.Error](err); ok {
			adbcErr.Msg = fmt.Sprintf("%s (Query: %s)", adbcErr.Msg, jobLink)
			return adbcErr
		}
		return fmt.Errorf("%w (Query: %s)", err, jobLink)
	}

	// XXX: Google SDK badness.  We can't use Wait here because queries that
	// *fail* with a rateLimitExceeded (e.g. too many metadata operations)
	// will get the *polling* retried infinitely in Google's SDK (I believe
	// the SDK wants to retry "polling for job status" rate limit exceeded but
	// doesn't differentiate between them because googleapi.CheckResponse
	// appears to put the API error from the response object as an error of
	// the API call, from digging around using a debugger.  In other words, it
	// seems to be confusing "I got an error that my API request was rate
	// limited" and "I got an error that my job was rate limited" because
	// their internal APIs mix both errors into a single error path.)
	js, err := safeWaitForJob(ctx, logger, job)
	if err != nil {
		return nil, nil, jobID, -1, wrap(err)
	}
	activeJob.markFinished()

	if enhancer != nil {
		enhancer = &compositeSchemaEnhancer{enhancers: []schemaEnhancer{enhancer, &jobStatisticsSchemaEnhancer{stats: js.Statistics, jobID: jobID}}}
	} else {
		enhancer = &jobStatisticsSchemaEnhancer{stats: js.Statistics, jobID: jobID}
	}

	if err := js.Err(); err != nil {
		return nil, enhancer, jobID, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "complete job"))
	} else if !js.Done() {
		return nil, enhancer, jobID, -1, wrap(adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[bq] Query job did not complete",
		})
	}

	mayReturnResults := false
	stats, statsOk := js.Statistics.Details.(*bigquery.QueryStatistics)
	if executeUpdate {
		if statsOk {
			return nil, enhancer, jobID, stats.NumDMLAffectedRows, nil
		}
		return nil, enhancer, jobID, -1, nil
	} else if query.DryRun {
		it, err := newDryRunArrowIterator(js.Statistics, jobID)
		if err != nil {
			return nil, enhancer, jobID, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "create dry run Arrow iterator"))
		}
		return it, enhancer, jobID, js.Statistics.TotalBytesProcessed, nil
	} else if statsOk {
		// note that SCRIPT doesn't always have results. we catch this below
		mayReturnResults = stats.StatementType == "SELECT" || stats.StatementType == "CALL" || stats.StatementType == "SCRIPT"
	}

	var arrowIterator bigquery.ArrowIterator
	totalRows := int64(-1)

	if !mayReturnResults && statsOk {
		arrowIterator = emptyArrowIterator{stats.Schema}
		totalRows = stats.NumDMLAffectedRows
	} else if mayReturnResults && readRowsFastPath {
		driverbase.DebugAssert(statsOk, "stats should be available if mayReturnResults is true")
		arrowIterator, err = newReadRowsArrowIterator(ctx, client, job, stats.Schema)
		if err != nil {
			return nil, enhancer, jobID, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "read from default stream"))
		}
		// We don't know total rows on this path.
	} else {
		// XXX: the Google SDK badness also applies here; it makes a similar
		// mistake with the retry, so we wait for the job above.

		// TODO(lidavidm): can we avoid having to read the job if we
		// know the job doesn't return results? Maybe the info we're
		// after is in the statistics already?
		iter, err := job.Read(ctx)
		if err != nil {
			return nil, enhancer, jobID, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "read query results"))
		}

		// We need to detect if this actually returned data. Originally we
		// checked for the presence of a schema, but it turns out statements
		// like CREATE VIEW return a schema! Then we checked if there are
		// rows, but it turns out that bigquery-emulator returns
		// iter.TotalRows == 0 (this is valid as per the API: the field is not
		// _necessarily_ populated until after a call to Next). Finally we use
		// job statistics instead
		if mayReturnResults {
			if arrowIterator, err = iter.ArrowIterator(); err != nil {
				if stats.StatementType == "SCRIPT" && err.Error() == "failed to resolve table for script job: no child jobs found" {
					// Script job with no results
					// N.B. BigQuery SDK doesn't give a structured error - it's a fmt.Errorf
					arrowIterator = emptyArrowIterator{iter.Schema}
				} else if apiErr, ok := errors.AsType[*apierror.APIError](err); ok && apiErr.GRPCStatus() != nil && apiErr.GRPCStatus().Code() == codes.PermissionDenied {
					// Preserve the previous error
					// message. readSessionUser may sound
					// unrelated but creating a "read session" is
					// the first step of using the Storage API.
					return nil, enhancer, jobID, -1, wrap(adbc.Error{
						Code: adbc.StatusUnauthorized,
						Msg:  fmt.Sprintf("[bq] Could not read Arrow query results: (%s) %s (Arrow reader requires roles/bigquery.readSessionUser, see https://github.com/apache/arrow-adbc/issues/3282)", apiErr.GRPCStatus().Code(), apiErr.GRPCStatus().Message()),
					})
				} else {
					return nil, enhancer, jobID, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "read Arrow query results"))
				}
			}
			totalRows = int64(iter.TotalRows)
		} else if statsOk {
			totalRows = stats.NumDMLAffectedRows
		} else {
			arrowIterator = emptyArrowIterator{iter.Schema}
			totalRows = 0
		}
	}
	return arrowIterator, enhancer, jobID, totalRows, nil
}

func runPlainQuery(ctx context.Context, logger *slog.Logger, client *bigquery.Client, query *bigquery.Query, alloc memory.Allocator, resultRecordBufferSize int, st *statement) (bigqueryRdr array.RecordReader, totalRows int64, err error) {
	arrowIterator, schemaEnhancer, jobID, totalRows, err := runQuery(ctx, logger, client, query, false, st)
	if err != nil {
		return nil, -1, err
	}

	rdr, schema, err := ipcReaderFromArrowIterator(arrowIterator, schemaEnhancer, jobID, alloc)
	if err != nil {
		return nil, -1, err
	}

	chs := make([]chan arrow.RecordBatch, 1)
	ctx, cancelFn := context.WithCancel(ctx)
	ch := make(chan arrow.RecordBatch, resultRecordBufferSize)
	chs[0] = ch

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	result := &reader{
		refCount:   1,
		chs:        chs,
		curChIndex: 0,
		err:        nil,
		cancelFn:   cancelFn,
		schema:     schema,
	}
	bigqueryRdr = result

	go streamRecordBatches(ctx, rdr, result, ch)
	return bigqueryRdr, totalRows, nil
}

// streamRecordBatches reads a RecordReader into a channel; it terminates if the supplied context is cancelled.
func streamRecordBatches(ctx context.Context, source array.RecordReader, result *reader, ch chan arrow.RecordBatch) {
	defer close(ch)
	defer source.Release()
	for source.Next() && ctx.Err() == nil {
		rec := source.RecordBatch()
		rec.Retain()
		select {
		case ch <- rec:
		case <-ctx.Done():
			rec.Release()
			result.setError(checkContext(ctx, nil))
			return
		}
	}
	result.setError(checkContext(ctx, source.Err()))
}

type inlineArrowIterator struct {
	bqSchema    bigquery.Schema
	arrowSchema []byte
	arrowBatch  []byte
}

var _ bigquery.ArrowIterator = &inlineArrowIterator{}

func newInlineArrowIterator(bqSchema bigquery.Schema, arrowSchema *bq.ArrowSchema, batch *bq.ArrowRecordBatch) (*inlineArrowIterator, error) {
	schemaBytes, err := base64.StdEncoding.DecodeString(arrowSchema.SerializedSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Arrow schema: %w", err)
	}

	batchBytes, err := base64.StdEncoding.DecodeString(batch.SerializedRecordBatch)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Arrow record batch: %w", err)
	}

	// XXX: strip weird extension types that Google inserts
	schema, err := flight.DeserializeSchema(schemaBytes, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Arrow schema: %w", err)
	}
	fields := make([]arrow.Field, len(schema.Fields()))
	for i, field := range schema.Fields() {
		m := field.Metadata.ToMap()

		if ty := guessBigQueryTypeFromArrowType(field.Type, m); ty != "" {
			m["BIGQUERY:type"] = ty
		}

		if m["ARROW:extension:name"] == "google:sqlType:geography" {
			m["ARROW:extension:name"] = "geoarrow.wkt"
			// TODO: factor this out
			m["ARROW:extension:metadata"] = `{"crs": "EPSG:4326", "crs_type": "authority_code", "edges": "spherical"}`
		} else {
			delete(m, "ARROW:extension:name")
			delete(m, "ARROW:extension:metadata")
		}
		field.Metadata = arrow.MetadataFrom(m)
		fields[i] = field
	}
	schemaBytes = flight.SerializeSchema(arrow.NewSchema(fields, nil), memory.DefaultAllocator)
	// strip the IPC end-of-stream
	schemaBytes = schemaBytes[:len(schemaBytes)-8]

	return &inlineArrowIterator{
		bqSchema:    bqSchema,
		arrowSchema: schemaBytes,
		arrowBatch:  batchBytes,
	}, nil
}

func (it *inlineArrowIterator) Next() (*bigquery.ArrowRecordBatch, error) {
	if it.arrowBatch == nil {
		return nil, iterator.Done
	}

	data := it.arrowBatch
	it.arrowBatch = nil
	return &bigquery.ArrowRecordBatch{
		Data:   data,
		Schema: it.arrowSchema,
	}, nil
}

func (it *inlineArrowIterator) Schema() bigquery.Schema {
	return it.bqSchema
}

func (it *inlineArrowIterator) SerializedArrowSchema() []byte {
	return it.arrowSchema
}

type dryRunArrowIterator struct {
	schema      bigquery.Schema
	arrowSchema []byte
}

var _ bigquery.ArrowIterator = &dryRunArrowIterator{}

func newDryRunArrowIterator(stats *bigquery.JobStatistics, jobID string) (*dryRunArrowIterator, error) {
	md := make(map[string]string)
	err := metadataFromJobStatistics(md, stats, jobID)
	if err != nil {
		return nil, err
	}
	metadata := new(arrow.MetadataFrom(md))

	var arrowSchema *arrow.Schema
	var schema bigquery.Schema
	if stats == nil {
		arrowSchema = arrow.NewSchema([]arrow.Field{}, metadata)
	} else {
		statistics, ok := stats.Details.(*bigquery.QueryStatistics)
		if !ok {
			// No schema, return an empty schema
			arrowSchema = arrow.NewSchema([]arrow.Field{}, metadata)
		} else {
			schema = statistics.Schema
			fields := make([]arrow.Field, len(schema))
			for i, field := range schema {
				var err error
				fields[i], err = buildField(field, 0)
				if err != nil {
					return nil, err
				}
			}
			arrowSchema = arrow.NewSchema(fields, metadata)
		}
	}
	return &dryRunArrowIterator{
		schema:      slices.Clone(schema),
		arrowSchema: flight.SerializeSchema(arrowSchema, memory.DefaultAllocator),
	}, nil
}

func (it *dryRunArrowIterator) Next() (*bigquery.ArrowRecordBatch, error) {
	return nil, iterator.Done
}

func (it *dryRunArrowIterator) Schema() bigquery.Schema {
	return it.schema
}

func (it *dryRunArrowIterator) SerializedArrowSchema() []byte {
	return it.arrowSchema
}

type readRowsArrowIterator struct {
	rows        storagepb.BigQueryRead_ReadRowsClient
	schema      bigquery.Schema
	batchCh     chan batchOrError
	arrowSchema []byte
}

var _ bigquery.ArrowIterator = &readRowsArrowIterator{}

type batchOrError struct {
	batch *bigquery.ArrowRecordBatch
	err   error
}

func newReadRowsArrowIterator(ctx context.Context, client *bigquery.Client, job *bigquery.Job, schema bigquery.Schema) (bigquery.ArrowIterator, error) {
	rc := client.StorageReadClient()
	if rc == nil {
		// TODO(lidavidm): eventually we will need to support disabling this
		return nil, errors.New("storage read client is not initialized")
	}

	msgSizeOpt := gax.WithGRPCOptions(
		// Read API sends up to 128 MiB of data per message; add some padding for the actual Protobuf message etc.
		// https://cloud.google.com/bigquery/quotas#storage-limits
		grpc.MaxCallRecvMsgSize(1024 * 1024 * 129),
	)
	readStream := fmt.Sprintf("projects/%s/locations/%s/jobs/%s/streams/_default", job.ProjectID(), job.Location(), job.ID())
	rows, err := rc.ReadRows(ctx, &storagepb.ReadRowsRequest{
		ReadStream: readStream,
		// TODO(lidavidm): if we get an inline response, we can use it, then set the offset to skip it
		Offset: 0,
		OutputFormatSerializationOptions: &storagepb.ReadRowsRequest_ArrowSerializationOptions{
			ArrowSerializationOptions: &storagepb.ArrowSerializationOptions{
				BufferCompression: storagepb.ArrowSerializationOptions_ZSTD,
				// TODO: hmm, there's a way to get nanos and picos (string) out of BigQuery...
			},
		},
	}, msgSizeOpt)
	if err != nil {
		return nil, err
	}

	// XXX: at some point maybe we ditch the BigQuery interfaces and do this all ourselves...
	schemaCh := make(chan []byte, 1)
	batchCh := make(chan batchOrError, 1)
	go func() {
		defer close(schemaCh)
		defer close(batchCh)

		var schema []byte

		for {
			resp, err := rows.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				batchCh <- batchOrError{nil, err}
				return
			}

			if schema == nil {
				// This should be in the first message
				schema = resp.GetArrowSchema().SerializedSchema
				schemaCh <- resp.GetArrowSchema().SerializedSchema
			}

			batchCh <- batchOrError{&bigquery.ArrowRecordBatch{
				Data:        resp.GetArrowRecordBatch().SerializedRecordBatch,
				Schema:      schema,
				PartitionID: readStream,
			}, nil}
		}
	}()

	arrowSchema, ok := <-schemaCh
	if !ok {
		// TODO: get the real error from batchCh
		return nil, errors.New("failed to receive schema from ReadRows")
	}

	return &readRowsArrowIterator{
		rows:        rows,
		schema:      schema,
		batchCh:     batchCh,
		arrowSchema: arrowSchema,
	}, nil
}

func (it *readRowsArrowIterator) Next() (*bigquery.ArrowRecordBatch, error) {
	batchOrErr, ok := <-it.batchCh
	if !ok {
		return nil, iterator.Done
	}
	if batchOrErr.err != nil {
		return nil, batchOrErr.err
	}
	return batchOrErr.batch, nil
}

func (it *readRowsArrowIterator) Schema() bigquery.Schema {
	return slices.Clone(it.schema)
}

func (it *readRowsArrowIterator) SerializedArrowSchema() []byte {
	return it.arrowSchema
}

func guessBigQueryTypeFromArrowType(dt arrow.DataType, md map[string]string) string {
	// Google doesn't want to return the BigQuery schema, so emulate BIGQUERY:type by guessing it from the Arrow type

	switch md["ARROW:extension:name"] {
	case "google:sqlType:geography":
		return "GEOGRAPHY"
	case "google:sqlType:interval":
		return "INTERVAL"
	}

	switch ty := dt.(type) {
	case *arrow.BinaryType:
		return "BYTES"
	case *arrow.BooleanType:
		return "BOOLEAN"
	case *arrow.Date32Type:
		return "DATE"
	case *arrow.Decimal128Type:
		return "NUMERIC"
	case *arrow.Decimal256Type:
		return "BIGNUMERIC"
	case *arrow.Float64Type:
		return "FLOAT"
	case *arrow.Int64Type:
		return "INTEGER"
	case *arrow.ListType:
		field := ty.ElemField()
		fieldMd := field.Metadata.ToMap()
		return fmt.Sprintf("ARRAY<%s>", guessBigQueryTypeFromArrowType(field.Type, fieldMd))
	case *arrow.StringType:
		return "STRING"
	case *arrow.StructType:
		// XXX: this can't differentiate between a STRUCT with these names and an actual RANGE
		var b strings.Builder
		fields := ty.Fields()

		if len(fields) == 2 && fields[0].Name == "start" && fields[1].Name == "end" && arrow.TypeEqual(fields[0].Type, fields[1].Type) {
			b.WriteString("RANGE<")
			b.WriteString(guessBigQueryTypeFromArrowType(fields[0].Type, fields[0].Metadata.ToMap()))
			b.WriteString(">")
			return b.String()
		}

		b.WriteString("STRUCT<")
		for i, field := range ty.Fields() {
			if i > 0 {
				b.WriteString(", ")
			}
			fieldMd := field.Metadata.ToMap()
			fmt.Fprintf(&b, "%s %s", quoteIdentifier(field.Name), guessBigQueryTypeFromArrowType(field.Type, fieldMd))
		}
		b.WriteString(">")
		return b.String()
	case *arrow.Time32Type:
		return "TIME"
	case *arrow.Time64Type:
		return "TIME"
	case *arrow.TimestampType:
		if ty.TimeZone == "" {
			return "DATETIME"
		} else {
			return "TIMESTAMP"
		}
	}

	return ""
}
