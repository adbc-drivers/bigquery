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
	"log/slog"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/googleapis/gax-go/v2/apierror"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// Helpers to manage BigQuery Jobs and query APIs.

func ipcReaderFromArrowIterator(arrowIterator bigquery.ArrowIterator, jobStatistics *bigquery.JobStatistics, jobID string, alloc memory.Allocator) (*ipc.Reader, *arrow.Schema, error) {
	arrowItReader := bigquery.NewArrowIteratorReader(arrowIterator)
	rdr, err := ipc.NewReader(arrowItReader, ipc.WithAllocator(alloc))

	fields := make([]arrow.Field, len(arrowIterator.Schema()))
	for i, field := range arrowIterator.Schema() {
		fields[i], err = buildField(field, 0)
		if err != nil {
			return nil, nil, err
		}
	}

	if err != nil {
		return nil, nil, err
	}

	metadata, err := metadataFromJobStatistics(jobStatistics, jobID)
	if err != nil {
		return nil, nil, err
	}
	return rdr, arrow.NewSchema(fields, metadata), nil
}

func runQuery(ctx context.Context, logger *slog.Logger, query *bigquery.Query, executeUpdate bool, st *statement) (bigquery.ArrowIterator, *bigquery.JobStatistics, string, int64, error) {
	// Parameterized execution reuses query, including its job ID policy.
	jobIDConfig := query.JobIDConfig
	defer func() { query.JobIDConfig = jobIDConfig }()

	var job *bigquery.Job
	var err error
	if !query.DryRun && query.JobCreationMode != nil && *query.JobCreationMode == bigquery.JobCreationModeOptional {
		// The behavior here is very muddled; the public API
		// documentation doesn't really actually document much.
		// Reference used instead:
		// https://github.com/googleapis/google-cloud-python/blob/1857302d5c602087b9a782c62694c33013eb77ea/packages/google-cloud-bigquery/google/cloud/bigquery/table.py#L2306
		var resp *bq.QueryResponse
		resp, job, _, err := query.TryRead(ctx)
		if err != nil {
			return nil, nil, "", -1, errToAdbcErr(adbc.StatusInternal, err, "read query")
		} else if job != nil {
			// query created a job; fallthrough
		} else if resp != nil {
			if !resp.JobComplete {
				// TODO: handle this case (I think it means we got an inline response and then need to wait for the rest of the results via the regular path - but it would need to be handled above. Is it possible to get here and not have a job? Maybe, if we get a page token instead (but can the page token path return Arrow?))
				return nil, nil, "", -1, adbc.Error{
					Code: adbc.StatusInternal,
					Msg:  "[bq] no job but query is not complete",
				}
			} else if resp.ArrowSchema == nil || resp.ArrowRecordBatch == nil {
				// TODO: handle the "struct_encoding" case
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
			return it, nil, "", int64(resp.TotalRows), nil
		}
		// TODO: there should be new metadata about whether a job was created
		// neither job nor iterator => query was not suitable, create a job below
	}

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

	if err := js.Err(); err != nil {
		return nil, js.Statistics, jobID, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "complete job"))
	} else if !js.Done() {
		return nil, js.Statistics, jobID, -1, wrap(adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[bq] Query job did not complete",
		})
	}

	mayReturnResults := false
	stats, statsOk := js.Statistics.Details.(*bigquery.QueryStatistics)
	if executeUpdate {
		if statsOk {
			return nil, js.Statistics, jobID, stats.NumDMLAffectedRows, nil
		}
		return nil, js.Statistics, jobID, -1, nil
	} else if query.DryRun {
		return nil, js.Statistics, jobID, js.Statistics.TotalBytesProcessed, nil
	} else if statsOk {
		// note that SCRIPT doesn't always have results. we catch this below
		mayReturnResults = stats.StatementType == "SELECT" || stats.StatementType == "CALL" || stats.StatementType == "SCRIPT"
	}

	// XXX: the Google SDK badness also applies here; it makes a similar
	// mistake with the retry, so we wait for the job above.
	iter, err := job.Read(ctx)
	if err != nil {
		return nil, js.Statistics, jobID, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "read query results"))
	}

	var arrowIterator bigquery.ArrowIterator
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
				return nil, js.Statistics, jobID, -1, wrap(adbc.Error{
					Code: adbc.StatusUnauthorized,
					Msg:  fmt.Sprintf("[bq] Could not read Arrow query results: (%s) %s (Arrow reader requires roles/bigquery.readSessionUser, see https://github.com/apache/arrow-adbc/issues/3282)", apiErr.GRPCStatus().Code(), apiErr.GRPCStatus().Message()),
				})
			} else {
				return nil, js.Statistics, jobID, -1, wrap(errToAdbcErr(adbc.StatusInternal, err, "read Arrow query results"))
			}
		}
	} else {
		arrowIterator = emptyArrowIterator{iter.Schema}
	}
	totalRows := int64(iter.TotalRows)
	return arrowIterator, js.Statistics, jobID, totalRows, nil
}

func runPlainQuery(ctx context.Context, logger *slog.Logger, query *bigquery.Query, alloc memory.Allocator, resultRecordBufferSize int, st *statement) (bigqueryRdr array.RecordReader, totalRows int64, err error) {
	arrowIterator, jobStatistics, jobID, totalRows, err := runQuery(ctx, logger, query, false, st)
	if err != nil {
		return nil, -1, err
	} else if query.DryRun || arrowIterator == nil {
		// Dry run queries don't have an arrow iterator, so return an empty reader
		rdr, err := makeDryRunReader(jobStatistics, jobID)
		if err != nil {
			return nil, -1, err
		}
		return rdr, totalRows, nil
	}

	rdr, schema, err := ipcReaderFromArrowIterator(arrowIterator, jobStatistics, jobID, alloc)
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
	schema := it.arrowSchema
	it.arrowSchema = nil
	return &bigquery.ArrowRecordBatch{
		Data:   data,
		Schema: schema,
	}, nil
}

func (it *inlineArrowIterator) Schema() bigquery.Schema {
	return it.bqSchema
}

func (it *inlineArrowIterator) SerializedArrowSchema() []byte {
	return it.arrowSchema
}
