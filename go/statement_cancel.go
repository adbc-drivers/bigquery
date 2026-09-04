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
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const jobCancelTimeout = 10 * time.Second

// inFlight is one BigQuery job the statement may still cancel.
type inFlight struct {
	job      *bigquery.Job
	once     sync.Once
	launched bool
}

type execOp struct {
	cancel context.CancelFunc
}

func (st *statement) beginExec(ctx context.Context) (context.Context, *execOp) {
	ctx, cancel := context.WithCancel(ctx)
	op := &execOp{cancel: cancel}
	st.cancelMu.Lock()
	prev := st.execOp
	st.execOp = op
	st.cancelMu.Unlock()
	if prev != nil {
		prev.cancel()
	}
	return ctx, op
}

func (st *statement) releaseExec(op *execOp) {
	op.cancel()
	st.cancelMu.Lock()
	if st.execOp == op {
		st.execOp = nil
	}
	st.cancelMu.Unlock()
}

func (st *statement) beginJob(job *bigquery.Job) {
	st.cancelMu.Lock()
	defer st.cancelMu.Unlock()
	st.inFlight = &inFlight{job: job}
}

func (st *statement) endJob(job *bigquery.Job) {
	st.cancelMu.Lock()
	defer st.cancelMu.Unlock()
	if st.inFlight != nil && st.inFlight.job == job {
		st.inFlight = nil
	}
}

func (st *statement) loggerLocked() *slog.Logger {
	if st.cnxn != nil {
		return st.cnxn.Logger
	}
	return nil
}

// Cancel stops the current statement execution and best-effort cancels the
// in-flight BigQuery job. It is safe to call concurrently with Execute.
func (st *statement) Cancel(ctx context.Context) error {
	st.stopExecution(false)
	return nil
}

func (st *statement) stopExecution(wait bool) {
	st.cancelMu.Lock()
	op := st.execOp
	flight := st.inFlight
	launch := false
	if flight != nil && !flight.launched {
		flight.launched = true
		launch = true
	}
	logger := st.loggerLocked()
	st.cancelMu.Unlock()

	if op != nil {
		op.cancel()
	}
	if flight == nil {
		return
	}
	if wait {
		cancelBigQueryJobOnce(flight, logger)
		return
	}
	if launch {
		go cancelBigQueryJobOnce(flight, logger)
	}
}

// Tracks in-flight jobs for cancel; delegates polling to safeWaitForJob (util.go).
func (st *statement) waitForJob(ctx context.Context, logger *slog.Logger, job *bigquery.Job) (*bigquery.JobStatus, error) {
	if st != nil {
		st.beginJob(job)
		defer st.endJob(job)
	}
	js, err := safeWaitForJob(ctx, logger, job)
	if ctx.Err() != nil {
		if st != nil {
			st.stopExecution(false)
		} else {
			go cancelBigQueryJob(job, logger)
		}
	}
	return js, err
}

func cancelBigQueryJobOnce(flight *inFlight, logger *slog.Logger) {
	if flight == nil {
		return
	}
	flight.once.Do(func() {
		cancelBigQueryJob(flight.job, logger)
	})
}

func cancelBigQueryJob(job *bigquery.Job, logger *slog.Logger) {
	if job == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), jobCancelTimeout)
	defer cancel()
	if err := job.Cancel(ctx); err != nil && logger != nil {
		logger.DebugContext(ctx, "best-effort BigQuery job cancel failed", "id", job.ID(), "error", err)
	}
}

// execBoundReader releases the statement's execOp when the result reader is
// fully released, without cancelling the stream while it is still in use.
type execBoundReader struct {
	inner     array.RecordReader
	refs      atomic.Int64
	onRelease func()
}

func bindExecReader(inner array.RecordReader, onRelease func()) array.RecordReader {
	r := &execBoundReader{inner: inner, onRelease: onRelease}
	r.refs.Store(1)
	return r
}

func (r *execBoundReader) Retain() {
	r.refs.Add(1)
	r.inner.Retain()
}

func (r *execBoundReader) Release() {
	if r.refs.Add(-1) == 0 {
		r.inner.Release()
		if r.onRelease != nil {
			r.onRelease()
		}
		return
	}
	r.inner.Release()
}

func (r *execBoundReader) Schema() *arrow.Schema          { return r.inner.Schema() }
func (r *execBoundReader) Next() bool                     { return r.inner.Next() }
func (r *execBoundReader) RecordBatch() arrow.RecordBatch { return r.inner.RecordBatch() }
func (r *execBoundReader) Record() arrow.RecordBatch      { return r.inner.Record() }
func (r *execBoundReader) Err() error                     { return r.inner.Err() }

var _ array.RecordReader = (*execBoundReader)(nil)
