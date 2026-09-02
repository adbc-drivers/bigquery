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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
)

const jobCancelTimeout = 30 * time.Second

type jobCanceler interface {
	Cancel(context.Context) error
}

// jobCancellation identifies a job before jobs.insert starts, so cancellation
// can still find it if BigQuery accepts the insert but its response is lost.
type jobCancellation struct {
	id      string
	resolve func(context.Context) (jobCanceler, error)

	jobMu sync.Mutex
	job   jobCanceler

	finished atomic.Bool
	once     sync.Once
	err      error
}

func newJobCancellation(id string, resolve func(context.Context) (jobCanceler, error)) *jobCancellation {
	return &jobCancellation{id: id, resolve: resolve}
}

func (j *jobCancellation) setJob(job jobCanceler) {
	j.jobMu.Lock()
	j.job = job
	j.jobMu.Unlock()
}

func (j *jobCancellation) currentJob() jobCanceler {
	j.jobMu.Lock()
	defer j.jobMu.Unlock()
	return j.job
}

func (j *jobCancellation) markFinished() { j.finished.Store(true) }

func (j *jobCancellation) cancel(ctx context.Context) error {
	if j.finished.Load() {
		return nil
	}

	j.once.Do(func() {
		j.err = j.cancelJob(ctx)
	})
	return j.err
}

func (j *jobCancellation) cancelJob(ctx context.Context) error {
	if j.finished.Load() {
		return nil
	}

	job := j.currentJob()
	if job == nil {
		if j.resolve == nil {
			return errors.New("BigQuery job is not available for cancellation")
		}

		backoff := gax.Backoff{Initial: 25 * time.Millisecond, Multiplier: 1.5, Max: 250 * time.Millisecond}
		for job == nil {
			if j.finished.Load() {
				return nil
			}
			resolved, err := j.resolve(ctx)
			if err == nil {
				job = resolved
				j.setJob(resolved)
				break
			}
			if !isJobNotFound(err) {
				return err
			}
			if err := gax.Sleep(ctx, backoff.Pause()); err != nil {
				return err
			}
		}
	}

	if j.finished.Load() {
		return nil
	}
	return job.Cancel(ctx)
}

func isJobNotFound(err error) bool {
	apiErr, ok := errors.AsType[*googleapi.Error](err)
	return ok && apiErr.Code == 404
}

type statementExecution struct {
	cancel context.CancelFunc
}

func (st *statement) beginExecution(ctx context.Context) (context.Context, *statementExecution) {
	ctx, cancel := context.WithCancel(ctx)
	op := &statementExecution{cancel: cancel}

	st.cancelMu.Lock()
	previous := st.execution
	st.execution = op
	st.cancelMu.Unlock()
	if previous != nil {
		previous.cancel()
	}
	return ctx, op
}

func (st *statement) endExecution(op *statementExecution) {
	op.cancel()
	st.cancelMu.Lock()
	if st.execution == op {
		st.execution = nil
	}
	st.cancelMu.Unlock()
}

func (st *statement) beginJob(client *bigquery.Client, config *bigquery.JobIDConfig) *jobCancellation {
	if config.JobID == "" {
		config.JobID = uuid.NewString()
	} else if config.AddJobIDSuffix {
		config.JobID += "-" + uuid.NewString()
	}
	config.AddJobIDSuffix = false
	projectID := config.ProjectID
	if projectID == "" {
		projectID = client.Project()
	}
	location := config.Location
	if location == "" {
		location = client.Location
	}

	jobID := config.JobID
	job := newJobCancellation(jobID, func(ctx context.Context) (jobCanceler, error) {
		return client.JobFromProject(ctx, projectID, jobID, location)
	})
	st.cancelMu.Lock()
	st.activeJob = job
	st.cancelMu.Unlock()
	return job
}

// finishJob clears the active job and asynchronously cancels it if execution
// returned before observing a terminal status.
func (st *statement) finishJob(ctx context.Context, logger *slog.Logger, job *jobCancellation) {
	st.cancelMu.Lock()
	if st.activeJob == job {
		st.activeJob = nil
	}
	st.cancelMu.Unlock()

	if job.finished.Load() {
		return
	}

	go func() {
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jobCancelTimeout)
		defer cancel()
		if err := job.cancel(cancelCtx); err != nil {
			logger.WarnContext(cancelCtx, "failed to cancel job, so it may keep running and billing until it finishes", "id", job.id, "error", err)
			return
		}
		logger.DebugContext(cancelCtx, "cancelled job", "id", job.id)
	}()
}

// Cancel stops the current statement execution and waits for BigQuery to
// accept the jobs.cancel request. It is safe to call concurrently with Execute.
func (st *statement) Cancel(ctx context.Context) error {
	st.cancelMu.Lock()
	op := st.execution
	job := st.activeJob
	st.cancelMu.Unlock()

	if op == nil && job == nil {
		return adbc.Error{Code: adbc.StatusInvalidState, Msg: "[bq] no active query to cancel"}
	}
	if op != nil {
		op.cancel()
	}
	if job == nil {
		return nil
	}

	cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jobCancelTimeout)
	defer cancel()
	if err := job.cancel(cancelCtx); err != nil {
		return adbc.Error{
			Code: adbc.StatusUnknown,
			Msg:  fmt.Sprintf("[bq] failed to cancel BigQuery job %s: %s", job.id, err),
		}
	}
	return nil
}

// executionBoundReader keeps a statement cancellable while its result stream
// is live, then releases that state at EOF, error, or final Release.
type executionBoundReader struct {
	inner  array.RecordReader
	refs   atomic.Int64
	done   sync.Once
	finish func()
}

func bindExecutionReader(inner array.RecordReader, finish func()) array.RecordReader {
	r := &executionBoundReader{inner: inner, finish: finish}
	r.refs.Store(1)
	return r
}

func (r *executionBoundReader) finishOnce() {
	r.done.Do(r.finish)
}

func (r *executionBoundReader) Retain() {
	r.refs.Add(1)
	r.inner.Retain()
}

func (r *executionBoundReader) Release() {
	r.inner.Release()
	if r.refs.Add(-1) == 0 {
		r.finishOnce()
	}
}

func (r *executionBoundReader) Schema() *arrow.Schema { return r.inner.Schema() }

func (r *executionBoundReader) Next() bool {
	if r.inner.Next() {
		return true
	}
	r.finishOnce()
	return false
}

func (r *executionBoundReader) RecordBatch() arrow.RecordBatch { return r.inner.RecordBatch() }
func (r *executionBoundReader) Record() arrow.RecordBatch      { return r.RecordBatch() }
func (r *executionBoundReader) Err() error                     { return r.inner.Err() }

var _ array.RecordReader = (*executionBoundReader)(nil)
