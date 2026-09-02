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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	cloudbigquery "cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

type recordingJobCanceler struct {
	mu        sync.Mutex
	calls     int
	err       error
	block     chan struct{}
	called    chan struct{}
	usableCtx bool
}

func newRecordingJobCanceler(err error) *recordingJobCanceler {
	return &recordingJobCanceler{err: err, called: make(chan struct{}, 1)}
}

func (c *recordingJobCanceler) Cancel(ctx context.Context) error {
	c.mu.Lock()
	c.calls++
	c.usableCtx = ctx.Err() == nil
	block := c.block
	c.mu.Unlock()
	select {
	case c.called <- struct{}{}:
	default:
	}
	if block != nil {
		<-block
	}
	return c.err
}

func (c *recordingJobCanceler) observed() (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.usableCtx
}

func testJobCancellation(canceler jobCanceler) *jobCancellation {
	job := newJobCancellation("test-job", nil)
	job.setJob(canceler)
	return job
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func requireStatus(t *testing.T, err error, status adbc.Status) {
	t.Helper()
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, status, adbcErr.Code)
}

func awaitCancel(t *testing.T, canceler *recordingJobCanceler) {
	t.Helper()
	select {
	case <-canceler.called:
	case <-time.After(10 * time.Second):
		t.Fatal("the job was never cancelled")
	}
}

func TestFinishJobCancelsUnfinishedJobWithoutBlocking(t *testing.T) {
	canceler := newRecordingJobCanceler(nil)
	canceler.block = make(chan struct{})
	job := testJobCancellation(canceler)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	returned := make(chan struct{})
	go func() {
		(&statement{activeJob: job}).finishJob(ctx, quietLogger(), job)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("finishing execution waited for jobs.cancel")
	}
	awaitCancel(t, canceler)
	close(canceler.block)
	require.NoError(t, job.cancel(context.Background()))

	calls, usableCtx := canceler.observed()
	require.Equal(t, 1, calls)
	require.True(t, usableCtx, "jobs.cancel inherited the cancelled query context")
}

func TestFinishJobDoesNotCancelTerminalJob(t *testing.T) {
	canceler := newRecordingJobCanceler(nil)
	job := testJobCancellation(canceler)
	job.markFinished()
	st := &statement{activeJob: job}

	st.finishJob(context.Background(), quietLogger(), job)

	calls, _ := canceler.observed()
	require.Zero(t, calls)
	require.Nil(t, st.activeJob)
}

func TestStatementCancelReturnsInvalidStateWithoutActiveQuery(t *testing.T) {
	err := (&statement{}).Cancel(context.Background())
	requireStatus(t, err, adbc.StatusInvalidState)
}

func TestStatementCancelReturnsUnknownWhenJobCancelFails(t *testing.T) {
	canceler := newRecordingJobCanceler(errors.New("cancel refused"))
	st := &statement{}
	ctx, execution := st.beginExecution(context.Background())
	job := testJobCancellation(canceler)
	st.activeJob = job

	err := st.Cancel(context.Background())
	requireStatus(t, err, adbc.StatusUnknown)
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	st.endExecution(execution)
}

func TestStatementCancelDoesNotCancelTerminalJob(t *testing.T) {
	canceler := newRecordingJobCanceler(nil)
	job := testJobCancellation(canceler)
	job.markFinished()
	st := &statement{}
	_, execution := st.beginExecution(context.Background())
	st.activeJob = job

	require.NoError(t, st.Cancel(context.Background()))
	calls, _ := canceler.observed()
	require.Zero(t, calls)

	st.finishJob(context.Background(), quietLogger(), job)
	st.endExecution(execution)
}

func TestStreamRecordBatchesReportsCancellation(t *testing.T) {
	schema := arrow.NewSchema(nil, nil)
	source, err := array.NewRecordReader(schema, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := &reader{refCount: 1, cancelFn: func() {}, schema: schema}
	ch := make(chan arrow.RecordBatch)

	streamRecordBatches(ctx, source, result, ch)

	_, open := <-ch
	require.False(t, open)
	requireStatus(t, result.Err(), adbc.StatusCancelled)
}

func TestExecutionBoundReaderFinishesAtEOF(t *testing.T) {
	inner, err := array.NewRecordReader(arrow.NewSchema(nil, nil), nil)
	require.NoError(t, err)
	finished := make(chan struct{})
	rdr := bindExecutionReader(inner, func() { close(finished) })

	require.False(t, rdr.Next())
	select {
	case <-finished:
	default:
		t.Fatal("execution remained active after EOF")
	}
	rdr.Release()
}

func TestExecutionBoundReaderFinishesAtRelease(t *testing.T) {
	inner, err := array.NewRecordReader(arrow.NewSchema(nil, nil), nil)
	require.NoError(t, err)
	finished := make(chan struct{})
	rdr := bindExecutionReader(inner, func() { close(finished) })

	rdr.Release()
	select {
	case <-finished:
	default:
		t.Fatal("execution remained active after reader release")
	}
}

type acceptedJobServer struct {
	t                  *testing.T
	server             *httptest.Server
	projectID          string
	location           string
	dropInsertResponse bool
	accepted           chan string
	cancelled          chan string
}

func newAcceptedJobServer(t *testing.T) *acceptedJobServer {
	s := &acceptedJobServer{
		t:         t,
		projectID: "test-project",
		location:  "us-west1",
		accepted:  make(chan string, 2),
		cancelled: make(chan string, 1),
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

func (s *acceptedJobServer) close() { s.server.Close() }

func (s *acceptedJobServer) client() (*cloudbigquery.Client, error) {
	client, err := cloudbigquery.NewClient(
		context.Background(),
		s.projectID,
		option.WithEndpoint(s.server.URL+"/bigquery/v2/"),
		option.WithHTTPClient(s.server.Client()),
		option.WithoutAuthentication(),
	)
	if err == nil {
		client.Location = s.location
	}
	return client, err
}

func (s *acceptedJobServer) jobResource(jobID, state string) string {
	return fmt.Sprintf(`{"configuration":{"query":{"query":"SELECT 1","useLegacySql":false}},"jobReference":{"projectId":%q,"location":%q,"jobId":%q},"statistics":{"query":{"statementType":"UPDATE","numDmlAffectedRows":"0"}},"status":{"state":%q}}`, s.projectID, s.location, jobID, state)
}

func (s *acceptedJobServer) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/bigquery/v2/projects/"+s.projectID+"/jobs")
	if r.Method == http.MethodPost && path == "" {
		var body struct {
			JobReference struct {
				JobID string `json:"jobId"`
			} `json:"jobReference"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.t.Errorf("decode jobs.insert body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.accepted <- body.JobReference.JobID
		if s.dropInsertResponse {
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, s.jobResource(body.JobReference.JobID, "DONE"))
		return
	}

	jobID := strings.TrimPrefix(strings.TrimSuffix(path, "/cancel"), "/")
	if r.Method == http.MethodGet {
		state := "DONE"
		if s.dropInsertResponse {
			state = "RUNNING"
		}
		_, _ = io.WriteString(w, s.jobResource(jobID, state))
		return
	}
	if r.Method == http.MethodPost && strings.HasSuffix(path, "/cancel") {
		s.cancelled <- jobID
		_, _ = fmt.Fprintf(w, `{"job":%s}`, s.jobResource(jobID, "DONE"))
		return
	}
	http.NotFound(w, r)
}

func TestStatementCancelFindsJobWhenInsertResponseIsLost(t *testing.T) {
	srv := newAcceptedJobServer(t)
	defer srv.close()
	srv.dropInsertResponse = true
	client, err := srv.client()
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	st := &statement{
		cnxn:        &connectionImpl{client: client, ConnectionImplBase: connectionBaseForCancellationTest()},
		queryConfig: cloudbigquery.QueryConfig{Q: "SELECT 1"},
	}
	execErr := make(chan error, 1)
	go func() {
		_, err := st.ExecuteUpdate(context.Background())
		execErr <- err
	}()

	var jobID string
	select {
	case jobID = <-srv.accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("jobs.insert was not accepted")
	}
	require.NotEmpty(t, jobID)
	require.NoError(t, st.Cancel(context.Background()))

	select {
	case cancelledID := <-srv.cancelled:
		require.Equal(t, jobID, cancelledID)
	case <-time.After(10 * time.Second):
		t.Fatal("jobs.cancel was not sent")
	}
	select {
	case err := <-execErr:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled execution did not return")
	}
}

func TestRunQueryUsesFreshGeneratedJobID(t *testing.T) {
	srv := newAcceptedJobServer(t)
	defer srv.close()
	client, err := srv.client()
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	st := &statement{cnxn: &connectionImpl{client: client, ConnectionImplBase: connectionBaseForCancellationTest()}}
	query := client.Query("UPDATE test SET value = 1")
	configured := query.JobIDConfig
	ids := make([]string, 2)
	for i := range ids {
		_, _, ids[i], _, err = runQuery(context.Background(), quietLogger(), query, true, st)
		require.NoError(t, err)
		require.Equal(t, configured, query.JobIDConfig)
	}
	require.NotEqual(t, ids[0], ids[1])
}

func connectionBaseForCancellationTest() driverbase.ConnectionImplBase {
	return driverbase.ConnectionImplBase{Logger: quietLogger()}
}
