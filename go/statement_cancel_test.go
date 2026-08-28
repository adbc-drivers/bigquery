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
	"log/slog"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/bigquery/go/internal/fakebq"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newHarnessStatement(t *testing.T, client *bigquery.Client, project string) *statement {
	t.Helper()
	client.Location = "US"
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	logger := slog.New(slog.DiscardHandler)
	return &statement{
		alloc: alloc,
		cnxn: &connectionImpl{
			ConnectionImplBase: driverbase.ConnectionImplBase{
				Alloc:  alloc,
				Logger: logger,
			},
			catalog:  project,
			dbSchema: "dataset",
			client:   client,
		},
		parameterMode:          OptionValueQueryParameterModePositional,
		resultRecordBufferSize: 1,
		prefetchConcurrency:    1,
		ingest:                 driverbase.NewBulkIngestOptions(),
		queryConfig: bigquery.QueryConfig{
			DefaultProjectID: project,
			DefaultDatasetID: "dataset",
			Q:                "SELECT 1",
		},
	}
}

func waitForKind(t *testing.T, srv *fakebq.Server, kind fakebq.Kind, n int) []fakebq.Request {
	t.Helper()
	var got []fakebq.Request
	require.Eventually(t, func() bool {
		got = srv.RequestsByKind(kind)
		return len(got) >= n
	}, 3*time.Second, 5*time.Millisecond, "timed out waiting for %d %s, kinds=%v", n, kind, srv.KindOrder())
	return got
}

func requireCancelled(t *testing.T, err error) {
	t.Helper()
	var adbcErr adbc.Error
	require.True(t, errors.As(err, &adbcErr), "got %T: %v", err, err)
	assert.Equal(t, adbc.StatusCancelled, adbcErr.Code)
}

func requireOneCancel(t *testing.T, srv *fakebq.Server, jobID string) fakebq.Request {
	t.Helper()
	cancels := waitForKind(t, srv, fakebq.KindCancel, 1)
	require.Never(t, func() bool {
		return len(srv.RequestsByKind(fakebq.KindCancel)) > 1
	}, 200*time.Millisecond, 10*time.Millisecond, "expected one jobs.cancel, kinds=%v", srv.KindOrder())
	assert.Equal(t, jobID, cancels[0].JobID)
	return cancels[0]
}

func waitExecErr(t *testing.T, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("execute did not return after cancel")
		return nil
	}
}

func TestStatementCancelSendsJobsCancelForInFlightJob(t *testing.T) {
	srv := fakebq.New(t)
	st := newHarnessStatement(t, srv.MustClient(t, "test-project"), "test-project")
	srv.SetDefaultStates("RUNNING")

	errCh := make(chan error, 1)
	go func() {
		_, err := st.ExecuteUpdate(context.Background())
		errCh <- err
	}()

	inserts := waitForKind(t, srv, fakebq.KindInsert, 1)
	jobID := inserts[0].JobID
	assert.NotEmpty(t, jobID)
	assert.Equal(t, "US", inserts[0].Location)
	assert.Equal(t, "test-project", inserts[0].Project)

	require.NoError(t, st.Cancel(context.Background()))
	requireCancelled(t, waitExecErr(t, errCh))

	cancel := requireOneCancel(t, srv, jobID)
	assert.Equal(t, "US", cancel.Location)
	assert.Equal(t, "test-project", cancel.Project)
	assert.Contains(t, cancel.Path, "/jobs/"+jobID+"/cancel")
}

func TestStatementCancelOnCompletedJobIsSafe(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client := srv.MustClient(t, "test-project")
	client.Location = "US"

	srv.SetJobStates("done-job", "DONE")
	job, err := client.JobFromIDLocation(ctx, "done-job", "US")
	require.NoError(t, err)
	require.True(t, job.LastStatus().Done())

	st := newHarnessStatement(t, client, "test-project")
	st.beginJob(job)
	require.NoError(t, st.Cancel(ctx))
	st.endJob(job)

	cancels := waitForKind(t, srv, fakebq.KindCancel, 1)
	assert.Equal(t, "done-job", cancels[0].JobID)
	assert.Equal(t, "US", cancels[0].Location)
}

func TestStatementCancelDoesNotCancelPreviousExecution(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	st := newHarnessStatement(t, srv.MustClient(t, "test-project"), "test-project")

	srv.ScriptNextJob("DONE")
	srv.ScriptNextJob("RUNNING")

	n, err := st.ExecuteUpdate(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	firstID := srv.RequestsByKind(fakebq.KindInsert)[0].JobID

	errCh := make(chan error, 1)
	go func() {
		_, err := st.ExecuteUpdate(ctx)
		errCh <- err
	}()

	inserts := waitForKind(t, srv, fakebq.KindInsert, 2)
	secondID := inserts[1].JobID
	require.NotEqual(t, firstID, secondID)

	require.NoError(t, st.Cancel(context.Background()))
	requireCancelled(t, waitExecErr(t, errCh))

	requireOneCancel(t, srv, secondID)
}

func TestExecuteContextCancelStillSendsJobsCancel(t *testing.T) {
	srv := fakebq.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := newHarnessStatement(t, srv.MustClient(t, "test-project"), "test-project")
	srv.SetDefaultStates("RUNNING")

	errCh := make(chan error, 1)
	go func() {
		_, err := st.ExecuteUpdate(ctx)
		errCh <- err
	}()

	inserts := waitForKind(t, srv, fakebq.KindInsert, 1)
	cancel()
	requireCancelled(t, waitExecErr(t, errCh))

	got := requireOneCancel(t, srv, inserts[0].JobID)
	assert.Equal(t, "US", got.Location)
}

func TestStatementCancelSendsJobsCancelForExecuteQuery(t *testing.T) {
	srv := fakebq.New(t)
	st := newHarnessStatement(t, srv.MustClient(t, "test-project"), "test-project")
	srv.SetDefaultStates("RUNNING")

	errCh := make(chan error, 1)
	go func() {
		_, _, err := st.ExecuteQuery(context.Background())
		errCh <- err
	}()

	inserts := waitForKind(t, srv, fakebq.KindInsert, 1)
	require.NoError(t, st.Cancel(context.Background()))
	requireCancelled(t, waitExecErr(t, errCh))

	requireOneCancel(t, srv, inserts[0].JobID)
}

func TestExecuteQueryErrorIsNotCancelledForCompletedJob(t *testing.T) {
	srv := fakebq.New(t)
	st := newHarnessStatement(t, srv.MustClient(t, "test-project"), "test-project")
	srv.SetDefaultStates("DONE")

	_, _, err := st.ExecuteQuery(context.Background())
	if err == nil {
		return
	}
	var adbcErr adbc.Error
	if errors.As(err, &adbcErr) {
		assert.NotEqual(t, adbc.StatusCancelled, adbcErr.Code, "completed ExecuteQuery must not cancel its own result context: %v", err)
	}
}

func TestStatementCloseCancelsInFlightJob(t *testing.T) {
	srv := fakebq.New(t)
	st := newHarnessStatement(t, srv.MustClient(t, "test-project"), "test-project")
	srv.SetDefaultStates("RUNNING")

	errCh := make(chan error, 1)
	go func() {
		_, err := st.ExecuteUpdate(context.Background())
		errCh <- err
	}()

	inserts := waitForKind(t, srv, fakebq.KindInsert, 1)
	require.NoError(t, st.Close(context.Background()))
	requireCancelled(t, waitExecErr(t, errCh))

	requireOneCancel(t, srv, inserts[0].JobID)
}

func TestExecuteQueryReleaseClearsExecOp(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	st := newHarnessStatement(t, srv.MustClient(t, "test-project"), "test-project")
	require.NoError(t, st.SetOption(ctx, OptionQueryDryRun, "true"))

	rr, _, err := st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.NotNil(t, rr)
	require.NotNil(t, st.execOp)

	rr.Retain()
	rr.Release()
	require.NotNil(t, st.execOp, "Retain keeps the execute context alive")
	rr.Release()
	require.Nil(t, st.execOp)
	require.NoError(t, st.Cancel(ctx))
}
