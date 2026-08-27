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

func TestStatementCancelSendsJobsCancelForInFlightJob(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.SetDefaultStates("RUNNING")
	st := newHarnessStatement(t, client, "test-project")

	errCh := make(chan error, 1)
	go func() {
		_, err := st.ExecuteUpdate(ctx)
		errCh <- err
	}()

	inserts := waitForKind(t, srv, fakebq.KindInsert, 1)
	jobID := inserts[0].JobID
	assert.NotEmpty(t, jobID)
	assert.Equal(t, "US", inserts[0].Location)
	assert.Equal(t, "test-project", inserts[0].Project)

	require.NoError(t, st.Cancel(context.Background()))

	select {
	case err := <-errCh:
		var adbcErr adbc.Error
		require.True(t, errors.As(err, &adbcErr), "got %T: %v", err, err)
		assert.Equal(t, adbc.StatusCancelled, adbcErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("ExecuteUpdate did not return after Cancel")
	}

	cancels := waitForKind(t, srv, fakebq.KindCancel, 1)
	assert.Equal(t, jobID, cancels[0].JobID)
	assert.Equal(t, "US", cancels[0].Location)
	assert.Equal(t, "test-project", cancels[0].Project)
	assert.Contains(t, cancels[0].Path, "/jobs/"+jobID+"/cancel")
}

func TestStatementCancelOnCompletedJobIsSafe(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

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
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.ScriptNextJob("DONE")
	srv.ScriptNextJob("RUNNING")

	st := newHarnessStatement(t, client, "test-project")
	n, err := st.ExecuteUpdate(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	firstID := srv.RequestsByKind(fakebq.KindInsert)[0].JobID

	errCh := make(chan error, 1)
	go func() {
		_, err := st.ExecuteUpdate(ctx)
		errCh <- err
	}()

	require.Eventually(t, func() bool {
		return len(srv.RequestsByKind(fakebq.KindInsert)) >= 2
	}, 3*time.Second, 5*time.Millisecond)
	secondID := srv.RequestsByKind(fakebq.KindInsert)[1].JobID
	require.NotEqual(t, firstID, secondID)

	require.NoError(t, st.Cancel(context.Background()))

	select {
	case err := <-errCh:
		var adbcErr adbc.Error
		require.True(t, errors.As(err, &adbcErr), "got %T: %v", err, err)
		assert.Equal(t, adbc.StatusCancelled, adbcErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("second ExecuteUpdate did not return after Cancel")
	}

	cancels := waitForKind(t, srv, fakebq.KindCancel, 1)
	for _, req := range cancels {
		assert.Equal(t, secondID, req.JobID, "must not cancel the completed first job")
	}
}

func TestExecuteContextCancelStillSendsJobsCancel(t *testing.T) {
	srv := fakebq.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.SetDefaultStates("RUNNING")
	st := newHarnessStatement(t, client, "test-project")

	errCh := make(chan error, 1)
	go func() {
		_, err := st.ExecuteUpdate(ctx)
		errCh <- err
	}()

	inserts := waitForKind(t, srv, fakebq.KindInsert, 1)
	cancel()

	select {
	case err := <-errCh:
		var adbcErr adbc.Error
		require.True(t, errors.As(err, &adbcErr), "got %T: %v", err, err)
		assert.Equal(t, adbc.StatusCancelled, adbcErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("ExecuteUpdate did not return after context cancel")
	}

	cancels := waitForKind(t, srv, fakebq.KindCancel, 1)
	assert.Equal(t, inserts[0].JobID, cancels[0].JobID)
	assert.Equal(t, "US", cancels[0].Location)
}

func TestStatementCancelSendsJobsCancelForExecuteQuery(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.SetDefaultStates("RUNNING")
	st := newHarnessStatement(t, client, "test-project")

	errCh := make(chan error, 1)
	go func() {
		_, _, err := st.ExecuteQuery(ctx)
		errCh <- err
	}()

	inserts := waitForKind(t, srv, fakebq.KindInsert, 1)
	require.NoError(t, st.Cancel(context.Background()))

	select {
	case err := <-errCh:
		var adbcErr adbc.Error
		require.True(t, errors.As(err, &adbcErr), "got %T: %v", err, err)
		assert.Equal(t, adbc.StatusCancelled, adbcErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("ExecuteQuery did not return after Cancel")
	}

	cancels := waitForKind(t, srv, fakebq.KindCancel, 1)
	assert.Equal(t, inserts[0].JobID, cancels[0].JobID)
}

func TestExecuteQueryErrorIsNotCancelledForCompletedJob(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.SetDefaultStates("DONE")
	st := newHarnessStatement(t, client, "test-project")

	_, _, err = st.ExecuteQuery(ctx)
	if err == nil {
		return
	}
	var adbcErr adbc.Error
	if errors.As(err, &adbcErr) {
		assert.NotEqual(t, adbc.StatusCancelled, adbcErr.Code, "completed ExecuteQuery must not cancel its own result context: %v", err)
	}
}
