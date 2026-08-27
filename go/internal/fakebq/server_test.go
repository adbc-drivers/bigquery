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

package fakebq_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/adbc-drivers/bigquery/go/internal/fakebq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestClientInsertAndGetRecordsJobIDAndLocation(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	client.Location = "US"

	q := client.Query("SELECT 1")
	q.JobID = "adbc-job-1"
	q.Location = "US"
	job, err := q.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "adbc-job-1", job.ID())
	require.Equal(t, "US", job.Location())

	inserts := srv.RequestsByKind(fakebq.KindInsert)
	require.Len(t, inserts, 1)
	assert.Equal(t, "test-project", inserts[0].Project)
	assert.Equal(t, "adbc-job-1", inserts[0].JobID)
	assert.Equal(t, "US", inserts[0].Location)
}

func TestGetStatesRunningThenDone(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.ScriptNextJob("RUNNING", "RUNNING", "DONE")

	q := client.Query("SELECT 1")
	q.JobID = "poll-job"
	q.Location = "EU"
	job, err := q.Run(ctx)
	require.NoError(t, err)
	require.False(t, job.LastStatus().Done())

	js, err := job.Status(ctx)
	require.NoError(t, err)
	require.False(t, js.Done())

	js, err = job.Status(ctx)
	require.NoError(t, err)
	require.False(t, js.Done())

	js, err = job.Status(ctx)
	require.NoError(t, err)
	require.True(t, js.Done())

	gets := srv.RequestsByKind(fakebq.KindGet)
	require.Len(t, gets, 3)
	assert.Equal(t, "poll-job", gets[0].JobID)
	assert.Equal(t, "poll-job", gets[1].JobID)
	assert.Equal(t, "EU", gets[0].Location)
	assert.Equal(t, http.MethodGet, gets[0].Method)
	assert.Contains(t, gets[0].Path, "/jobs/poll-job")
	assert.Equal(t, []fakebq.Kind{fakebq.KindInsert, fakebq.KindGet, fakebq.KindGet, fakebq.KindGet}, srv.KindOrder())
}

func TestJobFromIDLocationNoCredentials(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.SetJobStates("from-id", "DONE")
	job, err := client.JobFromIDLocation(ctx, "from-id", "US")
	require.NoError(t, err)
	assert.Equal(t, "from-id", job.ID())
	assert.Equal(t, "US", job.Location())
	assert.True(t, job.LastStatus().Done())

	gets := srv.RequestsByKind(fakebq.KindGet)
	require.Len(t, gets, 1)
	assert.Equal(t, "from-id", gets[0].JobID)
	assert.Equal(t, "US", gets[0].Location)
}

func TestInsertConflict409(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.NextInsertHTTPError(http.StatusConflict, "duplicate", "Already Exists: Job adbc-dup")

	q := client.Query("SELECT 1")
	q.JobID = "adbc-dup"
	_, err = q.Run(ctx)
	require.Error(t, err)
	var apiErr *googleapi.Error
	require.True(t, errors.As(err, &apiErr), "got %T: %v", err, err)
	assert.Equal(t, http.StatusConflict, apiErr.Code)

	inserts := srv.RequestsByKind(fakebq.KindInsert)
	require.Len(t, inserts, 1)
	assert.Equal(t, "adbc-dup", inserts[0].JobID)
	assert.Equal(t, http.StatusConflict, inserts[0].StatusCode)
}

func TestGetRateLimit429AndServerError5xx(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.SetJobStates("err-job", "DONE")
	srv.NextGetHTTPError(http.StatusTooManyRequests, "rateLimitExceeded", "Too many requests")
	_, _ = client.JobFromIDLocation(ctx, "err-job", "US")

	srv.NextGetHTTPError(http.StatusInternalServerError, "backendError", "boom")
	_, _ = client.JobFromIDLocation(ctx, "err-job", "US")

	codes := make([]int, 0)
	for _, req := range srv.RequestsByKind(fakebq.KindGet) {
		codes = append(codes, req.StatusCode)
	}
	assert.Contains(t, codes, http.StatusTooManyRequests)
	assert.Contains(t, codes, http.StatusInternalServerError)
}

func TestCancelRecordsProjectLocationAndJobID(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.SetJobStates("cancel-me", "RUNNING")
	job, err := client.JobFromIDLocation(ctx, "cancel-me", "US")
	require.NoError(t, err)

	require.NoError(t, job.Cancel(ctx))

	cancels := srv.RequestsByKind(fakebq.KindCancel)
	require.Len(t, cancels, 1)
	assert.Equal(t, http.MethodPost, cancels[0].Method)
	assert.Equal(t, "test-project", cancels[0].Project)
	assert.Equal(t, "cancel-me", cancels[0].JobID)
	assert.Equal(t, "US", cancels[0].Location)
	assert.Contains(t, cancels[0].Path, "/jobs/cancel-me/cancel")
}
