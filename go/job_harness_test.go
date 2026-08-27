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
	"testing"

	"github.com/adbc-drivers/bigquery/go/internal/fakebq"
	"github.com/stretchr/testify/require"
)

func TestSafeWaitForJobPollsRunningThenDone(t *testing.T) {
	srv := fakebq.New(t)
	ctx := context.Background()
	client, err := srv.Client(ctx, "test-project")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	srv.ScriptNextJob("RUNNING", "RUNNING", "DONE")

	q := client.Query("SELECT 1")
	q.JobID = "wait-job"
	q.Location = "US"
	job, err := q.Run(ctx)
	require.NoError(t, err)

	js, err := safeWaitForJob(ctx, slog.Default(), job)
	require.NoError(t, err)
	require.True(t, js.Done())

	gets := srv.RequestsByKind(fakebq.KindGet)
	require.GreaterOrEqual(t, len(gets), 2)
	require.Equal(t, "wait-job", gets[0].JobID)
	require.Equal(t, "US", gets[0].Location)
	require.Equal(t, fakebq.KindInsert, srv.KindOrder()[0])
}
