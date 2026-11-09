// Copyright (c) 2025 ADBC Drivers Contributors
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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestErrToAdbcErrMapsRestForbidden(t *testing.T) {
	for _, tc := range []struct {
		reason string
		want   adbc.Status
	}{
		{"accessDenied", adbc.StatusUnauthorized},
		{"billingNotEnabled", adbc.StatusUnauthorized},
		{"blocked", adbc.StatusUnauthorized},
		{"rateLimitExceeded", adbc.StatusInternal},
		{"quotaExceeded", adbc.StatusInternal},
		{"", adbc.StatusInternal},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			apiErr := &googleapi.Error{Code: http.StatusForbidden}
			if tc.reason != "" {
				apiErr.Errors = []googleapi.ErrorItem{{Reason: tc.reason}}
			}
			var adbcErr adbc.Error
			require.ErrorAs(t, errToAdbcErr(adbc.StatusInternal, apiErr, "run query"), &adbcErr)
			require.Equal(t, tc.want, adbcErr.Code, adbcErr.Msg)
		})
	}
}

// A job that never finishes: safeWaitForJob must back off between polls
// rather than re-requesting the status as fast as the API answers.
func TestSafeWaitForJobBacksOffBetweenPolls(t *testing.T) {
	const (
		projectID = "test-project"
		location  = "us-west1"
		jobID     = "never-finishes"
	)

	var polls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		polls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"configuration":{"query":{"query":"SELECT 1","useLegacySql":false}},"jobReference":{"projectId":%q,"location":%q,"jobId":%q},"status":{"state":"RUNNING"}}`, projectID, location, jobID)
	}))
	defer srv.Close()

	client, err := bigquery.NewClient(
		context.Background(),
		projectID,
		option.WithEndpoint(srv.URL+"/bigquery/v2/"),
		option.WithHTTPClient(srv.Client()),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	testutil.CheckedClose(t, client)

	job, err := client.JobFromIDLocation(context.Background(), jobID, location)
	require.NoError(t, err)

	// Only count the polls safeWaitForJob itself makes.
	polls.Store(0)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err = safeWaitForJob(ctx, logger, job)
	require.Error(t, err, "expected the deadline to end the wait")

	// The backoff starts at 50ms and grows, so a handful of polls fit in the
	// deadline. Without a sleep the loop is bounded only by how fast the
	// server answers, which is hundreds of polls against a local server.
	require.Less(t, polls.Load(), int64(50), "safeWaitForJob polled without backing off")
}

func TestStringToTable(t *testing.T) {
	const (
		defaultProject = "default-proj"
		defaultDataset = "default-ds"
	)

	t.Run("valid formats", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			input       string
			wantProject string
			wantDataset string
			wantTable   string
			wantStr     string
		}{
			{
				name:        "1-part table",
				input:       "my_table",
				wantProject: defaultProject,
				wantDataset: defaultDataset,
				wantTable:   "my_table",
				wantStr:     "default-proj.default-ds.my_table",
			},
			{
				name:        "2-part dataset.table",
				input:       "custom_ds.my_table",
				wantProject: defaultProject,
				wantDataset: "custom_ds",
				wantTable:   "my_table",
				wantStr:     "default-proj.custom_ds.my_table",
			},
			{
				name:        "3-part project.dataset.table",
				input:       "custom_proj.custom_ds.my_table",
				wantProject: "custom_proj",
				wantDataset: "custom_ds",
				wantTable:   "my_table",
				wantStr:     "custom_proj.custom_ds.my_table",
			},
			{
				name:        "4-part PCNT project.catalog.namespace.table",
				input:       "custom_proj.my_catalog.my_namespace.my_table",
				wantProject: "custom_proj",
				wantDataset: "my_catalog.my_namespace",
				wantTable:   "my_table",
				wantStr:     "custom_proj.my_catalog.my_namespace.my_table",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tbl, err := stringToTable(defaultProject, defaultDataset, tc.input)
				require.NoError(t, err)
				require.Equal(t, tc.wantProject, tbl.ProjectID)
				require.Equal(t, tc.wantDataset, tbl.DatasetID)
				require.Equal(t, tc.wantTable, tbl.TableID)
				require.Equal(t, tc.wantStr, tableToString(tbl))
			})
		}
	})

	t.Run("invalid formats", func(t *testing.T) {
		for _, input := range []string{
			"a.b.c.d.e",
			"a..b",
			"a.b..d",
			".a.b",
			"a.b.",
		} {
			t.Run(input, func(t *testing.T) {
				_, err := stringToTable(defaultProject, defaultDataset, input)
				var adbcErr adbc.Error
				require.ErrorAs(t, err, &adbcErr)
				require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
			})
		}
	})
}
