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
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

func TestGetStatisticsUsesTablesAPI(t *testing.T) {
	const (
		projectID   = "test-project"
		pcntDataset = "my_catalog.my_namespace"
		tableName   = "pcnt_table"
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case fmt.Sprintf("/bigquery/v2/projects/%s/datasets", projectID):
			_, _ = fmt.Fprintf(w, `{
				"datasets": [
					{"datasetReference": {"projectId": %q, "datasetId": %q}}
				]
			}`, projectID, pcntDataset)
		case fmt.Sprintf("/bigquery/v2/projects/%s/datasets/%s/tables", projectID, pcntDataset):
			_, _ = fmt.Fprintf(w, `{
				"tables": [
					{"tableReference": {"projectId": %q, "datasetId": %q, "tableId": %q}, "type": "TABLE"},
					{"tableReference": {"projectId": %q, "datasetId": %q, "tableId": "inaccessible_table"}, "type": "TABLE"}
				]
			}`, projectID, pcntDataset, tableName, projectID, pcntDataset)
		case fmt.Sprintf("/bigquery/v2/projects/%s/datasets/%s/tables/%s", projectID, pcntDataset, tableName):
			_, _ = fmt.Fprintf(w, `{
				"tableReference": {"projectId": %q, "datasetId": %q, "tableId": %q},
				"type": "TABLE",
				"numRows": "300",
				"numBytes": "4096",
				"numLongTermBytes": "1024",
				"lastModifiedTime": "1700000000000"
			}`, projectID, pcntDataset, tableName)
		case fmt.Sprintf("/bigquery/v2/projects/%s/datasets/%s/tables/inaccessible_table", projectID, pcntDataset):
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprint(w, `{"error": {"code": 403, "message": "Access Denied"}}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
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

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	drvBase := driverbase.NewDriverImplBase(driverbase.DefaultDriverInfo("BigQuery"), alloc)
	dbBase, err := driverbase.NewDatabaseImplBase(context.Background(), &drvBase, driverbase.TracingOptions{})
	require.NoError(t, err)

	cnxn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&dbBase),
		catalog:            projectID,
		dbSchema:           pcntDataset,
		client:             client,
	}

	rdr, err := cnxn.GetStatistics(context.Background(), new(projectID), new(pcntDataset), new(tableName), true)
	require.NoError(t, err)
	defer rdr.Release()

	var allStats []testutil.Statistic
	for rdr.Next() {
		rec := rdr.RecordBatch()
		stats := testutil.ExtractStatisticsForTable(rec, projectID, pcntDataset, tableName)
		allStats = append(allStats, stats...)
	}
	require.NoError(t, rdr.Err())
	require.NotEmpty(t, allStats)

	statsMap := testutil.StatisticsToLookupMap(allStats)
	require.Equal(t, float64(300), statsMap[int16(adbc.StatisticRowCountKey)].StatisticValue)
	require.True(t, statsMap[int16(adbc.StatisticRowCountKey)].IsApproximate)
	require.Equal(t, int64(4096), statsMap[StatisticTotalLogicalBytesKey].StatisticValue)
	require.Equal(t, int64(3072), statsMap[StatisticActiveLogicalBytesKey].StatisticValue)
	require.Equal(t, int64(1024), statsMap[StatisticLongTermLogicalBytesKey].StatisticValue)
	require.Equal(t, []byte(time.UnixMilli(1700000000000).UTC().Format(time.RFC3339Nano)), statsMap[StatisticLastModifiedTimeKey].StatisticValue)
	require.False(t, statsMap[StatisticLastModifiedTimeKey].IsApproximate)

	t.Run("non-PCNT dataset still queries INFORMATION_SCHEMA", func(t *testing.T) {
		const regularDataset = "regular_dataset"
		var jobsSubmitted atomic.Int64

		regularSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case fmt.Sprintf("/bigquery/v2/projects/%s/datasets", projectID):
				_, _ = fmt.Fprintf(w, `{
					"datasets": [
						{"datasetReference": {"projectId": %q, "datasetId": %q}}
					]
				}`, projectID, regularDataset)
			case fmt.Sprintf("/bigquery/v2/projects/%s/datasets/%s/tables", projectID, regularDataset):
				_, _ = fmt.Fprintf(w, `{
					"tables": [
						{"tableReference": {"projectId": %q, "datasetId": %q, "tableId": %q}, "type": "TABLE"}
					]
				}`, projectID, regularDataset, tableName)
			case fmt.Sprintf("/bigquery/v2/projects/%s/queries", projectID), fmt.Sprintf("/bigquery/v2/projects/%s/jobs", projectID):
				jobsSubmitted.Add(1)
				w.WriteHeader(http.StatusBadRequest)
				_, _ = fmt.Fprint(w, `{"error": {"code": 400, "message": "expected INFORMATION_SCHEMA query"}}`)
			default:
				t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer regularSrv.Close()

		regularClient, err := bigquery.NewClient(
			context.Background(),
			projectID,
			option.WithEndpoint(regularSrv.URL+"/bigquery/v2/"),
			option.WithHTTPClient(regularSrv.Client()),
			option.WithoutAuthentication(),
		)
		require.NoError(t, err)
		testutil.CheckedClose(t, regularClient)

		regularCnxn := &connectionImpl{
			ConnectionImplBase: driverbase.NewConnectionImplBase(&dbBase),
			catalog:            projectID,
			dbSchema:           regularDataset,
			client:             regularClient,
		}

		_, err = regularCnxn.GetStatistics(context.Background(), new(projectID), new(regularDataset), new(tableName), true)
		require.Error(t, err)
		require.Equal(t, int64(1), jobsSubmitted.Load(), "non-PCNT dataset should submit INFORMATION_SCHEMA query job")
	})
}
