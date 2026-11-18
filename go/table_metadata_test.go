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
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/option"
)

// tablesGetServer answers tables.get for project "p", dataset "d". A table
// named "denied…" gets a 403 accessDenied, "quota…" a 403 quotaExceeded,
// "gone…" a 404, "bad…" a 400, "view…" is a VIEW, and anything else is a
// TABLE. Every answer takes delay, and the server
// counts the calls it got and the most it had in flight at once.
type tablesGetServer struct {
	server *httptest.Server
	delay  time.Duration

	mu          sync.Mutex
	requests    int
	inFlight    int
	maxInFlight int
}

func newTablesGetServer(t *testing.T, delay time.Duration) *tablesGetServer {
	s := &tablesGetServer{delay: delay}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.server.Close)
	return s
}

func (s *tablesGetServer) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests++
	s.inFlight++
	s.maxInFlight = max(s.maxInFlight, s.inFlight)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()
	time.Sleep(s.delay)

	w.Header().Set("Content-Type", "application/json")
	tableID, ok := strings.CutPrefix(r.URL.Path, "/bigquery/v2/projects/p/datasets/d/tables/")
	if r.Method != http.MethodGet || !ok {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	switch {
	case strings.HasPrefix(tableID, "denied"):
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, `{"error":{"code":403,"message":"Access Denied: User does not have permission to access results of another user's job.","errors":[{"reason":"accessDenied"}]}}`)
	case strings.HasPrefix(tableID, "quota"):
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, `{"error":{"code":403,"message":"Quota exceeded: Your user exceeded quota for concurrent api requests","errors":[{"reason":"quotaExceeded"}]}}`)
	case strings.HasPrefix(tableID, "gone"):
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `{"error":{"code":404,"message":"Not found","errors":[{"reason":"notFound"}]}}`)
	case strings.HasPrefix(tableID, "bad"):
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"error":{"code":400,"message":"Invalid table","errors":[{"reason":"invalid"}]}}`)
	default:
		tableType := "TABLE"
		if strings.HasPrefix(tableID, "view") {
			tableType = "VIEW"
		}
		_, _ = fmt.Fprintf(w, `{"id":"p:d.%s","tableReference":{"projectId":"p","datasetId":"d","tableId":%q},"type":%q}`, tableID, tableID, tableType)
	}
}

func (s *tablesGetServer) tables(t *testing.T, ids ...string) []*bigquery.Table {
	client, err := bigquery.NewClient(
		context.Background(),
		"p",
		option.WithEndpoint(s.server.URL+"/bigquery/v2/"),
		option.WithHTTPClient(s.server.Client()),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	tables := make([]*bigquery.Table, len(ids))
	for i, id := range ids {
		tables[i] = client.DatasetInProject("p", "d").Table(id)
	}
	return tables
}

// listTables runs getTableInfos and renders each result as "name type".
func listTables(ctx context.Context, limit *semaphore.Weighted, tables []*bigquery.Table) ([]string, error) {
	infos, err := getTableInfos(ctx, limit, "p", "d", tables, func(table *bigquery.Table, md *bigquery.TableMetadata) (driverbase.TableInfo, error) {
		return driverbase.TableInfo{TableName: table.TableID, TableType: string(md.Type)}, nil
	})
	if err != nil {
		return nil, err
	}
	got := make([]string, len(infos))
	for i, info := range infos {
		got[i] = info.TableName + " " + info.TableType
	}
	return got, nil
}

func TestGetTableInfosKeepsOrderAndSkipsInaccessibleTables(t *testing.T) {
	srv := newTablesGetServer(t, 20*time.Millisecond)
	tables := srv.tables(t,
		"t0", "denied0", "view0", "t1", "gone0", "t2", "t3", "view1",
		"denied1", "t4", "t5", "t6", "gone1", "t7", "view2", "t8",
		"t9", "denied2", "t10", "t11", "t12", "t13", "t14", "t15",
	)

	got, err := listTables(context.Background(), semaphore.NewWeighted(tableMetadataConcurrency), tables)
	require.NoError(t, err)
	require.Equal(t, []string{
		"t0 TABLE", "view0 VIEW", "t1 TABLE", "t2 TABLE", "t3 TABLE", "view1 VIEW",
		"t4 TABLE", "t5 TABLE", "t6 TABLE", "t7 TABLE", "view2 VIEW", "t8 TABLE",
		"t9 TABLE", "t10 TABLE", "t11 TABLE", "t12 TABLE", "t13 TABLE", "t14 TABLE", "t15 TABLE",
	}, got)

	// 24 calls of 20ms each: with no concurrency at most one is in flight.
	require.Greater(t, srv.maxInFlight, 1, "tables.get calls ran one at a time")
	require.LessOrEqual(t, srv.maxInFlight, tableMetadataConcurrency)
}

// Two listings on one connection, such as two datasets GetObjects lists in
// parallel, share the connection's bound on tables.get calls in flight.
func TestGetTableInfosSharesTheConnectionLimit(t *testing.T) {
	srv := newTablesGetServer(t, 20*time.Millisecond)
	limit := semaphore.NewWeighted(tableMetadataConcurrency)
	var g errgroup.Group
	for range 3 {
		tables := srv.tables(t, "t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11")
		g.Go(func() error {
			_, err := listTables(context.Background(), limit, tables)
			return err
		})
	}
	require.NoError(t, g.Wait())
	require.Equal(t, 36, srv.requests)
	require.LessOrEqual(t, srv.maxInFlight, tableMetadataConcurrency)
}

// A listing waiting for the connection's permits parks one goroutine, not a
// worker per table, so many datasets waiting at once stay cheap.
func TestGetTableInfosWaitsForAPermitBeforeStartingAWorker(t *testing.T) {
	srv := newTablesGetServer(t, 0)
	limit := semaphore.NewWeighted(tableMetadataConcurrency)
	require.NoError(t, limit.Acquire(context.Background(), tableMetadataConcurrency))
	tables := srv.tables(t, "t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11")

	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := listTables(ctx, limit, tables)
		done <- err
	}()
	// Nothing signals that a goroutine has parked, so give any worker the
	// listing would start time to start, and read the peak.
	parked := 0
	for range 20 {
		time.Sleep(5 * time.Millisecond)
		parked = max(parked, runtime.NumGoroutine()-before)
	}
	cancel()

	err := <-done
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusCancelled, adbcErr.Code)
	require.Zero(t, srv.requests)
	require.LessOrEqual(t, parked, 1, "goroutines parked while waiting for a permit")
}

// A 403 for an exceeded quota is not a table the caller may not read, so it
// fails the listing instead of leaving out a readable table.
func TestGetTableInfosReturnsQuotaErrors(t *testing.T) {
	srv := newTablesGetServer(t, 0)

	_, err := listTables(context.Background(), nil, srv.tables(t, "t0", "quota0", "t1"))
	require.ErrorContains(t, err, "get table metadata for p.d.quota0")
	require.ErrorContains(t, err, "Quota exceeded")
}

func TestGetTableInfosReturnsOtherErrors(t *testing.T) {
	srv := newTablesGetServer(t, 0)

	_, err := listTables(context.Background(), nil, srv.tables(t, "t0", "bad0", "t1"))
	require.ErrorContains(t, err, "get table metadata for p.d.bad0")
	require.ErrorContains(t, err, "Invalid table")
}

func TestGetTableInfosStopsWhenCancelled(t *testing.T) {
	srv := newTablesGetServer(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := listTables(ctx, semaphore.NewWeighted(tableMetadataConcurrency), srv.tables(t, "t0", "t1", "t2"))
	require.Nil(t, got)
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusCancelled, adbcErr.Code)
	require.Zero(t, srv.requests, "a cancelled call still sent tables.get")
}

func TestGetTableInfosOfNoTables(t *testing.T) {
	got, err := listTables(context.Background(), nil, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}
