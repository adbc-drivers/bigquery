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
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// tablesGetServer answers tables.get for project "p", dataset "d". A table
// named "denied…" gets a 403, "gone…" a 404, "bad…" a 400, "view…" is a VIEW,
// and anything else is a TABLE. Every answer takes delay, and the server
// records the most calls it had in flight at once.
type tablesGetServer struct {
	server *httptest.Server
	delay  time.Duration

	mu          sync.Mutex
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

func TestGetTablesMetadataKeepsOrderAndSkipsDeniedAndMissingTables(t *testing.T) {
	srv := newTablesGetServer(t, 20*time.Millisecond)
	ids := []string{
		"t0", "denied0", "view0", "t1", "gone0", "t2", "t3", "view1",
		"denied1", "t4", "t5", "t6", "gone1", "t7", "view2", "t8",
		"t9", "denied2", "t10", "t11", "t12", "t13", "t14", "t15",
	}

	metadata, err := getTablesMetadata(context.Background(), "p", "d", srv.tables(t, ids...))
	require.NoError(t, err)

	got := make([]string, len(ids))
	for i, md := range metadata {
		if md == nil {
			got[i] = "<skipped>"
		} else {
			got[i] = md.FullID + " " + string(md.Type)
		}
	}
	require.Equal(t, []string{
		"p:d.t0 TABLE", "<skipped>", "p:d.view0 VIEW", "p:d.t1 TABLE", "<skipped>", "p:d.t2 TABLE", "p:d.t3 TABLE", "p:d.view1 VIEW",
		"<skipped>", "p:d.t4 TABLE", "p:d.t5 TABLE", "p:d.t6 TABLE", "<skipped>", "p:d.t7 TABLE", "p:d.view2 VIEW", "p:d.t8 TABLE",
		"p:d.t9 TABLE", "<skipped>", "p:d.t10 TABLE", "p:d.t11 TABLE", "p:d.t12 TABLE", "p:d.t13 TABLE", "p:d.t14 TABLE", "p:d.t15 TABLE",
	}, got)

	// 24 calls of 20ms each: with no concurrency at most one is in flight.
	require.Greater(t, srv.maxInFlight, 1, "tables.get calls ran one at a time")
	require.LessOrEqual(t, srv.maxInFlight, tableMetadataConcurrency)
}

func TestGetTablesMetadataReturnsOtherErrors(t *testing.T) {
	srv := newTablesGetServer(t, 0)

	_, err := getTablesMetadata(context.Background(), "p", "d", srv.tables(t, "t0", "bad0", "t1"))
	require.ErrorContains(t, err, "get table metadata for p.d.bad0")
	require.ErrorContains(t, err, "Invalid table")
}

func TestGetTablesMetadataOfNoTables(t *testing.T) {
	metadata, err := getTablesMetadata(context.Background(), "p", "d", nil)
	require.NoError(t, err)
	require.Empty(t, metadata)
}
