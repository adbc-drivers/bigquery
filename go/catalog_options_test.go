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
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/option"
)

const anonDataset = "_0123456789abcdef0123456789abcdef01234567"

// catalogServer serves datasets.list, tables.list and tables.get for project
// "p" from a fixed set of datasets, and records which tables got a tables.get.
type catalogServer struct {
	server   *httptest.Server
	datasets map[string][]string

	mu  sync.Mutex
	got []string
}

func newCatalogServer(t *testing.T) *catalogServer {
	s := &catalogServer{datasets: map[string][]string{
		"sales":     {"orders", "anon_audit"},
		"_staging":  {"orders", "anonymous_users"},
		anonDataset: {"anon0a1b2c", "anon3d4e5f_0698_44af", "anonev_aB3"},
	}}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.server.Close)
	return s
}

func (s *catalogServer) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.Split(strings.TrimPrefix(r.URL.Path, "/bigquery/v2/projects/p/"), "/")
	enc := json.NewEncoder(w)
	switch {
	case len(path) == 1 && path[0] == "datasets":
		var list []map[string]any
		for name := range s.datasets {
			if strings.HasPrefix(name, "_") && r.URL.Query().Get("all") != "true" {
				continue
			}
			list = append(list, map[string]any{"datasetReference": map[string]string{"projectId": "p", "datasetId": name}})
		}
		_ = enc.Encode(map[string]any{"datasets": list})
	case len(path) == 3 && path[2] == "tables":
		var list []map[string]any
		for _, name := range s.datasets[path[1]] {
			list = append(list, map[string]any{"tableReference": map[string]string{"projectId": "p", "datasetId": path[1], "tableId": name}})
		}
		_ = enc.Encode(map[string]any{"tables": list})
	case len(path) == 4 && path[2] == "tables":
		s.mu.Lock()
		s.got = append(s.got, path[1]+"."+path[3])
		s.mu.Unlock()
		_ = enc.Encode(map[string]any{"type": "TABLE", "tableReference": map[string]string{"projectId": "p", "datasetId": path[1], "tableId": path[3]}})
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func (s *catalogServer) connection(t *testing.T, options map[string]string) *connectionImpl {
	client, err := bigquery.NewClient(
		context.Background(),
		"p",
		option.WithEndpoint(s.server.URL+"/bigquery/v2/"),
		option.WithHTTPClient(s.server.Client()),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	c := &connectionImpl{client: client, tableMetadataLimit: semaphore.NewWeighted(tableMetadataConcurrency)}
	for k, v := range options {
		require.NoError(t, c.SetOption(context.Background(), k, v))
	}
	return c
}

// listCatalog returns "dataset.table" for every table GetObjects would list.
func listCatalog(t *testing.T, c *connectionImpl) []string {
	ctx := context.Background()
	datasets, err := c.GetDBSchemasForCatalog(ctx, "p", nil)
	require.NoError(t, err)
	var got []string
	for _, ds := range datasets {
		tables, err := c.GetTablesForDBSchema(ctx, "p", ds, nil, nil, false)
		require.NoError(t, err)
		for _, table := range tables {
			got = append(got, ds+"."+table.TableName)
		}
	}
	slices.Sort(got)
	return got
}

func TestCatalogListsHiddenAndAnonymousTablesByDefault(t *testing.T) {
	srv := newCatalogServer(t)
	got := listCatalog(t, srv.connection(t, nil))
	all := []string{
		anonDataset + ".anon0a1b2c", anonDataset + ".anon3d4e5f_0698_44af", anonDataset + ".anonev_aB3",
		"_staging.anonymous_users", "_staging.orders",
		"sales.anon_audit", "sales.orders",
	}
	require.Equal(t, all, got)
	slices.Sort(srv.got)
	require.Equal(t, all, srv.got)
}

func TestCatalogSkipHiddenDatasets(t *testing.T) {
	srv := newCatalogServer(t)
	got := listCatalog(t, srv.connection(t, map[string]string{OptionSkipHiddenDatasets: "true"}))
	require.Equal(t, []string{"sales.anon_audit", "sales.orders"}, got)
}

// Skipped anonymous tables get no tables.get, while a user's hidden dataset and
// "anon…" names outside anonymous datasets are kept.
func TestCatalogSkipAnonymousTables(t *testing.T) {
	srv := newCatalogServer(t)
	got := listCatalog(t, srv.connection(t, map[string]string{OptionSkipAnonymousTables: "true"}))
	kept := []string{"_staging.anonymous_users", "_staging.orders", "sales.anon_audit", "sales.orders"}
	require.Equal(t, kept, got)
	slices.Sort(srv.got)
	require.Equal(t, kept, srv.got, "tables.get calls")
}

func TestCatalogSkipOptionsRoundTripAndValidate(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	c := &connectionImpl{}
	for _, key := range []string{OptionSkipHiddenDatasets, OptionSkipAnonymousTables} {
		for _, set := range []func(context.Context, string, string) error{db.SetOption, c.SetOption} {
			require.NoError(t, set(ctx, key, "true"))
			err := set(ctx, key, "yes")
			var adbcErr adbc.Error
			require.ErrorAs(t, err, &adbcErr)
			require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
			require.Contains(t, adbcErr.Msg, key)
		}
		got, err := db.GetOption(ctx, key)
		require.NoError(t, err)
		require.Equal(t, "true", got)
		got, err = c.GetOption(ctx, key)
		require.NoError(t, err)
		require.Equal(t, "true", got)
	}
}
