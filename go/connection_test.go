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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/googleapi"
)

// fakeTableGetter stands in for tables.get: each call takes a second (of
// synctest's fake time), and the getter records how many calls it got and the
// most it had in flight at once. A table named "skip…" yields no info, and
// "fail…" an error.
type fakeTableGetter struct {
	mu          sync.Mutex
	calls       int
	inFlight    int
	maxInFlight int
}

var errFakeGet = errors.New("fake tables.get failure")

func (f *fakeTableGetter) get(_ context.Context, table *bigquery.Table) (*driverbase.TableInfo, error) {
	f.mu.Lock()
	f.calls++
	f.inFlight++
	f.maxInFlight = max(f.maxInFlight, f.inFlight)
	f.mu.Unlock()
	time.Sleep(time.Second)
	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()

	switch {
	case strings.HasPrefix(table.TableID, "skip"):
		return nil, nil
	case strings.HasPrefix(table.TableID, "fail"):
		return nil, errFakeGet
	}
	return &driverbase.TableInfo{TableName: table.TableID}, nil
}

func tablesNamed(names ...string) []*bigquery.Table {
	tables := make([]*bigquery.Table, len(names))
	for i, name := range names {
		tables[i] = &bigquery.Table{ProjectID: "p", DatasetID: "d", TableID: name}
	}
	return tables
}

func tableNames(infos []driverbase.TableInfo) []string {
	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.TableName
	}
	return names
}

func TestGetTableInfosKeepsOrderAndRunsConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var f fakeTableGetter
		tables := tablesNamed(
			"t0", "skip0", "t1", "t2", "t3", "t4", "skip1", "t5",
			"t6", "t7", "t8", "skip2", "t9", "t10", "t11", "t12",
			"t13", "t14", "skip3", "t15", "t16", "t17", "t18", "t19",
		)

		start := time.Now()
		infos, err := getTableInfos(t.Context(), semaphore.NewWeighted(tableMetadataConcurrency), "p", "d", tables, f.get)
		require.NoError(t, err)
		require.Equal(t, []string{
			"t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9",
			"t10", "t11", "t12", "t13", "t14", "t15", "t16", "t17", "t18", "t19",
		}, tableNames(infos))
		require.Equal(t, 24, f.calls)
		require.Equal(t, tableMetadataConcurrency, f.maxInFlight)
		// 24 one-second calls, 8 at a time.
		require.Equal(t, 3*time.Second, time.Since(start))
	})
}

// Listings on one connection, such as the datasets GetObjects lists in
// parallel, share the connection's bound on calls in flight.
func TestGetTableInfosSharesTheConnectionLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var f fakeTableGetter
		limit := semaphore.NewWeighted(tableMetadataConcurrency)
		g, ctx := errgroup.WithContext(t.Context())
		for range 3 {
			g.Go(func() error {
				_, err := getTableInfos(ctx, limit, "p", "d", tablesNamed("t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11"), f.get)
				return err
			})
		}
		require.NoError(t, g.Wait())
		require.Equal(t, 36, f.calls)
		require.Equal(t, tableMetadataConcurrency, f.maxInFlight)
	})
}

func TestGetTableInfosReturnsTheFirstError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var f fakeTableGetter
		infos, err := getTableInfos(t.Context(), semaphore.NewWeighted(tableMetadataConcurrency), "p", "d", tablesNamed("t0", "fail0", "t1"), f.get)
		require.Nil(t, infos)
		require.ErrorIs(t, err, errFakeGet)
	})
}

func TestGetTableInfosStopsWhenCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var f fakeTableGetter
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		infos, err := getTableInfos(ctx, semaphore.NewWeighted(tableMetadataConcurrency), "p", "d", tablesNamed("t0", "t1", "t2"), f.get)
		require.Nil(t, infos)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr)
		require.Equal(t, adbc.StatusCancelled, adbcErr.Code)
		require.Zero(t, f.calls)
	})
}

// A listing waiting for the connection's permits starts no call, and returns
// once the caller cancels.
func TestGetTableInfosStopsWhenCancelledWhileWaitingForAPermit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var f fakeTableGetter
		limit := semaphore.NewWeighted(tableMetadataConcurrency)
		require.NoError(t, limit.Acquire(t.Context(), tableMetadataConcurrency))
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := getTableInfos(ctx, limit, "p", "d", tablesNamed("t0", "t1", "t2"), f.get)
			done <- err
		}()

		synctest.Wait()
		require.Zero(t, f.calls)
		cancel()

		err := <-done
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr)
		require.Equal(t, adbc.StatusCancelled, adbcErr.Code)
		require.Zero(t, f.calls)
	})
}

func TestGetTableInfosOfNoTables(t *testing.T) {
	var f fakeTableGetter
	infos, err := getTableInfos(t.Context(), semaphore.NewWeighted(tableMetadataConcurrency), "p", "d", nil, f.get)
	require.NoError(t, err)
	require.Empty(t, infos)
}

func TestIsInaccessibleTable(t *testing.T) {
	apiError := func(code int, reasons ...string) error {
		err := &googleapi.Error{Code: code}
		for _, reason := range reasons {
			err.Errors = append(err.Errors, googleapi.ErrorItem{Reason: reason})
		}
		return err
	}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"another user's query results", apiError(http.StatusForbidden, "accessDenied"), true},
		{"wrapped access denied", fmt.Errorf("tables.get: %w", apiError(http.StatusForbidden, "accessDenied")), true},
		{"deleted after tables.list", apiError(http.StatusNotFound, "notFound"), true},
		{"quota exceeded", apiError(http.StatusForbidden, "quotaExceeded"), false},
		{"rate limit exceeded", apiError(http.StatusForbidden, "rateLimitExceeded"), false},
		{"403 without a reason", apiError(http.StatusForbidden), false},
		{"bad request", apiError(http.StatusBadRequest, "invalid"), false},
		{"not an API error", errors.New("connection reset"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isInaccessibleTable(tc.err))
		})
	}
}

func TestIsQueryResultTable(t *testing.T) {
	const resultDataset = "_0123456789abcdef0123456789abcdef01234567"
	for _, tc := range []struct {
		dataset, table string
		want           bool
	}{
		// The table names BigQuery gives cached query results.
		{resultDataset, "anon0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", true},
		{resultDataset, "anon3d4e5f6a_0698_44af_9c1b_2d3e4f5a6b7c", true},
		{resultDataset, "anonev_aB3xYz", true},
		{resultDataset, "orders", false},
		// A user's hidden datasets, and datasets of other lengths or letters.
		{"_staging", "anon_audit", false},
		{"_0123456789abcdef0123456789abcdef0123456", "anon0a1b", false},
		{"_0123456789abcdef0123456789abcdef012345678", "anon0a1b", false},
		{"_0123456789ABCDEF0123456789ABCDEF01234567", "anon0a1b", false},
		{"sales", "anon_audit", false},
	} {
		t.Run(tc.dataset+"."+tc.table, func(t *testing.T) {
			require.Equal(t, tc.want, isQueryResultTable(tc.dataset, tc.table))
		})
	}
}

func TestSkipOptionsRoundTripAndValidate(t *testing.T) {
	ctx := context.Background()
	db := &databaseImpl{}
	c := &connectionImpl{}
	for _, key := range []string{OptionSkipHiddenDatasets, OptionSkipQueryResultTables} {
		for _, opts := range []interface {
			SetOption(context.Context, string, string) error
			GetOption(context.Context, string) (string, error)
		}{db, c} {
			for _, value := range []string{adbc.OptionValueEnabled, adbc.OptionValueDisabled} {
				require.NoError(t, opts.SetOption(ctx, key, value))
				got, err := opts.GetOption(ctx, key)
				require.NoError(t, err)
				require.Equal(t, value, got)
			}

			err := opts.SetOption(ctx, key, "yes")
			var adbcErr adbc.Error
			require.ErrorAs(t, err, &adbcErr)
			require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
			require.Equal(t, fmt.Sprintf(`[bq] invalid value for %s: "yes" (expected true or false)`, key), adbcErr.Msg)
		}
	}
}
