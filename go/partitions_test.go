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
	"net"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestPartitionDescriptorRoundTrip(t *testing.T) {
	streamName := "projects/p/locations/us/sessions/s1/streams/stream-0"
	descriptor := encodePartitionDescriptor(streamName)

	got, err := decodePartitionDescriptor(descriptor)
	require.NoError(t, err)
	require.Equal(t, streamName, got)

	// The descriptor carries only the stream name and the version byte.
	require.Len(t, descriptor, 1+len(streamName))
}

func TestPartitionDescriptorMalformedRefused(t *testing.T) {
	for name, descriptor := range map[string][]byte{
		"nil":             nil,
		"empty":           {},
		"version only":    {partitionDescriptorV1},
		"unknown version": append([]byte{partitionDescriptorV1 + 1}, "stream"...),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodePartitionDescriptor(descriptor)
			var adbcErr adbc.Error
			require.ErrorAs(t, err, &adbcErr)
			require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, adbcErr.Msg)

			// ReadPartition refuses the same descriptors before it touches the client.
			_, err = (&connectionImpl{}).ReadPartition(context.Background(), descriptor)
			require.ErrorAs(t, err, &adbcErr)
			require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, adbcErr.Msg)
		})
	}
}

func TestExecutePartitionsRefusals(t *testing.T) {
	params, err := array.NewRecordReader(arrow.NewSchema(nil, nil), nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		st   *statement
		want adbc.Status
	}{
		{"empty query", &statement{}, adbc.StatusInvalidState},
		{
			"dry run",
			&statement{queryConfig: bigquery.QueryConfig{Q: "SELECT 1", DryRun: true}},
			adbc.StatusInvalidState,
		},
		{
			"bound parameters",
			&statement{queryConfig: bigquery.QueryConfig{Q: "SELECT 1"}, params: params},
			adbc.StatusNotImplemented,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, partitions, rows, err := tc.st.ExecutePartitions(context.Background())
			var adbcErr adbc.Error
			require.ErrorAs(t, err, &adbcErr)
			require.Equal(t, tc.want, adbcErr.Code, adbcErr.Msg)
			require.Zero(t, partitions.NumPartitions)
			require.Empty(t, partitions.PartitionIDs)
			require.EqualValues(t, -1, rows)
		})
	}
}

func TestPartitionStreamCountFor(t *testing.T) {
	for _, tc := range []struct {
		name                string
		rows                uint64
		maxStreams          int32
		targetRowsPerStream int64
		want                int32
	}{
		{"zero rows still asks for one stream", 0, 4, 1_000_000, 1},
		{"rows exactly fill one stream", 1_000_000, 4, 1_000_000, 1},
		{"one row over spills into a second stream", 1_000_001, 4, 1_000_000, 2},
		{"a wanted count above max is capped at max", 10_000_000, 4, 1_000_000, 4},
		{"rows per stream unset always asks for max", 10_000_000, 4, 0, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, partitionStreamCountFor(tc.rows, tc.maxStreams, tc.targetRowsPerStream))
		})
	}
}

func TestPartitionOptionsRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := &statement{}

	// Unset: max streams reads back as its default, rows per stream as zero.
	got, err := st.GetOptionInt(ctx, OptionQueryPartitionsMaxStreams)
	require.NoError(t, err)
	require.EqualValues(t, defaultPartitionMaxStreams, got)
	got, err = st.GetOptionInt(ctx, OptionQueryPartitionsTargetRowsPerStream)
	require.NoError(t, err)
	require.Zero(t, got)

	require.NoError(t, st.SetOption(ctx, OptionQueryPartitionsMaxStreams, "7"))
	require.NoError(t, st.SetOptionInt(ctx, OptionQueryPartitionsTargetRowsPerStream, 50000))

	str, err := st.GetOption(ctx, OptionQueryPartitionsMaxStreams)
	require.NoError(t, err)
	require.Equal(t, "7", str)
	str, err = st.GetOption(ctx, OptionQueryPartitionsTargetRowsPerStream)
	require.NoError(t, err)
	require.Equal(t, "50000", str)
	require.EqualValues(t, 7, st.partitionMaxStreams)
	require.EqualValues(t, 50000, st.partitionTargetRowsPerStream)
}

func TestPartitionOptionsInvalidRefused(t *testing.T) {
	ctx := context.Background()
	const pastInt32 = 1 << 31

	for _, key := range []string{OptionQueryPartitionsMaxStreams, OptionQueryPartitionsTargetRowsPerStream} {
		for _, v := range []string{"0", "-1", "not-a-number", "1.5", ""} {
			t.Run(key+"="+v, func(t *testing.T) {
				st := &statement{}
				var adbcErr adbc.Error
				require.ErrorAs(t, st.SetOption(ctx, key, v), &adbcErr)
				require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, adbcErr.Msg)
			})
		}
		t.Run(key+" int", func(t *testing.T) {
			for _, n := range []int64{0, -1} {
				st := &statement{}
				var adbcErr adbc.Error
				require.ErrorAs(t, st.SetOptionInt(ctx, key, n), &adbcErr)
				require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, adbcErr.Msg)
			}
		})
	}

	// The Storage Read API's MaxStreamCount is an int32: a value one past that range
	// is refused, not wrapped into a negative stream count.
	st := &statement{}
	var adbcErr adbc.Error
	require.ErrorAs(t, st.SetOptionInt(ctx, OptionQueryPartitionsMaxStreams, pastInt32), &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, adbcErr.Msg)
	require.Zero(t, st.partitionMaxStreams)

	// Rows per stream is int64-wide and takes it.
	require.NoError(t, st.SetOptionInt(ctx, OptionQueryPartitionsTargetRowsPerStream, pastInt32))
	require.EqualValues(t, pastInt32, st.partitionTargetRowsPerStream)
}

// fakeStorageRead stands in for the Storage Read API on loopback. It records the
// one request each call receives and refuses it, so a test can read what the
// driver sent without serving a session.
type fakeStorageRead struct {
	storagepb.UnimplementedBigQueryReadServer
	sessions chan *storagepb.CreateReadSessionRequest
	reads    chan *storagepb.ReadRowsRequest
}

func (f *fakeStorageRead) CreateReadSession(_ context.Context, req *storagepb.CreateReadSessionRequest) (*storagepb.ReadSession, error) {
	f.sessions <- req
	return nil, status.Error(codes.InvalidArgument, "fake: recorded")
}

func (f *fakeStorageRead) ReadRows(req *storagepb.ReadRowsRequest, _ storagepb.BigQueryRead_ReadRowsServer) error {
	f.reads <- req
	return status.Error(codes.InvalidArgument, "fake: recorded")
}

func clientOfFakeStorageRead(t *testing.T) (*bigquery.Client, *fakeStorageRead) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	fake := &fakeStorageRead{
		sessions: make(chan *storagepb.CreateReadSessionRequest, 1),
		reads:    make(chan *storagepb.ReadRowsRequest, 1),
	}
	srv := grpc.NewServer()
	storagepb.RegisterBigQueryReadServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "p", option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.EnableStorageReadClient(ctx, option.WithGRPCConn(conn), option.WithoutAuthentication()))
	return client, fake
}

func recorded[T any](t *testing.T, ch chan T) T {
	t.Helper()
	select {
	case req := <-ch:
		return req
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the fake Storage Read API received no request")
		panic("unreachable")
	}
}

// BigQuery refuses per-request serialization options on a session stream with
// InvalidArgument ("Arrow serialization options are only supported for job
// streams"), so ReadPartition must send the stream name and nothing else.
func TestPartitionReadSendsNoRequestSerializationOptions(t *testing.T) {
	client, fake := clientOfFakeStorageRead(t)
	streamName := "projects/p/locations/us/sessions/s1/streams/stream-0"

	_, err := readPartitionRecordReader(context.Background(), client, streamName, memory.DefaultAllocator, 1)
	require.Error(t, err, "the fake refuses every read")

	req := recorded(t, fake.reads)
	require.Equal(t, streamName, req.GetReadStream())
	require.Nil(t, req.GetOutputFormatSerializationOptions())
}

// The session, not the read, carries the compression. Its preferred minimum
// equals its maximum, so a small table is still asked to split that far.
func TestPartitionReadSessionCompressesAndAsksForAMinimum(t *testing.T) {
	client, fake := clientOfFakeStorageRead(t)

	_, _, err := createPartitionReadSession(context.Background(), client, client.Dataset("d").Table("t"), 2)
	require.Error(t, err, "the fake refuses every session")

	req := recorded(t, fake.sessions)
	require.Equal(t, "projects/p/datasets/d/tables/t", req.GetReadSession().GetTable())
	require.Equal(t,
		storagepb.ArrowSerializationOptions_ZSTD,
		req.GetReadSession().GetReadOptions().GetArrowSerializationOptions().GetBufferCompression())
	require.EqualValues(t, 2, req.GetMaxStreamCount())
	require.EqualValues(t, 2, req.GetPreferredMinStreamCount())
}
