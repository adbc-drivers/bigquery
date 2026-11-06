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
	"math"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc"
)

// partitionDescriptorV1 tags a partition descriptor whose remaining bytes are
// a Storage Read API stream name. It is the only descriptor shape
// ExecutePartitions produces today; the version byte lets a future shape be
// introduced without breaking a descriptor already handed to a caller.
const partitionDescriptorV1 byte = 1

// encodePartitionDescriptor builds the opaque descriptor ExecutePartitions
// returns per partition. It carries only what ReadPartition needs to reopen
// the stream -- the stream name -- never a credential or token.
func encodePartitionDescriptor(streamName string) []byte {
	descriptor := make([]byte, 0, 1+len(streamName))
	descriptor = append(descriptor, partitionDescriptorV1)
	descriptor = append(descriptor, streamName...)
	return descriptor
}

// decodePartitionDescriptor recovers the stream name from a descriptor
// produced by encodePartitionDescriptor. A malformed or unrecognized
// descriptor is reported as adbc.StatusInvalidArgument, never a panic.
func decodePartitionDescriptor(descriptor []byte) (string, error) {
	if len(descriptor) < 1 {
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[bq] empty partition descriptor",
		}
	}
	if descriptor[0] != partitionDescriptorV1 {
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] unsupported partition descriptor version %d", descriptor[0]),
		}
	}
	streamName := string(descriptor[1:])
	if streamName == "" {
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[bq] partition descriptor has no stream name",
		}
	}
	return streamName, nil
}

// defaultPartitionMaxStreams is how many Storage Read API streams ExecutePartitions
// requests when OptionQueryPartitionsMaxStreams is unset.
const defaultPartitionMaxStreams = 4

// maxPartitionStreams is OptionQueryPartitionsMaxStreams, or its default when unset.
func (st *statement) maxPartitionStreams() int32 {
	if st.partitionMaxStreams == 0 {
		return defaultPartitionMaxStreams
	}
	return st.partitionMaxStreams
}

// setPartitionOption stores one of the two partition sizing options. Both must be
// positive. OptionQueryPartitionsMaxStreams must also fit the int32 that the Storage
// Read API's MaxStreamCount field has, so a larger value is refused instead of wrapping.
func (st *statement) setPartitionOption(key string, n int64) error {
	limit := int64(math.MaxInt64)
	if key == OptionQueryPartitionsMaxStreams {
		limit = math.MaxInt32
	}
	if n <= 0 || n > limit {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] `%s` must be a positive integer of at most %d, got %d", key, limit, n),
		}
	}
	if key == OptionQueryPartitionsMaxStreams {
		st.partitionMaxStreams = int32(n)
	} else {
		st.partitionTargetRowsPerStream = n
	}
	return nil
}

// partitionStreamCountFor decides how many streams ExecutePartitions asks for, from
// the two statement options (maxStreams and targetRowsPerStream) and the result's
// exact row count.
//
// targetRowsPerStream <= 0 means unset, and the answer is always maxStreams. Set, the
// row count is divided into chunks of that size, rounding up and asking for at least
// one stream, and the answer is capped at maxStreams.
//
// This is what is asked for, not what is returned: the Storage Read API may return
// fewer streams than requested, never more.
func partitionStreamCountFor(rows uint64, maxStreams int32, targetRowsPerStream int64) int32 {
	if maxStreams < 1 {
		maxStreams = 1
	}
	if targetRowsPerStream <= 0 {
		return maxStreams
	}
	perStream := uint64(targetRowsPerStream)
	wanted := (rows + perStream - 1) / perStream
	if wanted < 1 {
		wanted = 1
	}
	if wanted > uint64(maxStreams) {
		return maxStreams
	}
	return int32(wanted)
}

// createPartitionReadSession opens a multi-stream Storage Read API session
// against a query's destination table and returns the Arrow schema and the
// resulting streams' resource names, one per partition. It asks for exactly
// maxStreamCount streams, as both bounds; BigQuery's preferred minimum is best
// effort, so the session may still hold fewer. The session, not the read, carries the
// Arrow buffer compression: BigQuery refuses per-request serialization options on a
// session stream.
func createPartitionReadSession(ctx context.Context, client *bigquery.Client, dst *bigquery.Table, maxStreamCount int32) (*arrow.Schema, []string, error) {
	rc := client.StorageReadClient()
	if rc == nil {
		return nil, nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[bq] storage read client is not initialized",
		}
	}

	tablePath := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", dst.ProjectID, dst.DatasetID, dst.TableID)
	msgSizeOpt := gax.WithGRPCOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 129))

	readSession, err := rc.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", dst.ProjectID),
		ReadSession: &storagepb.ReadSession{
			Table:      tablePath,
			DataFormat: storagepb.DataFormat_ARROW,
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				OutputFormatSerializationOptions: &storagepb.ReadSession_TableReadOptions_ArrowSerializationOptions{
					ArrowSerializationOptions: &storagepb.ArrowSerializationOptions{
						BufferCompression: storagepb.ArrowSerializationOptions_ZSTD,
					},
				},
			},
		},
		// MaxStreamCount alone is a ceiling the server may answer with one stream;
		// the preferred minimum asks it to split the table at least this far.
		MaxStreamCount:          maxStreamCount,
		PreferredMinStreamCount: maxStreamCount,
	}, msgSizeOpt)
	if err != nil {
		return nil, nil, errToAdbcErr(adbc.StatusIO, err, "create read session")
	}

	schema, err := arrowSchemaFromReadSession(readSession)
	if err != nil {
		return nil, nil, errToAdbcErr(adbc.StatusInternal, err, "decode read session schema")
	}

	streamNames := make([]string, len(readSession.GetStreams()))
	for i, stream := range readSession.GetStreams() {
		streamNames[i] = stream.GetName()
	}
	return schema, streamNames, nil
}

// arrowSchemaFromReadSession decodes a read session's IPC-serialized Arrow
// schema and undoes the same BigQuery extension-type mangling that
// newInlineArrowIterator corrects for the REST API's inline Arrow path.
func arrowSchemaFromReadSession(readSession *storagepb.ReadSession) (*arrow.Schema, error) {
	serialized := readSession.GetArrowSchema().GetSerializedSchema()
	schema, err := flight.DeserializeSchema(serialized, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Arrow schema: %w", err)
	}
	fields := schema.Fields()
	for i := range fields {
		unmangleBigQueryArrowType(&fields[i])
	}
	return arrow.NewSchema(fields, nil), nil
}
