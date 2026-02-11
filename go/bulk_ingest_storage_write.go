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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// executeIngestStorageWrite implements bulk ingest using the BigQuery Storage Write API.
func (st *statement) executeIngestStorageWrite(ctx context.Context) (n int64, err error) {
	logger := st.cnxn.Logger.With("op", "bulkingest-storagewrite")

	params := st.params
	st.params = nil
	defer params.Release()

	schema := params.Schema()

	// TODO: this needs to be generalized to cover lists; we have to remove null list values and splice in empty lists instead.
	// TODO: we need an option to allow unsafe casts
	var casts []*compute.CastOptions
	fields := make([]arrow.Field, len(schema.Fields()))
	for i, field := range schema.Fields() {
		fields[i] = field
		var cast *compute.CastOptions
		switch field.Type.ID() {
		case arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
			// TODO: arrow-go doesn't implement binary_view -> binary
			cast = compute.SafeCastOptions(arrow.BinaryTypes.Binary)
			fields[i].Type = arrow.BinaryTypes.Binary
		case arrow.STRING_VIEW:
			// TODO: arrow-go doesn't implement string_view -> binary
			cast = compute.SafeCastOptions(arrow.BinaryTypes.String)
			fields[i].Type = arrow.BinaryTypes.String
		case arrow.TIME32:
			cast = compute.SafeCastOptions(arrow.FixedWidthTypes.Time64us)
			fields[i].Type = arrow.FixedWidthTypes.Time64us
		case arrow.TIME64:
			timeTy := field.Type.(*arrow.Time64Type)
			if timeTy.Unit != arrow.Microsecond {
				cast = compute.SafeCastOptions(arrow.FixedWidthTypes.Time64us)
				fields[i].Type = arrow.FixedWidthTypes.Time64us
			}
		case arrow.TIMESTAMP:
			// XXX: BigQuery ignores the Arrow unit and just assumes microseconds
			tsTy := field.Type.(*arrow.TimestampType)
			if tsTy.Unit != arrow.Microsecond {
				ty := &arrow.TimestampType{
					Unit:     arrow.Microsecond,
					TimeZone: tsTy.TimeZone,
				}
				cast = compute.SafeCastOptions(ty)
				fields[i].Type = ty
			}
		}

		if cast != nil {
			if casts == nil {
				casts = make([]*compute.CastOptions, len(schema.Fields()))
			}
			casts[i] = cast
		}
	}
	if casts != nil {
		md := schema.Metadata()
		schema = arrow.NewSchema(fields, &md)
	}

	// Create table if needed
	if err := st.createTableForIngest(ctx, schema); err != nil {
		return -1, err
	}

	// Create Storage Write client
	writeClient, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		return -1, errToAdbcErr(adbc.StatusIO, err, "create storage write client")
	}
	defer func() {
		err = errors.Join(err, writeClient.Close())
	}()

	// Create and manage write stream
	stream := &storageWriteStream{
		client:     writeClient,
		logger:     logger,
		tableName:  st.buildTableReference(),
		streamType: st.selectStreamType(),
		casts:      casts,
		ipcOpts:    append([]ipc.Option{ipc.WithAllocator(st.alloc)}, st.getCompressionOptions()...),
		alloc:      st.alloc,
	}
	defer func() {
		err = errors.Join(err, stream.Close())
	}()

	// Execute streaming ingestion
	rowsWritten, err := stream.ingest(ctx, schema, params)
	if err != nil {
		return -1, err
	}

	return rowsWritten, nil
}

// selectStreamType chooses the appropriate stream type based on ingest mode.
func (st *statement) selectStreamType() storagepb.WriteStream_Type {
	return storagepb.WriteStream_PENDING
}

// buildTableReference constructs the table reference string for Storage Write API.
func (st *statement) buildTableReference() string {
	catalog := st.ingest.CatalogName
	if catalog == "" {
		catalog = st.queryConfig.DefaultProjectID
	}

	schema := st.ingest.SchemaName
	if schema == "" {
		schema = st.queryConfig.DefaultDatasetID
	}

	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s",
		catalog, schema, st.ingest.TableName)
}

// getCompressionOptions returns Arrow IPC compression options based on statement options.
func (st *statement) getCompressionOptions() []ipc.Option {
	compression, err := st.GetOption(OptionStringBulkIngestCompression)
	if err != nil {
		compression = OptionValueCompressionNone
	}

	switch compression {
	case OptionValueCompressionLZ4:
		return []ipc.Option{ipc.WithLZ4()}
	case OptionValueCompressionZSTD:
		return []ipc.Option{ipc.WithZstd()}
	default:
		return nil
	}
}

// createTableForIngest creates the target table if needed based on ingest mode.
func (st *statement) createTableForIngest(ctx context.Context, schema *arrow.Schema) error {
	// Determine behaviors based on ingest mode
	var ifTableExists driverbase.BulkIngestTableExistsBehavior
	var ifTableMissing driverbase.BulkIngestTableMissingBehavior

	switch st.ingest.Mode {
	case adbc.OptionValueIngestModeCreate:
		ifTableExists = driverbase.BulkIngestTableExistsError
		ifTableMissing = driverbase.BulkIngestTableMissingCreate
	case adbc.OptionValueIngestModeAppend:
		ifTableExists = driverbase.BulkIngestTableExistsIgnore
		ifTableMissing = driverbase.BulkIngestTableMissingError
	case adbc.OptionValueIngestModeReplace:
		ifTableExists = driverbase.BulkIngestTableExistsDrop
		ifTableMissing = driverbase.BulkIngestTableMissingCreate
	case adbc.OptionValueIngestModeCreateAppend:
		ifTableExists = driverbase.BulkIngestTableExistsIgnore
		ifTableMissing = driverbase.BulkIngestTableMissingCreate
	}

	// Use API-based table creation
	if err := createTableWithAPI(ctx, st.cnxn.client, st.queryConfig, &st.ingest, schema, ifTableExists, ifTableMissing); err != nil {
		return err
	}

	return nil
}

// storageWriteStream manages a BigQuery Storage Write API stream.
type storageWriteStream struct {
	client     *storage.BigQueryWriteClient
	logger     *slog.Logger
	tableName  string // projects/{project}/datasets/{dataset}/tables/{table}
	streamType storagepb.WriteStream_Type
	casts      []*compute.CastOptions
	ipcOpts    []ipc.Option
	alloc      memory.Allocator

	streamName   string
	appendStream storagepb.BigQueryWrite_AppendRowsClient
	offset       int64
}

// ingest reads Arrow data and streams it to BigQuery using the Storage Write API.
func (s *storageWriteStream) ingest(ctx context.Context, schema *arrow.Schema, reader array.RecordReader) (int64, error) {
	// Open stream
	if err := s.openStream(ctx); err != nil {
		return 0, err
	}

	var totalRows int64

	// Read and stream batches
	execCtx := compute.WithAllocator(ctx, s.alloc)
	for reader.Next() {
		batch := reader.RecordBatch()

		if err := s.appendBatch(execCtx, schema, batch); err != nil {
			return totalRows, err
		}

		totalRows += batch.NumRows()
	}

	if err := reader.Err(); err != nil {
		return totalRows, errToAdbcErr(adbc.StatusInternal, err, "read arrow data")
	}

	// Finalize and commit if using PENDING stream
	if s.streamType == storagepb.WriteStream_PENDING {
		if err := s.finalizeAndCommit(ctx); err != nil {
			return totalRows, err
		}
	}

	return totalRows, nil
}

// openStream creates or connects to a write stream.
func (s *storageWriteStream) openStream(ctx context.Context) error {
	// For COMMITTED type, use the default stream
	if s.streamType == storagepb.WriteStream_COMMITTED {
		s.streamName = fmt.Sprintf("%s/streams/_default", s.tableName)
	} else {
		// Create explicit stream for PENDING type
		req := &storagepb.CreateWriteStreamRequest{
			Parent: s.tableName,
			WriteStream: &storagepb.WriteStream{
				Type: s.streamType,
			},
		}

		var stream *storagepb.WriteStream
		if err := retry(ctx, "create write stream", func() (bool, error) {
			var err error
			stream, err = s.client.CreateWriteStream(ctx, req)
			if err == nil {
				// completed
				return true, nil
			}

			var apiError *apierror.APIError
			if errors.As(err, &apiError) && apiError.GRPCStatus().Code() == codes.NotFound {
				s.logger.Debug("retrying create write stream", "table", s.tableName, "error", err)
				return false, nil
			}
			return false, err
		}); err != nil {
			return errToAdbcErr(adbc.StatusIO, err, "create %s write stream for %s", s.streamType, s.tableName)
		}
		s.streamName = stream.Name
		s.logger.Debug("created write stream", "stream", s.streamName)
	}
	return nil
}

// appendBatch sends a single Arrow record batch to the stream.
func (s *storageWriteStream) appendBatch(ctx context.Context, schema *arrow.Schema, batch arrow.RecordBatch) error {
	batchBytes, err := serializeArrowRecordBatch(ctx, schema, batch, s.ipcOpts, s.casts)
	if err != nil {
		return err
	}

	rows := &storagepb.AppendRowsRequest_ArrowRows{
		ArrowRows: &storagepb.AppendRowsRequest_ArrowData{
			Rows: &storagepb.ArrowRecordBatch{
				SerializedRecordBatch: batchBytes,
			},
		},
	}

	// Include schema on first request
	if s.appendStream == nil {
		schemaBytes, err := serializeArrowSchema(schema, s.alloc)
		if err != nil {
			return err
		}
		rows.ArrowRows.WriterSchema = &storagepb.ArrowSchema{
			SerializedSchema: schemaBytes,
		}
	}

	req := &storagepb.AppendRowsRequest{
		WriteStream: s.streamName,
		Rows:        rows,
	}
	if s.streamType == storagepb.WriteStream_COMMITTED {
		// Include offset for exactly-once semantics
		req.Offset = &wrapperspb.Int64Value{Value: s.offset}
	}

	if s.appendStream == nil {
		// Send the schema and the first batch to try to flush out an
		// issue where BQ doesn't seem to know that the stream
		// exists...
		if err := retry(ctx, "append rows to "+s.tableName, func() (bool, error) {
			err := func() error {
				appendStream, err := s.client.AppendRows(ctx)
				if err != nil {
					return errToAdbcErr(adbc.StatusIO, err, "begin AppendRows(%s)", s.tableName)
				}
				if err := appendStream.Send(req); err != nil {
					return errToAdbcErr(adbc.StatusIO, err, "send AppendRows(%s)", s.tableName)
				}
				resp, err := appendStream.Recv()
				if err != nil {
					return errToAdbcErr(adbc.StatusIO, err, "receive AppendRows(%s)", s.tableName)
				}

				if resp.GetError() != nil {
					return errToAdbcErr(adbc.StatusIO,
						fmt.Errorf("append failed: %v", resp.GetError()),
						"append rows")
				}
				s.appendStream = appendStream
				return nil
			}()
			if err == nil {
				return true, nil
			}

			// Weirdly annoyingly, gRPC returns a status.Error which is an internal type
			if strings.Contains(err.Error(), "NotFound") {
				s.logger.Debug("retrying AppendRows",
					"table", s.tableName,
					"error", err)

				return false, nil
			}
			return false, err
		}); err != nil {
			return errToAdbcErr(adbc.StatusIO, err, "AppendRows(%s)", s.tableName)
		}
	} else {
		// Once we've managed to send the first message, it seems
		// there's no need to retry
		if err := s.appendStream.Send(req); err != nil {
			return errToAdbcErr(adbc.StatusIO, err, "send append request")
		}
		resp, err := s.appendStream.Recv()
		if err != nil {
			return errToAdbcErr(adbc.StatusIO, err, "receive append response")
		}

		if resp.GetError() != nil {
			return errToAdbcErr(adbc.StatusIO,
				fmt.Errorf("append failed: %v", resp.GetError()),
				"append rows")
		}
	}

	// Update offset
	s.offset += batch.NumRows()
	s.logger.Debug("appended batch", "rows", batch.NumRows(), "offset", s.offset)
	return nil
}

// finalizeAndCommit finalizes and commits a PENDING stream.
func (s *storageWriteStream) finalizeAndCommit(ctx context.Context) error {
	// Close send side
	if err := s.appendStream.CloseSend(); err != nil {
		s.logger.Warn("error closing send stream", "err", err)
	}

	// Finalize the stream
	finalizeReq := &storagepb.FinalizeWriteStreamRequest{
		Name: s.streamName,
	}

	_, err := s.client.FinalizeWriteStream(ctx, finalizeReq)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "finalize write stream")
	}

	s.logger.Debug("finalized write stream", "stream", s.streamName)

	// Commit the stream atomically
	parent := s.tableName
	commitReq := &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       parent,
		WriteStreams: []string{s.streamName},
	}

	_, err = s.client.BatchCommitWriteStreams(ctx, commitReq)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "commit write streams")
	}

	s.logger.Debug("committed write stream", "stream", s.streamName)
	return nil
}

// close cleans up the stream.
func (s *storageWriteStream) Close() error {
	if s.appendStream != nil {
		return s.appendStream.CloseSend()
	}
	return nil
}

// serializeArrowSchema serializes an Arrow schema to IPC format.
func serializeArrowSchema(schema *arrow.Schema, alloc memory.Allocator) ([]byte, error) {
	// TODO: try to use ipc.PayloadWriter, though this requires us to
	// convert push-to-pull (since we have to consolidate the schema and
	// record batch into a single RPC call, we can't just directly turn
	// one IPC payload into one gRPC send).
	payload := ipc.GetSchemaPayload(schema, alloc)
	defer payload.Release()
	var buf bytes.Buffer
	if _, err := payload.WritePayload(&buf); err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err, "serialize schema payload")
	}
	return buf.Bytes(), nil
}

// serializeArrowRecordBatch serializes an Arrow record batch to IPC format with optional compression.
func serializeArrowRecordBatch(ctx context.Context, schema *arrow.Schema, batch arrow.RecordBatch, opts []ipc.Option, casts []*compute.CastOptions) ([]byte, error) {
	var payload ipc.Payload
	// TODO: BigQuery limits us to ~10 MB of data per call; need to probe
	// how this limit is computed and also respect it

	if casts != nil {
		rawCols := batch.Columns()
		cols := make([]arrow.Array, len(rawCols))
		ownedCols := []arrow.Array{}
		defer func() {
			for _, col := range ownedCols {
				col.Release()
			}
		}()

		for i, cast := range casts {
			if cast == nil {
				cols[i] = rawCols[i]
			} else {
				var err error
				cols[i], err = compute.CastArray(ctx, rawCols[i], cast)
				if err != nil {
					return nil, fmt.Errorf("could not prepare ingest data: could not cast column %d to `%s`: %v", i+1, cast.ToType, err)
				}
				ownedCols = append(ownedCols, cols[i])
			}
		}

		rb := array.NewRecordBatch(schema, cols, batch.NumRows())
		defer rb.Release()

		var err error
		payload, err = ipc.GetRecordBatchPayload(rb, opts...)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusInternal, err, "get record batch payload")
		}
	} else {
		var err error
		payload, err = ipc.GetRecordBatchPayload(batch, opts...)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusInternal, err, "get record batch payload")
		}
	}
	defer payload.Release()

	// Serialize the payload to bytes
	var buf bytes.Buffer
	if _, err := payload.WritePayload(&buf); err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err, "serialize record batch payload")
	}

	return buf.Bytes(), nil
}
