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
	"time"

	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// executeIngestStorageWrite implements bulk ingest using the BigQuery Storage Write API.
func (st *statement) executeIngestStorageWrite(ctx context.Context) (n int64, err error) {
	logger := st.cnxn.Logger.With("op", "bulkingest-storagewrite")

	params := st.params
	st.params = nil
	defer params.Release()

	// Create table if needed
	schema := params.Schema()
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
		client:          writeClient,
		logger:          logger,
		tableName:       st.buildTableReference(),
		streamType:      st.selectStreamType(),
		compressionOpts: st.getCompressionOptions(),
		alloc:           st.alloc,
	}
	defer func() {
		err = errors.Join(err, stream.Close())
	}()

	// Execute streaming ingestion
	rowsWritten, err := stream.ingest(ctx, params)
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

	// Reuse existing createTableStatement function from bulk_ingest.go
	stmt, err := createTableStatement(&st.ingest, schema, ifTableExists, ifTableMissing)
	if err != nil {
		return err
	}

	if stmt == "" {
		return nil // No table creation needed
	}

	// Execute CREATE TABLE statement
	query := st.cnxn.client.Query("")
	query.QueryConfig = st.queryConfig
	query.Q = stmt
	job, err := query.Run(ctx)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "create table")
	}

	status, err := safeWaitForJob(ctx, st.cnxn.Logger, job)
	if err != nil {
		return err
	}

	if err := status.Err(); err != nil {
		return errToAdbcErr(adbc.StatusInternal, err, "create table")
	}

	// Poll until BigQuery recognizes the new table
	if err := st.waitForTableAvailable(ctx); err != nil {
		return err
	}

	return nil
}

// waitForTableAvailable polls until the newly created table is available in BigQuery.
// BigQuery can have eventual consistency delays after table creation.
func (st *statement) waitForTableAvailable(ctx context.Context) error {
	catalog := st.ingest.CatalogName
	if catalog == "" {
		catalog = st.queryConfig.DefaultProjectID
	}

	schema := st.ingest.SchemaName
	if schema == "" {
		schema = st.queryConfig.DefaultDatasetID
	}

	table := st.cnxn.client.DatasetInProject(catalog, schema).Table(st.ingest.TableName)
	backoff := gax.Backoff{
		Initial:    100 * time.Millisecond,
		Multiplier: 2.0,
		Max:        5 * time.Second,
	}

	for {
		_, err := table.Metadata(ctx)
		if err == nil {
			st.cnxn.Logger.Debug("table is available", "table", st.ingest.TableName)
			return nil
		}

		// Check if it's a 404 (not found) error - this is expected while waiting
		if apiErr, ok := err.(*googleapi.Error); ok && apiErr.Code == 404 {
			duration := backoff.Pause()
			st.cnxn.Logger.Debug("waiting for table to be available",
				"table", st.ingest.TableName,
				"backoff", duration)

			if err := gax.Sleep(ctx, duration); err != nil {
				return err
			}
			continue
		}

		// For other errors, fail immediately
		return errToAdbcErr(adbc.StatusInternal, err, "check table availability")
	}
}

// storageWriteStream manages a BigQuery Storage Write API stream.
type storageWriteStream struct {
	client          *storage.BigQueryWriteClient
	logger          *slog.Logger
	tableName       string // projects/{project}/datasets/{dataset}/tables/{table}
	streamType      storagepb.WriteStream_Type
	compressionOpts []ipc.Option
	alloc           memory.Allocator

	streamName   string
	appendStream storagepb.BigQueryWrite_AppendRowsClient
	offset       int64
	schemaSet    bool
}

// ingest reads Arrow data and streams it to BigQuery using the Storage Write API.
func (s *storageWriteStream) ingest(ctx context.Context, reader array.RecordReader) (int64, error) {
	// Open stream
	if err := s.openStream(ctx); err != nil {
		return 0, err
	}

	var totalRows int64

	// Read and stream batches
	for reader.Next() {
		batch := reader.RecordBatch()

		if err := s.appendBatch(ctx, batch); err != nil {
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

		stream, err := s.client.CreateWriteStream(ctx, req)
		if err != nil {
			return errToAdbcErr(adbc.StatusIO, err, "create write stream")
		}

		s.streamName = stream.Name
		s.logger.Debug("created write stream", "stream", s.streamName)
	}

	// Open bidirectional AppendRows stream
	appendStream, err := s.client.AppendRows(ctx)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "open append stream")
	}

	s.appendStream = appendStream
	return nil
}

// appendBatch sends a single Arrow record batch to the stream.
func (s *storageWriteStream) appendBatch(ctx context.Context, batch arrow.RecordBatch) error {
	// Build AppendRowsRequest
	req := &storagepb.AppendRowsRequest{
		WriteStream: s.streamName,
	}

	// Include schema on first request
	if !s.schemaSet {
		schemaBytes, err := serializeArrowSchema(batch.Schema(), s.alloc)
		if err != nil {
			return err
		}

		req.Rows = &storagepb.AppendRowsRequest_ArrowRows{
			ArrowRows: &storagepb.AppendRowsRequest_ArrowData{
				WriterSchema: &storagepb.ArrowSchema{
					SerializedSchema: schemaBytes,
				},
				Rows: &storagepb.ArrowRecordBatch{
					SerializedRecordBatch: nil, // Will set below
				},
			},
		}
		s.schemaSet = true
	}

	// Serialize record batch with optional compression
	batchBytes, err := serializeArrowRecordBatch(batch, s.alloc, s.compressionOpts)
	if err != nil {
		return err
	}

	// Set record batch in request
	if req.Rows == nil {
		req.Rows = &storagepb.AppendRowsRequest_ArrowRows{
			ArrowRows: &storagepb.AppendRowsRequest_ArrowData{
				Rows: &storagepb.ArrowRecordBatch{
					SerializedRecordBatch: batchBytes,
				},
			},
		}
	} else {
		req.Rows.(*storagepb.AppendRowsRequest_ArrowRows).ArrowRows.Rows.SerializedRecordBatch = batchBytes
	}

	// Include offset for exactly-once semantics
	if s.streamType == storagepb.WriteStream_COMMITTED {
		req.Offset = &wrapperspb.Int64Value{Value: s.offset}
	}

	// Send request
	if err := s.appendStream.Send(req); err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "send append request")
	}

	// Receive response
	resp, err := s.appendStream.Recv()
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "receive append response")
	}

	// Check for errors
	if resp.GetError() != nil {
		return errToAdbcErr(adbc.StatusIO,
			fmt.Errorf("append failed: %v", resp.GetError()),
			"append rows")
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
	// Get the schema payload
	payload := ipc.GetSchemaPayload(schema, alloc)
	defer payload.Release()

	// Serialize the payload to bytes
	var buf bytes.Buffer
	if _, err := payload.WritePayload(&buf); err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err, "serialize schema payload")
	}

	return buf.Bytes(), nil
}

// serializeArrowRecordBatch serializes an Arrow record batch to IPC format with optional compression.
func serializeArrowRecordBatch(record arrow.RecordBatch, alloc memory.Allocator, compressionOpts []ipc.Option) ([]byte, error) {
	// Build options with allocator
	opts := []ipc.Option{ipc.WithAllocator(alloc)}
	opts = append(opts, compressionOpts...)

	// Get the record batch payload with compression applied
	payload, err := ipc.GetRecordBatchPayload(record, opts...)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err, "get record batch payload")
	}
	defer payload.Release()

	// Serialize the payload to bytes
	var buf bytes.Buffer
	if _, err := payload.WritePayload(&buf); err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err, "serialize record batch payload")
	}

	return buf.Bytes(), nil
}
