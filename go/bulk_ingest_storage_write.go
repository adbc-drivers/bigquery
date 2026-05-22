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

	"cloud.google.com/go/bigquery"
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
)

type storageWriteBulkIngestImpl struct {
	alloc       memory.Allocator
	schema      *arrow.Schema
	logger      *slog.Logger
	options     driverbase.BulkIngestOptions
	queryConfig bigquery.QueryConfig
	client      *bigquery.Client

	casts          []*compute.CastOptions
	writeClient    *storage.BigQueryWriteClient
	tableReference string
	streamName     string
	appendStream   storagepb.BigQueryWrite_AppendRowsClient
	offset         int64
}

var _ driverbase.BulkIngestImpl = (*storageWriteBulkIngestImpl)(nil)
var _ driverbase.BulkIngestInitFinalizeImpl = (*storageWriteBulkIngestImpl)(nil)
var _ driverbase.BulkIngestTransformImpl = (*storageWriteBulkIngestImpl)(nil)

func (impl *storageWriteBulkIngestImpl) createWriteStream(ctx context.Context) error {
	// use a pending stream for atomicity
	req := &storagepb.CreateWriteStreamRequest{
		Parent: impl.tableReference,
		WriteStream: &storagepb.WriteStream{
			Type: storagepb.WriteStream_PENDING,
		},
	}

	if err := retry(ctx, "create write stream", func() (bool, error) {
		stream, err := impl.writeClient.CreateWriteStream(ctx, req)
		if err == nil {
			// completed
			impl.streamName = stream.Name
			return true, nil
		}

		var apiError *apierror.APIError
		if errors.As(err, &apiError) && apiError.GRPCStatus().Code() == codes.NotFound {
			impl.logger.Debug("retrying create write stream", "table", impl.tableReference, "error", err)
			return false, err
		}
		return true, err
	}); err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "create write stream for %s", impl.tableReference)
	}

	impl.logger.Debug("created write stream", "stream", impl.streamName)
	return nil
}

func (impl *storageWriteBulkIngestImpl) Init(ctx context.Context) error {
	catalog := impl.options.CatalogName
	if catalog == "" {
		catalog = impl.queryConfig.DefaultProjectID
	}
	schema := impl.options.SchemaName
	if schema == "" {
		schema = impl.queryConfig.DefaultDatasetID
	}
	impl.tableReference = fmt.Sprintf("projects/%s/datasets/%s/tables/%s", catalog, schema, impl.options.TableName)

	// update schema/prepare to transform batches
	// TODO: this needs to be generalized to cover lists; we have to remove null list values and splice in empty lists instead.
	// TODO: we need an option to allow unsafe casts
	fields := make([]arrow.Field, len(impl.schema.Fields()))
	for i, field := range impl.schema.Fields() {
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
			if impl.casts == nil {
				impl.casts = make([]*compute.CastOptions, len(impl.schema.Fields()))
			}
			impl.casts[i] = cast
		}
	}
	if impl.casts != nil {
		md := impl.schema.Metadata()
		impl.schema = arrow.NewSchema(fields, &md)
	}

	writeClient, err := storage.NewBigQueryWriteClient(ctx)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "create storage write client")
	}
	impl.writeClient = writeClient
	return nil
}

func (impl *storageWriteBulkIngestImpl) TransformedSchema() *arrow.Schema {
	return impl.schema
}

func (impl *storageWriteBulkIngestImpl) TransformBatch(ctx context.Context, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	if impl.casts == nil {
		batch.Retain()
		return batch, nil
	}

	rawCols := batch.Columns()
	cols := make([]arrow.Array, len(rawCols))
	ownedCols := []arrow.Array{}
	defer func() {
		for _, col := range ownedCols {
			col.Release()
		}
	}()

	for i, cast := range impl.casts {
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

	return array.NewRecordBatch(impl.schema, cols, batch.NumRows()), nil
}

func (impl *storageWriteBulkIngestImpl) Finalize(ctx context.Context, success bool) error {
	// N.B. always called
	if impl.appendStream != nil {
		// commit the pending stream
		if err := impl.appendStream.CloseSend(); err != nil {
			impl.logger.Warn("error closing send stream", "err", err)
		}
	}

	if success {
		_, err := impl.writeClient.FinalizeWriteStream(ctx, &storagepb.FinalizeWriteStreamRequest{
			Name: impl.streamName,
		})
		if err != nil {
			return errToAdbcErr(adbc.StatusIO, err, "finalize write stream")
		}
		impl.logger.Debug("finalized write stream", "stream", impl.streamName)

		// Commit the stream atomically
		commitReq := &storagepb.BatchCommitWriteStreamsRequest{
			Parent:       impl.tableReference,
			WriteStreams: []string{impl.streamName},
		}

		_, err = impl.writeClient.BatchCommitWriteStreams(ctx, commitReq)
		if err != nil {
			return errToAdbcErr(adbc.StatusIO, err, "commit write streams")
		}

		impl.logger.Debug("committed write stream", "stream", impl.streamName)
	}

	if impl.writeClient != nil {
		return impl.writeClient.Close()
	}
	// N.B. there's no way to explicitly dispose of a write stream; it
	// simply expires after three days
	return nil
}

func (impl *storageWriteBulkIngestImpl) CreateTable(ctx context.Context, schema *arrow.Schema, ifTableExists driverbase.BulkIngestTableExistsBehavior, ifTableMissing driverbase.BulkIngestTableMissingBehavior) error {
	return createTableWithAPI(ctx, impl.client, impl.queryConfig, &impl.options, schema, ifTableExists, ifTableMissing)
}

func (impl *storageWriteBulkIngestImpl) CreateSink(ctx context.Context, options *driverbase.BulkIngestOptions) (driverbase.BulkIngestSink, error) {
	return &driverbase.BufferBulkIngestSink{}, nil
}

func (impl *storageWriteBulkIngestImpl) Serialize(ctx context.Context, writerProps *driverbase.WriterProps, schema *arrow.Schema, batches chan arrow.RecordBatch, sink driverbase.BulkIngestSink) (int64, int64, error) {
	batch := <-batches
	if batch == nil {
		return 0, 0, nil
	}
	defer batch.Release()

	// TODO: try to fit about 10 MB of data per call
	// TODO: somehow try to split incoming batches to limit to 10 MB

	payload, err := ipc.GetRecordBatchPayload(batch, writerProps.ArrowIpcProps...)
	if err != nil {
		return 0, 0, errToAdbcErr(adbc.StatusInternal, err, "get record batch payload")
	}
	defer payload.Release()

	written, err := payload.WritePayload(sink.Sink())
	if err != nil {
		return 0, 0, errToAdbcErr(adbc.StatusInternal, err, "serialize record batch payload")
	}
	return int64(batch.NumRows()), int64(written), nil
}

func (impl *storageWriteBulkIngestImpl) Copy(ctx context.Context, chunk driverbase.BulkIngestPendingCopy) error {
	// Create the actual append stream request. We may have to retry on
	// the first upload since apparently this part is eventually
	// consistent and creating the stream doesn't mean that the request
	// will succeed.
	c := chunk.(*driverbase.BulkIngestPendingUpload)
	b := c.Data.(*driverbase.BufferBulkIngestSink)
	rows := &storagepb.AppendRowsRequest_ArrowRows{
		ArrowRows: &storagepb.AppendRowsRequest_ArrowData{
			Rows: &storagepb.ArrowRecordBatch{
				SerializedRecordBatch: b.Bytes(),
			},
		},
	}
	if impl.appendStream == nil {
		if err := impl.createWriteStream(ctx); err != nil {
			return err
		}
	}

	schemaBytes, err := serializeArrowSchema(impl.schema, impl.alloc)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err, "serialize schema for append stream")
	}
	rows.ArrowRows.WriterSchema = &storagepb.ArrowSchema{
		SerializedSchema: schemaBytes,
	}

	req := &storagepb.AppendRowsRequest{
		WriteStream: impl.streamName,
		Rows:        rows,
	}

	// The first time we try to write, BigQuery appears to have an issue
	// where it thinks the stream doesn't exist, even though we just
	// created it, so retry if needed
	if err := retry(ctx, "append rows to "+impl.tableReference, func() (bool, error) {
		err := func() error {
			var appendStream storagepb.BigQueryWrite_AppendRowsClient

			if impl.appendStream != nil {
				appendStream = impl.appendStream
			} else {
				var err error
				appendStream, err = impl.writeClient.AppendRows(ctx)
				if err != nil {
					return errToAdbcErr(adbc.StatusIO, err, "begin AppendRows(%s)", impl.tableReference)
				}
			}

			if err := appendStream.Send(req); err != nil {
				return errToAdbcErr(adbc.StatusIO, err, "send AppendRows(%s)", impl.tableReference)
			}
			resp, err := appendStream.Recv()
			if err != nil {
				return errToAdbcErr(adbc.StatusIO, err, "receive AppendRows(%s)", impl.tableReference)
			}

			if resp.GetError() != nil {
				return errToAdbcErr(adbc.StatusIO,
					fmt.Errorf("append failed: %v", resp.GetError()),
					"append rows")
			}
			impl.appendStream = appendStream
			return nil
		}()
		if err == nil {
			return true, nil
		}

		// Only retry on the first request; if we've already created
		// the stream, we don't want to recreate it.  Weirdly
		// annoyingly, gRPC returns a status.Error which is an
		// internal type, making it hard to determine whether to retry
		// TODO(lidavidm): maybe it's OK to recreate appendStream?
		if impl.appendStream == nil && strings.Contains(err.Error(), "NotFound") {
			impl.logger.Debug("retrying AppendRows",
				"table", impl.tableReference,
				"error", err)

			return false, nil
		}
		return true, err
	}); err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "AppendRows(%s)", impl.tableReference)
	}

	impl.offset += int64(chunk.NumRows())
	impl.logger.Debug("appended batch", "rows", chunk.NumRows(), "offset", impl.offset)
	return nil
}

func serializeArrowSchema(schema *arrow.Schema, alloc memory.Allocator) ([]byte, error) {
	payload := ipc.GetSchemaPayload(schema, alloc)
	defer payload.Release()
	var buf bytes.Buffer
	if _, err := payload.WritePayload(&buf); err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err, "serialize schema payload")
	}
	return buf.Bytes(), nil
}
