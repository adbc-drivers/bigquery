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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
)

type bigqueryBulkIngestSink struct {
	f    *os.File
	path string
	rows int64
}

func (sink *bigqueryBulkIngestSink) NumRows() int64 {
	return sink.rows
}

func (sink *bigqueryBulkIngestSink) Sink() io.Writer {
	return sink.f
}

func (sink *bigqueryBulkIngestSink) String() string {
	return sink.path
}

func (sink *bigqueryBulkIngestSink) Close() error {
	// Suppress error because Parquet writer may have closed the file already
	err := sink.f.Close()
	if !errors.Is(err, os.ErrClosed) {
		return err
	}
	return nil
}

type bigqueryBulkIngestImpl struct {
	driverbase.ParquetIngestImpl

	logger      *slog.Logger
	options     driverbase.BulkIngestOptions
	queryConfig bigquery.QueryConfig
	client      *bigquery.Client

	tmpdir string
}

var _ driverbase.BulkIngestImpl = (*bigqueryBulkIngestImpl)(nil)
var _ driverbase.BulkIngestFileImpl = (*bigqueryBulkIngestImpl)(nil)
var _ driverbase.BulkIngestInitFinalizeImpl = (*bigqueryBulkIngestImpl)(nil)

func (bi *bigqueryBulkIngestImpl) Init(ctx context.Context) error {
	tmpdir, err := os.MkdirTemp("", fmt.Sprintf("bq-bulk-%s", bi.options.TableName))
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "create temporary directory for bulk ingest")
	}
	bi.tmpdir = tmpdir
	return nil
}

func (bi *bigqueryBulkIngestImpl) Finalize(ctx context.Context, success bool) error {
	if bi.tmpdir != "" {
		if err := os.RemoveAll(bi.tmpdir); err != nil {
			// Suppress errors
			bi.logger.Error("failed to remove temporary directory", "dir", bi.tmpdir, "err", err)
		}
	}
	bi.tmpdir = ""
	return nil
}

func (bi *bigqueryBulkIngestImpl) Copy(ctx context.Context, chunk driverbase.BulkIngestPendingCopy) error {
	pendingFile := chunk.(*bigqueryBulkIngestSink)

	source := bigquery.NewReaderSource(pendingFile.f)
	source.ParquetOptions = &bigquery.ParquetOptions{
		// Needed for ARRAY to work
		EnableListInference: true,
	}
	source.SourceFormat = bigquery.Parquet

	var dataset *bigquery.Dataset
	if bi.options.CatalogName != "" && bi.options.SchemaName != "" {
		dataset = bi.client.DatasetInProject(bi.options.CatalogName, bi.options.SchemaName)
	} else if bi.options.SchemaName != "" {
		// bulk ingest base will check if the catalog but not the schema is set
		dataset = bi.client.Dataset(bi.options.SchemaName)
	} else {
		dataset = bi.client.Dataset(bi.queryConfig.DefaultDatasetID)
	}
	loader := dataset.Table(bi.options.TableName).LoaderFrom(source)
	// TODO(lidavidm): we could skip the explicit create table; but does
	// that give us enough control over the schema? Also, we would have to
	// buffer all the data into one file or buffer all the data into cloud
	// storage and do a single upload.  That may or may not be preferable
	// (less parallelism, but BigQuery itself will guarantee atomicity)
	loader.CreateDisposition = bigquery.CreateNever

	job, err := loader.Run(ctx)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "run loader")
	}
	status, err := safeWaitForJob(ctx, bi.logger, job)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "load data")
	}
	return nil
}

func (bi *bigqueryBulkIngestImpl) CreateSink(ctx context.Context, options *driverbase.BulkIngestOptions) (driverbase.BulkIngestSink, error) {
	filename := fmt.Sprintf("%s/%s.parquet", bi.tmpdir, uuid.Must(uuid.NewV7()))
	f, err := os.Create(filename)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err, "create Parquet file to load")
	}
	return &bigqueryBulkIngestSink{
		f:    f,
		path: filename,
	}, nil
}

func (bi *bigqueryBulkIngestImpl) CreateTable(ctx context.Context, schema *arrow.Schema, ifTableExists driverbase.BulkIngestTableExistsBehavior, ifTableMissing driverbase.BulkIngestTableMissingBehavior) error {
	bi.logger.Debug("creating table", "table", bi.options.TableName)

	if err := createTableWithAPI(ctx, bi.client, bi.queryConfig, &bi.options, schema, ifTableExists, ifTableMissing); err != nil {
		bi.logger.Debug("failed to create table", "table", bi.options.TableName, "error", err)
		return err
	}

	bi.logger.Debug("created table", "table", bi.options.TableName)
	return nil
}

func (bi *bigqueryBulkIngestImpl) Upload(ctx context.Context, chunk driverbase.BulkIngestPendingUpload) (driverbase.BulkIngestPendingCopy, error) {
	// No need for separate upload step
	sink := chunk.Data.(*bigqueryBulkIngestSink)
	sink.rows = chunk.Rows
	file, err := os.Open(sink.path)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err, "open file to load")
	}
	return &bigqueryBulkIngestSink{
		f:    file,
		path: sink.path,
		rows: chunk.Rows,
	}, nil
}

func (bi *bigqueryBulkIngestImpl) Delete(ctx context.Context, chunk driverbase.BulkIngestPendingCopy) error {
	sink := chunk.(*bigqueryBulkIngestSink)
	if err := os.Remove(sink.path); err != nil {
		return errToAdbcErr(adbc.StatusInternal, err, "remove temporary file")
	}
	return nil
}

// arrowFieldToBigQueryField converts an Arrow field to a BigQuery FieldSchema.
func arrowFieldToBigQueryField(field arrow.Field) (*bigquery.FieldSchema, error) {
	bqField := &bigquery.FieldSchema{
		Name:     field.Name,
		Required: !field.Nullable,
	}

	switch field.Type.ID() {
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		bqField.Type = bigquery.BytesFieldType
	case arrow.BOOL:
		bqField.Type = bigquery.BooleanFieldType
	case arrow.DATE32:
		bqField.Type = bigquery.DateFieldType
	case arrow.DECIMAL128, arrow.DECIMAL256:
		dec := field.Type.(arrow.DecimalType)
		bqField.Type = bigquery.NumericFieldType
		bqField.Precision = int64(dec.GetPrecision())
		bqField.Scale = int64(dec.GetScale())
	case arrow.FLOAT32, arrow.FLOAT64:
		bqField.Type = bigquery.FloatFieldType
	case arrow.INT16, arrow.INT32, arrow.INT64:
		bqField.Type = bigquery.IntegerFieldType
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		bqField.Type = bigquery.StringFieldType
	case arrow.TIME32, arrow.TIME64:
		bqField.Type = bigquery.TimeFieldType
	case arrow.TIMESTAMP:
		ts := field.Type.(*arrow.TimestampType)
		if ts.TimeZone != "" {
			bqField.Type = bigquery.TimestampFieldType
		} else {
			bqField.Type = bigquery.DateTimeFieldType
		}
	case arrow.LIST, arrow.LARGE_LIST, arrow.LIST_VIEW, arrow.FIXED_SIZE_LIST:
		child := field.Type.(arrow.NestedType).Fields()[0]
		if child.Type.ID() == arrow.LIST || child.Type.ID() == arrow.LARGE_LIST ||
			child.Type.ID() == arrow.LIST_VIEW || child.Type.ID() == arrow.FIXED_SIZE_LIST {
			return nil, adbc.Error{
				Msg:  "[bigquery] nested lists are not supported",
				Code: adbc.StatusNotImplemented,
			}
		}
		// Recursively convert child field
		childField, err := arrowFieldToBigQueryField(child)
		if err != nil {
			return nil, err
		}
		bqField.Type = childField.Type
		bqField.Repeated = true
		bqField.Precision = childField.Precision
		bqField.Scale = childField.Scale
		bqField.Schema = childField.Schema
	case arrow.STRUCT:
		bqField.Type = bigquery.RecordFieldType
		structType := field.Type.(*arrow.StructType)
		bqField.Schema = make([]*bigquery.FieldSchema, len(structType.Fields()))
		for i, f := range structType.Fields() {
			nested, err := arrowFieldToBigQueryField(f)
			if err != nil {
				return nil, err
			}
			bqField.Schema[i] = nested
		}
	default:
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("[bigquery] Unsupported type %s", field.Type),
			Code: adbc.StatusNotImplemented,
		}
	}

	return bqField, nil
}

// arrowSchemaToBigQuerySchema converts an Arrow schema to a BigQuery schema.
func arrowSchemaToBigQuerySchema(schema *arrow.Schema) (bigquery.Schema, error) {
	bqSchema := make([]*bigquery.FieldSchema, len(schema.Fields()))
	for i, field := range schema.Fields() {
		bqField, err := arrowFieldToBigQueryField(field)
		if err != nil {
			return nil, err
		}
		bqSchema[i] = bqField
	}
	return bqSchema, nil
}

// createTableWithAPI creates or drops a table using the BigQuery Client API.
func createTableWithAPI(
	ctx context.Context,
	client *bigquery.Client,
	queryConfig bigquery.QueryConfig,
	options *driverbase.BulkIngestOptions,
	schema *arrow.Schema,
	ifTableExists driverbase.BulkIngestTableExistsBehavior,
	ifTableMissing driverbase.BulkIngestTableMissingBehavior,
) error {
	// Get table reference
	var dataset *bigquery.Dataset
	if options.CatalogName != "" && options.SchemaName != "" {
		dataset = client.DatasetInProject(options.CatalogName, options.SchemaName)
	} else if options.SchemaName != "" {
		dataset = client.Dataset(options.SchemaName)
	} else {
		dataset = client.Dataset(queryConfig.DefaultDatasetID)
	}
	table := dataset.Table(options.TableName)

	_, err := table.Metadata(ctx)
	tableExists := err == nil

	skipCreate := false
	if tableExists {
		switch ifTableExists {
		case driverbase.BulkIngestTableExistsError:
			return adbc.Error{
				Code: adbc.StatusAlreadyExists,
				Msg:  fmt.Sprintf("[bigquery] table %s already exists", options.TableName),
			}
		case driverbase.BulkIngestTableExistsIgnore:
			skipCreate = true
		case driverbase.BulkIngestTableExistsDrop:
			if err := table.Delete(ctx); err != nil {
				return errToAdbcErr(adbc.StatusInternal, err, "drop table")
			}
			tableExists = false
		}
	}

	switch ifTableMissing {
	case driverbase.BulkIngestTableMissingError:
		if !tableExists {
			return adbc.Error{
				Code: adbc.StatusNotFound,
				Msg:  fmt.Sprintf("[bigquery] Not found: Table %s", options.TableName),
			}
		}
	case driverbase.BulkIngestTableMissingCreate:
		if !skipCreate && !tableExists {
			// Convert Arrow schema to BigQuery schema
			bqSchema, err := arrowSchemaToBigQuerySchema(schema)
			if err != nil {
				return err
			}

			// Create table
			if err := table.Create(ctx, &bigquery.TableMetadata{Schema: bqSchema}); err != nil {
				return errToAdbcErr(adbc.StatusInternal, err, "create table")
			}

			// Wait for eventual consistency
			if err := retry(ctx, "check table availability", func() (bool, error) {
				_, err := table.Metadata(ctx)
				return err == nil, err
			}); err != nil {
				return errToAdbcErr(adbc.StatusInternal, err, "verify table creation")
			}
		}
	}

	return nil
}
