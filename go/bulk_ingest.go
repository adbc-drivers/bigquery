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
	"strings"

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

func (sink *bigqueryBulkIngestSink) Rows() int64 {
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
	logger      *slog.Logger
	options     driverbase.BulkIngestOptions
	queryConfig bigquery.QueryConfig
	client      *bigquery.Client

	tmpdir string
}

func (bi *bigqueryBulkIngestImpl) Init() error {
	tmpdir, err := os.MkdirTemp("", fmt.Sprintf("bq-bulk-%s", bi.options.TableName))
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "create temporary directory for bulk ingest")
	}
	bi.tmpdir = tmpdir
	return nil
}

func (bi *bigqueryBulkIngestImpl) Close() {
	if bi.tmpdir != "" {
		if err := os.RemoveAll(bi.tmpdir); err != nil {
			bi.logger.Error("failed to remove temporary directory", "dir", bi.tmpdir, "err", err)
		}
	}
	bi.tmpdir = ""
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
	status, err := job.Wait(ctx)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "load data")
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
	if stmt, err := createTableStatement(&bi.options, schema, ifTableExists, ifTableMissing); err != nil {
		return err
	} else if stmt != "" {
		bi.logger.Debug("creating table", "table", bi.options.TableName, "stmt", stmt)
		query := bi.client.Query("")
		query.QueryConfig = bi.queryConfig
		query.Q = stmt
		job, err := query.Run(ctx)
		if err != nil {
			return err
		}
		js, err := job.Wait(ctx)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err, "create table")
		} else if err = js.Err(); err != nil {
			return errToAdbcErr(adbc.StatusInternal, err, "create table")
		} else if !js.Done() {
			return adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[bq] CREATE TABLE query did not complete",
			}
		}
		bi.logger.Debug("created table", "table", bi.options.TableName)
	}
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

func writeFields(b *strings.Builder, fields []arrow.Field, skipName bool) error {
	for i, field := range fields {
		if i > 0 {
			b.WriteString(", ")
		}

		if !skipName {
			b.WriteString(quoteIdentifier(field.Name))
		}

		switch field.Type.ID() {
		case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
			b.WriteString(" BYTES")
		case arrow.BOOL:
			b.WriteString(" BOOLEAN")
		case arrow.DATE32:
			b.WriteString(" DATE")
		case arrow.DECIMAL128, arrow.DECIMAL256:
			dec := field.Type.(arrow.DecimalType)
			fmt.Fprintf(b, "NUMERIC(%d, %d)", dec.GetPrecision(), dec.GetScale())
		case arrow.FLOAT32:
			b.WriteString(" FLOAT64")
		case arrow.FLOAT64:
			b.WriteString(" FLOAT64")
		case arrow.INT16, arrow.INT32, arrow.INT64:
			b.WriteString(" INT64")
		case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
			b.WriteString(" STRING")
		case arrow.TIME32, arrow.TIME64:
			b.WriteString(" TIME")
		case arrow.TIMESTAMP:
			ts := field.Type.(*arrow.TimestampType)
			if ts.TimeZone != "" {
				b.WriteString(" TIMESTAMP")
			} else {
				b.WriteString(" DATETIME")
			}
		case arrow.LIST, arrow.LARGE_LIST, arrow.LIST_VIEW, arrow.FIXED_SIZE_LIST:
			child := field.Type.(arrow.NestedType).Fields()[0]
			if child.Type.ID() == arrow.LIST || child.Type.ID() == arrow.LARGE_LIST || child.Type.ID() == arrow.LIST_VIEW || child.Type.ID() == arrow.FIXED_SIZE_LIST {
				return adbc.Error{
					Msg:  "[bigquery] nested lists are not supported",
					Code: adbc.StatusNotImplemented,
				}
			}

			b.WriteString(" ARRAY<")
			if err := writeFields(b, []arrow.Field{child}, true); err != nil {
				return err
			}
			b.WriteString(">")
		case arrow.STRUCT:
			b.WriteString(" STRUCT<")
			if err := writeFields(b, field.Type.(*arrow.StructType).Fields(), false); err != nil {
				return err
			}
			b.WriteString(">")
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[bigquery] Unsupported type %s", field.Type),
				Code: adbc.StatusNotImplemented,
			}
		}

		if !field.Nullable {
			b.WriteString(" NOT NULL")
		}
	}
	return nil
}

func createTableStatement(options *driverbase.BulkIngestOptions, schema *arrow.Schema, ifTableExists driverbase.BulkIngestTableExistsBehavior, ifTableMissing driverbase.BulkIngestTableMissingBehavior) (string, error) {
	var b strings.Builder

	switch ifTableExists {
	case driverbase.BulkIngestTableExistsError:
		// Do nothing
	case driverbase.BulkIngestTableExistsIgnore:
		// Do nothing
	case driverbase.BulkIngestTableExistsDrop:
		b.WriteString("DROP TABLE IF EXISTS ")
		b.WriteString(quoteIdentifier(options.TableName))
		b.WriteString("; ")
	}

	switch ifTableMissing {
	case driverbase.BulkIngestTableMissingError:
		// Do nothing
	case driverbase.BulkIngestTableMissingCreate:
		b.WriteString("CREATE TABLE ")
		if ifTableExists == driverbase.BulkIngestTableExistsIgnore {
			b.WriteString("IF NOT EXISTS ")
		}
		if options.CatalogName != "" {
			b.WriteString(quoteIdentifier(options.CatalogName))
			b.WriteString(".")
		}
		if options.SchemaName != "" {
			b.WriteString(quoteIdentifier(options.SchemaName))
			b.WriteString(".")
		}
		b.WriteString(quoteIdentifier(options.TableName))
		b.WriteString(" (")

		if err := writeFields(&b, schema.Fields(), false); err != nil {
			return "", err
		}

		b.WriteString(")")
	}
	return b.String(), nil
}
