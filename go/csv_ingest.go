// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License").

// Legacy CSV-file ingest path carried over from the legacy arrow-adbc bigquery
// driver. Triggered by setting OptionStringIngestPath. New callers should use
// the standard ADBC bulk ingest API instead.

package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// loadExplicitSchema parses an Arrow IPC-encoded schema and stores the
// resulting BigQuery schema for use during CSV ingest. Used by both
// SetOptionBytes and the string-encoded fallback in SetOption.
func (st *statement) loadExplicitSchema(_ context.Context, ipcBytes []byte) error {
	r, err := ipc.NewReader(bytes.NewReader(ipcBytes))
	if err != nil {
		return err
	}
	defer r.Release()

	schema, err := arrowSchemaToBQ(r.Schema())
	if err != nil {
		return err
	}
	st.explicitSchema = schema
	return nil
}

func (st *statement) executeCSVIngest(ctx context.Context) (array.RecordReader, int64, error) {
	if st.ingestPath == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[bq] cannot execute CSV ingest without a file path",
		}
	}
	file, err := os.Open(st.ingestPath)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] open %q: %w", st.ingestPath, err)
	}
	defer file.Close()

	if st.queryConfig.Dst == nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[bq] CSV ingest requires a destination table; set OptionStringQueryDestinationTable",
		}
	}
	if st.queryConfig.Dst.ProjectID == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[bq] CSV ingest destination missing ProjectID",
		}
	}

	loadSource := bigquery.NewReaderSource(file)
	loader := st.queryConfig.Dst.LoaderFrom(loadSource)
	loader.WriteDisposition = st.queryConfig.WriteDisposition

	fileCfg := &loader.Src.(*bigquery.ReaderSource).FileConfig
	fileCfg.SourceFormat = bigquery.CSV
	fileCfg.SkipLeadingRows = 1
	fileCfg.FieldDelimiter = st.ingestFileDelimiter

	if st.explicitSchema != nil {
		fileCfg.Schema = st.explicitSchema
		fileCfg.AutoDetect = false
	} else {
		fileCfg.AutoDetect = true
	}

	job, err := loader.Run(ctx)
	if err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "start CSV load job")
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "wait CSV load job")
	}
	if err := status.Err(); err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "CSV load job")
	}
	return emptyResult()
}

// arrowSchemaToBQ converts an Arrow schema into BigQuery field schemas using
// the existing arrowFieldToBigQueryField helper from bulk_ingest.go.
func arrowSchemaToBQ(s *arrow.Schema) ([]*bigquery.FieldSchema, error) {
	out := make([]*bigquery.FieldSchema, 0, len(s.Fields()))
	for _, f := range s.Fields() {
		bq, err := arrowFieldToBigQueryField(f)
		if err != nil {
			return nil, err
		}
		out = append(out, bq)
	}
	return out, nil
}
