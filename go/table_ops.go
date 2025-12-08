// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License").

// Table-mutation helpers ported from the legacy dbt-labs/arrow-adbc bigquery
// driver. Each helper is dispatched from statement.ExecuteQuery when the
// corresponding statement option is set.

package bigquery

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// emptyResult returns a record reader with an empty schema/no batches, used
// for statement execution paths that produce no result set.
func emptyResult() (array.RecordReader, int64, error) {
	emptySchema := arrow.NewSchema([]arrow.Field{}, nil)
	reader, err := array.NewRecordReader(emptySchema, []arrow.RecordBatch{})
	if err != nil {
		return nil, -1, err
	}
	return reader, 0, nil
}

func (st *statement) executeCopyTable(ctx context.Context) (array.RecordReader, int64, error) {
	if st.copyTableSource == "" || st.copyTableDestination == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[bq] copy_table requires both source and destination to be set",
		}
	}
	source, err := stringToTable(st.cnxn.catalog, st.cnxn.dbSchema, st.copyTableSource)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] invalid source table: %v", err),
		}
	}
	dest, err := stringToTable(st.cnxn.catalog, st.cnxn.dbSchema, st.copyTableDestination)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] invalid destination table: %v", err),
		}
	}
	copier := st.cnxn.client.DatasetInProject(dest.ProjectID, dest.DatasetID).Table(dest.TableID).CopierFrom(
		st.cnxn.client.DatasetInProject(source.ProjectID, source.DatasetID).Table(source.TableID),
	)
	if st.copyTableWriteDisposition != "" {
		wd, err := stringToTableWriteDisposition(st.copyTableWriteDisposition)
		if err != nil {
			return nil, -1, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid copy write disposition: %v", err),
			}
		}
		copier.WriteDisposition = wd
	}
	job, err := copier.Run(ctx)
	if err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "start copy job")
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "wait copy job")
	}
	if err := status.Err(); err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err, "copy job")
	}
	return emptyResult()
}
