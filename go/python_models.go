// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License").

// Python-models Dataproc + GCS support ported from the legacy
// dbt-labs/arrow-adbc bigquery driver. Statement-level "execution mode"
// options let dbt submit python models (via Dataproc batch/job) and upload
// their sources to GCS.

package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"cloud.google.com/go/storage"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
)

// newDataprocBatchClient creates a Dataproc BatchControllerClient using the
// connection's auth options, targeted at the regional dataproc endpoint.
func (c *connectionImpl) newDataprocBatchClient(ctx context.Context, computeRegion string) (*dataproc.BatchControllerClient, error) {
	authOptions, err := c.authOptions(ctx)
	if err != nil {
		return nil, err
	}
	authOptions = append(authOptions, option.WithEndpoint(fmt.Sprintf("%s-dataproc.googleapis.com:443", computeRegion)))
	return dataproc.NewBatchControllerClient(ctx, authOptions...)
}

// newJobControllerClient creates a Dataproc JobControllerClient.
func (c *connectionImpl) newJobControllerClient(ctx context.Context, computeRegion string) (*dataproc.JobControllerClient, error) {
	authOptions, err := c.authOptions(ctx)
	if err != nil {
		return nil, err
	}
	authOptions = append(authOptions, option.WithEndpoint(fmt.Sprintf("%s-dataproc.googleapis.com:443", computeRegion)))
	return dataproc.NewJobControllerClient(ctx, authOptions...)
}

// newGCSClient creates a Google Cloud Storage client.
func (c *connectionImpl) newGCSClient(ctx context.Context) (*storage.Client, error) {
	authOptions, err := c.authOptions(ctx)
	if err != nil {
		return nil, err
	}
	return storage.NewClient(ctx, authOptions...)
}

// executeDataprocCreateBatch parses createBatchReqBatchYML as YAML, converts
// to a dataprocpb.Batch protobuf, and submits it via the Dataproc Batch API.
// Used for dbt python models on serverless Spark.
func (st *statement) executeDataprocCreateBatch(ctx context.Context) (array.RecordReader, int64, error) {
	var intermediate map[string]any
	if err := yaml.Unmarshal([]byte(st.createBatchReqBatchYML), &intermediate); err != nil {
		return nil, -1, fmt.Errorf("[bq] parse batch YAML: %w", err)
	}
	jsonBytes, err := json.Marshal(intermediate)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] parse batch YAML: %w", err)
	}
	batch := &dataprocpb.Batch{}
	if err := protojson.Unmarshal(jsonBytes, batch); err != nil {
		return nil, -1, fmt.Errorf("[bq] unmarshal batch proto: %w", err)
	}

	client, err := st.cnxn.newDataprocBatchClient(ctx, st.dataprocRegion)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] create Dataproc client: %w", err)
	}
	defer client.Close()

	req := &dataprocpb.CreateBatchRequest{
		Parent:  st.createBatchReqParent,
		Batch:   batch,
		BatchId: st.createBatchReqBatchId,
	}
	op, err := client.CreateBatch(ctx, req)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] create batch: %w", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(st.dataprocPoolingTimeout)*time.Second)
	defer cancel()
	if _, err := op.Wait(waitCtx); err != nil {
		return nil, -1, fmt.Errorf("[bq] batch failed or timed out: %w", err)
	}
	return emptyResult()
}

// executeSubmitJobAsOperation submits a Dataproc Spark/PySpark job on a
// pre-existing cluster. Used for dbt python models on standing Spark clusters.
func (st *statement) executeSubmitJobAsOperation(ctx context.Context) (array.RecordReader, int64, error) {
	client, err := st.cnxn.newJobControllerClient(ctx, st.dataprocRegion)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] create Dataproc JobController client: %w", err)
	}
	defer client.Close()

	req := &dataprocpb.SubmitJobRequest{
		ProjectId: st.dataprocProject,
		Region:    st.dataprocRegion,
		Job: &dataprocpb.Job{
			Placement: &dataprocpb.JobPlacement{ClusterName: st.submitJobReqClusterName},
			TypeJob: &dataprocpb.Job_PysparkJob{
				PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: st.submitJobReqGCSPath},
			},
		},
	}
	op, err := client.SubmitJobAsOperation(ctx, req)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] submit Dataproc job: %w", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(st.dataprocPoolingTimeout)*time.Second)
	defer cancel()
	resp, err := op.Wait(waitCtx)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] dataproc job failed or timed out: %w", err)
	}
	if resp.GetStatus() != nil && resp.GetStatus().GetState() == dataprocpb.JobStatus_ERROR {
		return nil, -1, fmt.Errorf("[bq] dataproc job error: %s", resp.GetStatus().GetDetails())
	}
	return emptyResult()
}

// writeToGCS uploads writeGCSContent to the bucket/object specified by
// writeGCSBucket/writeGCSObjectName. Used to stage python-model sources
// before submitting a Dataproc job or notebook execution.
func (st *statement) writeToGCS(ctx context.Context) (array.RecordReader, int64, error) {
	client, err := st.cnxn.newGCSClient(ctx)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] create GCS client: %w", err)
	}
	defer client.Close()

	wc := client.Bucket(st.writeGCSBucket).Object(st.writeGCSObjectName).NewWriter(ctx)
	if _, err := wc.Write([]byte(st.writeGCSContent)); err != nil {
		return nil, -1, fmt.Errorf("[bq] write to GCS object: %w", err)
	}
	if err := wc.Close(); err != nil {
		return nil, -1, fmt.Errorf("[bq] close GCS writer: %w", err)
	}
	return emptyResult()
}
