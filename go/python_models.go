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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	aiplatform "cloud.google.com/go/aiplatform/apiv1"
	"cloud.google.com/go/aiplatform/apiv1/aiplatformpb"
	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"cloud.google.com/go/storage"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
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

// newNotebookClient creates an AI-Platform NotebookClient.
func (c *connectionImpl) newNotebookClient(ctx context.Context, computeRegion string) (*aiplatform.NotebookClient, error) {
	authOptions, err := c.authOptions(ctx)
	if err != nil {
		return nil, err
	}
	authOptions = append(authOptions, option.WithEndpoint(fmt.Sprintf("%s-aiplatform.googleapis.com:443", computeRegion)))
	return aiplatform.NewNotebookClient(ctx, authOptions...)
}

// addExecutionIdentityDetails populates the NotebookExecutionJob's
// ExecutionIdentity based on the connection's auth configuration.
func (c *connectionImpl) addExecutionIdentityDetails(ctx context.Context, job *aiplatformpb.NotebookExecutionJob) (*aiplatformpb.NotebookExecutionJob, error) {
	switch c.authType {
	case OptionValueAuthTypeJSONCredentialFile:
		data, err := os.ReadFile(c.credentials)
		if err != nil {
			return nil, fmt.Errorf("read credential file: %w", err)
		}
		var sa struct {
			ClientEmail string `json:"client_email"`
		}
		if err := json.Unmarshal(data, &sa); err != nil {
			return nil, fmt.Errorf("parse credential JSON: %w", err)
		}
		job.ExecutionIdentity = &aiplatformpb.NotebookExecutionJob_ServiceAccount{ServiceAccount: sa.ClientEmail}
		return job, nil
	case OptionValueAuthTypeJSONCredentialString, OptionValueAuthTypeJSONCredentials:
		var sa struct {
			ClientEmail string `json:"client_email"`
		}
		if err := json.Unmarshal([]byte(c.credentials), &sa); err != nil {
			return nil, fmt.Errorf("parse credential JSON: %w", err)
		}
		job.ExecutionIdentity = &aiplatformpb.NotebookExecutionJob_ServiceAccount{ServiceAccount: sa.ClientEmail}
		return job, nil
	case OptionValueAuthTypeDefault,
		OptionValueAuthTypeAppDefaultCredentials,
		OptionValueAuthTypeUserAuthentication,
		OptionValueAuthTypeTemporaryAccessToken,
		"":
		if c.impersonateTargetPrincipal != "" {
			job.ExecutionIdentity = &aiplatformpb.NotebookExecutionJob_ServiceAccount{ServiceAccount: c.impersonateTargetPrincipal}
			return job, nil
		}
		authOptions, err := c.authOptions(ctx)
		if err != nil {
			return nil, err
		}
		ts, _, err := transport.NewHTTPClient(ctx, append(authOptions, option.WithScopes("https://www.googleapis.com/auth/userinfo.email"))...)
		if err != nil {
			return nil, fmt.Errorf("build http transport: %w", err)
		}
		var tokenSource oauth2.TokenSource = oauth2.StaticTokenSource(&oauth2.Token{})
		if t, ok := ts.Transport.(*oauth2.Transport); ok {
			tokenSource = t.Source
		}
		token, err := tokenSource.Token()
		if err != nil {
			return nil, fmt.Errorf("fetch token: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, "GET", "https://www.googleapis.com/oauth2/v2/userinfo", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Add("Authorization", "Bearer "+token.AccessToken)
		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			return nil, fmt.Errorf("call userinfo: %w", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("userinfo returned %d: %s", resp.StatusCode, string(body))
		}
		var data map[string]any
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, err
		}
		email, _ := data["email"].(string)
		if email == "" {
			return nil, errors.New("userinfo response did not include an email")
		}
		if strings.HasSuffix(email, "iam.gserviceaccount.com") {
			job.ExecutionIdentity = &aiplatformpb.NotebookExecutionJob_ServiceAccount{ServiceAccount: email}
		} else {
			job.ExecutionIdentity = &aiplatformpb.NotebookExecutionJob_ExecutionUser{ExecutionUser: email}
		}
		return job, nil
	default:
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] unsupported credential method in BigFrames: %s", c.authType),
		}
	}
}

func (st *statement) getNotebookTemplateName(ctx context.Context) (string, error) {
	client, err := st.cnxn.newNotebookClient(ctx, st.createNotebookExecuteJobRegion)
	if err != nil {
		return "", fmt.Errorf("[bq] create notebook client: %w", err)
	}
	defer client.Close()

	req := &aiplatformpb.ListNotebookRuntimeTemplatesRequest{
		Parent: st.createNotebookExecuteJobParent,
		Filter: "notebookRuntimeType = ONE_CLICK",
	}
	it := client.ListNotebookRuntimeTemplates(ctx, req)
	tmpl, err := it.Next()
	if err == iterator.Done {
		template := &aiplatformpb.NotebookRuntimeTemplate{
			DisplayName:         "default-one-click-notebook",
			NotebookRuntimeType: aiplatformpb.NotebookRuntimeType_ONE_CLICK,
			MachineSpec:         &aiplatformpb.MachineSpec{MachineType: "e2-standard-4"},
			NetworkSpec: &aiplatformpb.NetworkSpec{
				EnableInternetAccess: false,
				Network:              fmt.Sprintf("projects/%s/global/networks/default", st.createNotebookExecuteJobProject),
				Subnetwork:           fmt.Sprintf("projects/%s/regions/%s/subnetworks/default", st.createNotebookExecuteJobProject, st.createNotebookExecuteJobRegion),
			},
		}
		op, err := client.CreateNotebookRuntimeTemplate(ctx, &aiplatformpb.CreateNotebookRuntimeTemplateRequest{
			Parent:                  st.createNotebookExecuteJobParent,
			NotebookRuntimeTemplate: template,
		})
		if err != nil {
			return "", fmt.Errorf("[bq] create notebook runtime template: %w", err)
		}
		resp, err := op.Wait(ctx)
		if err != nil {
			return "", fmt.Errorf("[bq] notebook template create op: %w", err)
		}
		return resp.GetName(), nil
	}
	if err != nil {
		return "", fmt.Errorf("[bq] list runtime templates: %w", err)
	}
	return tmpl.GetName(), nil
}

func (st *statement) executeCreateNotebookExecutionJob(ctx context.Context) (array.RecordReader, int64, error) {
	client, err := st.cnxn.newNotebookClient(ctx, st.createNotebookExecuteJobRegion)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] create notebook client: %w", err)
	}
	defer client.Close()

	templateName := ""
	if st.createNotebookExecuteJobTemplateId != "" {
		templateName = fmt.Sprintf(
			"projects/%s/locations/%s/notebookRuntimeTemplates/%s",
			st.createNotebookExecuteJobProject, st.createNotebookExecuteJobRegion, st.createNotebookExecuteJobTemplateId,
		)
	} else {
		templateName, err = st.getNotebookTemplateName(ctx)
		if err != nil {
			return nil, -1, err
		}
	}

	job := &aiplatformpb.NotebookExecutionJob{
		NotebookSource: &aiplatformpb.NotebookExecutionJob_GcsNotebookSource_{
			GcsNotebookSource: &aiplatformpb.NotebookExecutionJob_GcsNotebookSource{Uri: st.createNotebookExecuteJobGscPath},
		},
		ExecutionSink: &aiplatformpb.NotebookExecutionJob_GcsOutputUri{
			GcsOutputUri: fmt.Sprintf("gs://%s/%s/logs", st.createNotebookExecuteJobGCSBucket, st.createNotebookExecuteJobModelFileName),
		},
		DisplayName: st.createBatchReqBatchId,
		EnvironmentSpec: &aiplatformpb.NotebookExecutionJob_NotebookRuntimeTemplateResourceName{
			NotebookRuntimeTemplateResourceName: templateName,
		},
	}
	job, err = st.cnxn.addExecutionIdentityDetails(ctx, job)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] add execution identity: %w", err)
	}
	op, err := client.CreateNotebookExecutionJob(ctx, &aiplatformpb.CreateNotebookExecutionJobRequest{
		Parent:               st.createNotebookExecuteJobParent,
		NotebookExecutionJob: job,
	})
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] create notebook execution job: %w", err)
	}

	lroName := op.Name()
	parts := strings.Split(lroName, "/operations/")
	jobName := parts[0]

	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(st.dataprocPoolingTimeout)*time.Second)
	defer cancel()
	retrievedJob, err := op.Wait(waitCtx)
	if err != nil {
		return nil, -1, fmt.Errorf("[bq] notebook execution op: %w", err)
	}

	elapsed := time.Duration(0)
	for {
		retrievedJob, err = client.GetNotebookExecutionJob(ctx, &aiplatformpb.GetNotebookExecutionJobRequest{Name: jobName})
		if err != nil {
			return nil, -1, fmt.Errorf("[bq] get notebook execution job: %w", err)
		}
		s := retrievedJob.JobState
		if s == aiplatformpb.JobState_JOB_STATE_SUCCEEDED ||
			s == aiplatformpb.JobState_JOB_STATE_PARTIALLY_SUCCEEDED ||
			s == aiplatformpb.JobState_JOB_STATE_FAILED ||
			s == aiplatformpb.JobState_JOB_STATE_CANCELLED ||
			s == aiplatformpb.JobState_JOB_STATE_EXPIRED {
			break
		}
		if elapsed >= time.Duration(st.dataprocPoolingTimeout)*time.Second {
			return nil, -1, fmt.Errorf("[bq] notebook job did not complete within %v s; cancel manually via GCP console", st.dataprocPoolingTimeout)
		}
		time.Sleep(30 * time.Second)
		elapsed += 30 * time.Second
	}

	parts = strings.Split(jobName, "/")
	jobID := parts[len(parts)-1]
	gcsLogURI := fmt.Sprintf("%s/%s/%s.py", retrievedJob.GetGcsOutputUri(), jobID, st.createNotebookExecuteJobModelName)

	gcsClient, err := st.cnxn.newGCSClient(ctx)
	if err == nil {
		defer gcsClient.Close()
		if data, err := readJSONFromGCS(ctx, gcsLogURI, gcsClient); err == nil && data != nil {
			processGCSNotebookLog(st.cnxn.Logger, data)
		}
	}

	switch retrievedJob.GetJobState() {
	case aiplatformpb.JobState_JOB_STATE_SUCCEEDED:
		st.cnxn.Logger.Info("colab notebook execution job finished successfully", "name", retrievedJob.GetName())
	case aiplatformpb.JobState_JOB_STATE_FAILED:
		return nil, -1, fmt.Errorf("[bq] colab notebook execution job '%s' failed", retrievedJob.GetName())
	default:
		return nil, -1, fmt.Errorf("[bq] colab notebook execution job '%s' finished with unexpected state: %s", retrievedJob.GetName(), retrievedJob.GetJobState().String())
	}
	return emptyResult()
}

func readJSONFromGCS(ctx context.Context, gcsURI string, storageClient *storage.Client) (any, error) {
	parts := strings.SplitN(strings.TrimPrefix(gcsURI, "gs://"), "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid GCS URI: %s", gcsURI)
	}
	reader, err := storageClient.Bucket(parts[0]).Object(parts[1]).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	var data any
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, nil
	}
	return data, nil
}

func processGCSNotebookLog(logger *slog.Logger, gcsLog any) {
	logMap, ok := gcsLog.(map[string]any)
	if !ok {
		return
	}
	cells, ok := logMap["cells"].([]any)
	if !ok || len(cells) == 0 {
		return
	}
	firstCell, ok := cells[0].(map[string]any)
	if !ok {
		return
	}
	outputs, ok := firstCell["outputs"].([]any)
	if !ok || len(outputs) == 0 {
		return
	}
	var b strings.Builder
	for _, item := range outputs {
		switch v := item.(type) {
		case map[string]any:
			for key, value := range v {
				fmt.Fprintf(&b, "%s: %v\n", key, value)
			}
		default:
			fmt.Fprintf(&b, "%v\n", item)
		}
	}
	logger.Info("colab notebook outputs", "output", b.String())
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
