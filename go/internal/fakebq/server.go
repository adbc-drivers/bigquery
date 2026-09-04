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

// Package fakebq implements a small httptest fake of the BigQuery Jobs REST
// API so tests can drive a real *bigquery.Client without credentials.
//
// Minimal Jobs REST fake for credential-free Go tests. Unlike goccy/bigquery-emulator
// (full SQL + Storage API) or the C# BigQueryMockServer (no jobs.cancel), fakebq scripts
// poll states and records RPC order for cancel/rate-limit tests.
package fakebq

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// Kind classifies a recorded Jobs API call.
type Kind string

const (
	KindInsert          Kind = "insert"
	KindGet             Kind = "get"
	KindCancel          Kind = "cancel"
	KindGetQueryResults Kind = "queries.get"
	KindUnknown         Kind = "unknown"
)

// Request is one recorded HTTP call to the fake Jobs API.
type Request struct {
	Method     string
	Path       string
	Project    string
	JobID      string
	Location   string
	Time       time.Time
	StatusCode int
	Kind       Kind
}

// HTTPError is a scripted Jobs API error response.
type HTTPError struct {
	Code    int
	Reason  string
	Message string
}

type jobRecord struct {
	project     string
	jobID       string
	location    string
	states      []string
	errorResult map[string]any
}

// Server is a scripted BigQuery Jobs API endpoint.
type Server struct {
	HTTP *httptest.Server

	mu             sync.Mutex
	requests       []Request
	jobs           map[string]*jobRecord
	nextJobScripts [][]string
	defaultStates  []string
	insertErrors   []HTTPError
	getErrors      []HTTPError
	cancelErrors   []HTTPError
}

var (
	reInsert  = regexp.MustCompile(`^/projects/([^/]+)/jobs$`)
	reGet     = regexp.MustCompile(`^/projects/([^/]+)/jobs/([^/]+)$`)
	reCancel  = regexp.MustCompile(`^/projects/([^/]+)/jobs/([^/]+)/cancel$`)
	reQueries = regexp.MustCompile(`^/projects/([^/]+)/queries/([^/]+)$`)
)

// New starts a fake Jobs API server that is closed when tb finishes.
func New(tb testing.TB) *Server {
	tb.Helper()
	s := &Server{
		jobs:          make(map[string]*jobRecord),
		defaultStates: []string{"DONE"},
	}
	s.HTTP = httptest.NewServer(http.HandlerFunc(s.serve))
	tb.Cleanup(s.HTTP.Close)
	return s
}

// Client returns a real BigQuery client pointed at this fake, with auth disabled.
func (s *Server) Client(ctx context.Context, project string) (*bigquery.Client, error) {
	return bigquery.NewClient(ctx, project,
		option.WithEndpoint(s.HTTP.URL),
		option.WithoutAuthentication(),
	)
}

// MustClient returns a client pointed at this fake and closes it when tb finishes.
func (s *Server) MustClient(tb testing.TB, project string) *bigquery.Client {
	tb.Helper()
	client, err := s.Client(context.Background(), project)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = client.Close() })
	return client
}

// SetDefaultStates sets the poll script used when a job has no dedicated script.
// The last state repeats. Defaults to DONE.
func (s *Server) SetDefaultStates(states ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(states) == 0 {
		states = []string{"DONE"}
	}
	s.defaultStates = append([]string(nil), states...)
}

// ScriptNextJob queues a poll script for the next jobs.insert.
func (s *Server) ScriptNextJob(states ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextJobScripts = append(s.nextJobScripts, append([]string(nil), states...))
}

// SetJobStates sets the poll script for a specific job ID (JobFromID / insert).
func (s *Server) SetJobStates(jobID string, states ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(states) == 0 {
		states = []string{"DONE"}
	}
	s.jobs[jobID] = &jobRecord{
		jobID:  jobID,
		states: append([]string(nil), states...),
	}
}

// NextInsertHTTPError queues an error for the next jobs.insert.
func (s *Server) NextInsertHTTPError(code int, reason, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	enqueueHTTPError(&s.insertErrors, code, reason, message)
}

// NextGetHTTPError queues an error for the next jobs.get.
func (s *Server) NextGetHTTPError(code int, reason, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	enqueueHTTPError(&s.getErrors, code, reason, message)
}

// NextCancelHTTPError queues an error for the next jobs.cancel.
func (s *Server) NextCancelHTTPError(code int, reason, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	enqueueHTTPError(&s.cancelErrors, code, reason, message)
}

func enqueueHTTPError(queue *[]HTTPError, code int, reason, message string) {
	*queue = append(*queue, HTTPError{Code: code, Reason: reason, Message: message})
}

func popHTTPError(queue *[]HTTPError) (HTTPError, bool) {
	if len(*queue) == 0 {
		return HTTPError{}, false
	}
	err := (*queue)[0]
	*queue = (*queue)[1:]
	return err, true
}

// Requests returns a copy of recorded calls in order.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Request, len(s.requests))
	copy(out, s.requests)
	return out
}

// RequestsByKind returns recorded calls of the given kind, in order.
func (s *Server) RequestsByKind(kind Kind) []Request {
	all := s.Requests()
	out := make([]Request, 0)
	for _, r := range all {
		if r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// KindOrder returns the kind of each recorded call in order.
func (s *Server) KindOrder() []Kind {
	all := s.Requests()
	out := make([]Kind, len(all))
	for i, r := range all {
		out[i] = r.Kind
	}
	return out
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	location := r.URL.Query().Get("location")

	switch {
	case r.Method == http.MethodPost && reInsert.MatchString(path):
		m := reInsert.FindStringSubmatch(path)
		s.handleInsert(w, r, m[1], location)
	case r.Method == http.MethodGet && reGet.MatchString(path):
		m := reGet.FindStringSubmatch(path)
		s.handleGet(w, r, m[1], m[2], location)
	case r.Method == http.MethodPost && reCancel.MatchString(path):
		m := reCancel.FindStringSubmatch(path)
		s.handleCancel(w, r, m[1], m[2], location)
	case r.Method == http.MethodGet && reQueries.MatchString(path):
		m := reQueries.FindStringSubmatch(path)
		s.handleQueries(w, r, m[1], m[2], location)
	default:
		s.recordCall(r, "", "", location, http.StatusNotFound, KindUnknown)
		writeAPIError(w, http.StatusNotFound, "notFound", "unknown path "+path)
	}
}

type insertBody struct {
	JobReference struct {
		ProjectID string `json:"projectId"`
		JobID     string `json:"jobId"`
		Location  string `json:"location"`
	} `json:"jobReference"`
}

func (s *Server) handleInsert(w http.ResponseWriter, r *http.Request, project, location string) {
	raw, _ := io.ReadAll(r.Body)
	var body insertBody
	_ = json.Unmarshal(raw, &body)
	jobID := body.JobReference.JobID
	if jobID == "" {
		jobID = "generated-job"
	}
	if location == "" {
		location = body.JobReference.Location
	}

	s.mu.Lock()
	if err, ok := popHTTPError(&s.insertErrors); ok {
		s.mu.Unlock()
		s.recordCall(r, project, jobID, location, err.Code, KindInsert)
		writeAPIError(w, err.Code, err.Reason, err.Message)
		return
	}

	job := s.ensureJobLocked(project, jobID, location)
	if len(s.nextJobScripts) > 0 {
		job.states = s.nextJobScripts[0]
		s.nextJobScripts = s.nextJobScripts[1:]
	}
	state := peekState(job)
	resp := jobResource(job, state)
	s.mu.Unlock()

	s.recordCall(r, project, jobID, location, http.StatusOK, KindInsert)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, project, jobID, location string) {
	s.mu.Lock()
	if err, ok := popHTTPError(&s.getErrors); ok {
		s.mu.Unlock()
		s.recordCall(r, project, jobID, location, err.Code, KindGet)
		writeAPIError(w, err.Code, err.Reason, err.Message)
		return
	}
	job := s.ensureJobLocked(project, jobID, location)
	state := popState(job)
	resp := jobResource(job, state)
	s.mu.Unlock()

	s.recordCall(r, project, jobID, location, http.StatusOK, KindGet)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request, project, jobID, location string) {
	s.mu.Lock()
	if err, ok := popHTTPError(&s.cancelErrors); ok {
		s.mu.Unlock()
		s.recordCall(r, project, jobID, location, err.Code, KindCancel)
		writeAPIError(w, err.Code, err.Reason, err.Message)
		return
	}
	job := s.ensureJobLocked(project, jobID, location)
	job.states = []string{"DONE"}
	job.errorResult = map[string]any{
		"reason":  "stopped",
		"message": "Job execution was cancelled",
	}
	state := peekState(job)
	resp := map[string]any{
		"kind": "bigquery#jobCancelResponse",
		"job":  jobResource(job, state),
	}
	s.mu.Unlock()

	s.recordCall(r, project, jobID, location, http.StatusOK, KindCancel)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleQueries(w http.ResponseWriter, r *http.Request, project, jobID, location string) {
	s.mu.Lock()
	job := s.ensureJobLocked(project, jobID, location)
	state := peekState(job)
	resp := map[string]any{
		"kind":         "bigquery#getQueryResultsResponse",
		"jobComplete":  strings.EqualFold(state, "DONE"),
		"jobReference": jobReference(project, jobID, location),
		"schema": map[string]any{
			"fields": []map[string]any{
				{"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"},
			},
		},
		"rows":      []any{},
		"totalRows": "0",
	}
	s.mu.Unlock()

	s.recordCall(r, project, jobID, location, http.StatusOK, KindGetQueryResults)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) ensureJobLocked(project, jobID, location string) *jobRecord {
	job, ok := s.jobs[jobID]
	if !ok {
		states := append([]string(nil), s.defaultStates...)
		job = &jobRecord{states: states}
		s.jobs[jobID] = job
	}
	job.project = project
	job.jobID = jobID
	if location != "" {
		job.location = location
	}
	return job
}

func peekState(job *jobRecord) string {
	if len(job.states) == 0 {
		return "DONE"
	}
	return job.states[0]
}

func popState(job *jobRecord) string {
	if len(job.states) == 0 {
		return "DONE"
	}
	state := job.states[0]
	if len(job.states) > 1 {
		job.states = job.states[1:]
	}
	return state
}

func (s *Server) recordCall(r *http.Request, project, jobID, location string, code int, kind Kind) {
	s.record(Request{
		Method:     r.Method,
		Path:       r.URL.Path,
		Project:    project,
		JobID:      jobID,
		Location:   location,
		Time:       time.Now(),
		StatusCode: code,
		Kind:       kind,
	})
}

func (s *Server) record(req Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
}

func jobReference(project, jobID, location string) map[string]any {
	ref := map[string]any{
		"projectId": project,
		"jobId":     jobID,
	}
	if location != "" {
		ref["location"] = location
	}
	return ref
}

func jobResource(job *jobRecord, state string) map[string]any {
	status := map[string]any{
		"state": state,
	}
	if job.errorResult != nil {
		copied := make(map[string]any, len(job.errorResult))
		for k, v := range job.errorResult {
			copied[k] = v
		}
		status["errorResult"] = copied
	}
	return map[string]any{
		"kind":         "bigquery#job",
		"id":           job.project + ":" + job.jobID,
		"jobReference": jobReference(job.project, job.jobID, job.location),
		"configuration": map[string]any{
			"query": map[string]any{
				"query":        "SELECT 1",
				"useLegacySql": false,
			},
		},
		"status": status,
		"statistics": map[string]any{
			"creationTime": "1",
			"startTime":    "1",
			"query": map[string]any{
				"statementType":       "SELECT",
				"totalBytesProcessed": "0",
				"numDmlAffectedRows":  "0",
			},
		},
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeAPIError(w http.ResponseWriter, code int, reason, message string) {
	writeJSON(w, code, map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"errors": []map[string]any{
				{"reason": reason, "message": message},
			},
		},
	})
}
