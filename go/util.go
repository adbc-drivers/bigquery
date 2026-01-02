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
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/googleapi"
)

func quoteIdentifier(ident string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(ident, "`", "\\`"))
}

// XXX: Google SDK badness.  We can't use Wait here because queries that
// *fail* with a rateLimitExceeded (e.g. too many metadata operations) will
// get the *polling* retried infinitely in Google's SDK (I believe the SDK
// wants to retry "polling for job status" rate limit exceeded but doesn't
// differentiate between them because googleapi.CheckResponse appears to put
// the API error from the response object as an error of the API call, from
// digging around using a debugger.  In other words, it seems to be confusing
// "I got an error that my API request was rate limited" and "I got an error
// that my job was rate limited" because their internal APIs mix both errors
// into a single error path.)
func safeWaitForJob(ctx context.Context, logger *slog.Logger, job *bigquery.Job) (js *bigquery.JobStatus, err error) {
	logger.DebugContext(ctx, "waiting for job", "id", job.ID())
	backoff := gax.Backoff{
		Initial:    50 * time.Millisecond,
		Multiplier: 1.3,
		Max:        60 * time.Second,
	}
	for {
		js, err = func() (*bigquery.JobStatus, error) {
			ctxWithDeadline, cancel := context.WithTimeout(ctx, time.Minute*5)
			defer cancel()
			js, err := job.Status(ctxWithDeadline)
			if err != nil {
				return nil, err
			}
			return js, err
		}()

		if err != nil {
			// Note that we do not retry cancellations because we
			// can't differentiate between our own timeout and the
			// user-supplied deadline. We can retry "rate limited"
			// here because job.Status does not behave like job.Wait
			// and does not put the job's error into the API call's
			// error.
			if isRetryableError(err) {
				duration := backoff.Pause()
				logger.DebugContext(ctx, "retry job", "id", job.ID(), "backoff", duration, "error", err)
				if err := gax.Sleep(ctx, duration); err != nil {
					return nil, err
				}

				continue
			}
			logger.DebugContext(ctx, "job failed", "id", job.ID(), "error", err)
			return nil, errToAdbcErr(adbc.StatusInternal, err, "poll job status")
		}

		if js.Err() != nil || js.Done() {
			break
		}

		duration := backoff.Pause()
		logger.DebugContext(ctx, "job not complete", "id", job.ID(), "backoff", duration)
	}
	logger.DebugContext(ctx, "job complete", "id", job.ID())
	return
}

func isRetryableError(err error) bool {
	// Modeled on retryableError in bigquery.go
	switch {
	case err == nil:
		return false
	case err == io.ErrUnexpectedEOF:
		return true
	case err.Error() == "http2: stream closed":
		return true
	}

	retryableReasons := []string{"backendError", "internalError"}
	switch e := err.(type) {
	case *googleapi.Error:
		var reason string
		if len(e.Errors) > 0 {
			reason = e.Errors[0].Reason

			for _, r := range retryableReasons {
				if r == reason {
					return true
				}
			}
		}

		for _, code := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
			if e.Code == code {
				return true
			}
		}

	case *url.Error:
		for _, r := range []string{"connection refused", "connection reset"} {
			if strings.Contains(e.Error(), r) {
				return true
			}
		}

	case interface{ Temporary() bool }:
		if e.Temporary() {
			return true
		}
	}

	return isRetryableError(errors.Unwrap(err))
}

// errToAdbcErr converts an error to an ADBC error, using the metadata from
// Google API errors if possible and including the supplied context
func errToAdbcErr(defaultStatus adbc.Status, err error, context string, contextArgs ...any) error {
	if errors.Is(err, adbc.Error{}) {
		return err
	}

	adbcErr := adbc.Error{
		Code: defaultStatus,
	}
	var msg strings.Builder
	msg.WriteString("[bq] Could not")
	msg.WriteString(fmt.Sprintf(" %s", fmt.Sprintf(context, contextArgs...)))
	msg.WriteString(": ")

	var apiErr *apierror.APIError
	var bqErr *bigquery.Error
	var httpErr *googleapi.Error
	var urlErr *url.Error
	statusCode := -1
	if errors.As(err, &httpErr) {
		statusCode = httpErr.Code
		msg.WriteString(fmt.Sprintf("%d %s: %s", httpErr.Code, http.StatusText(httpErr.Code), httpErr.Message))
	} else if errors.As(err, &apiErr) {
		// Despite all the structure inside the error, there isn't a great way to
		// extract or map it onto anything (e.g. there are two types of errors
		// depending on whether HTTP or gRPC is used, but you can't actually
		// branch on that because the HTTP error is not exposed to you)
		msg.WriteString(apiErr.Error())
	} else if errors.As(err, &urlErr) {
		cleanURL := urlErr.URL
		if url, err := url.Parse(urlErr.URL); err == nil {
			url.RawQuery = ""
			cleanURL = url.String()
		}
		msg.WriteString(fmt.Sprintf("failed to %s %s", urlErr.Op, cleanURL))
		if urlErr.Err != nil {
			msg.WriteString(fmt.Sprintf(": %s", urlErr.Err.Error()))
		}
	} else if errors.As(err, &bqErr) {
		msg.WriteString(fmt.Sprintf("%s: %s (%s)", bqErr.Reason, bqErr.Message, bqErr.Location))

		switch bqErr.Reason {
		case "accessDenied", "billingNotEnabled", "blocked":
			adbcErr.Code = adbc.StatusUnauthorized
		case "attributeError", "badRequest", "billingTierLimitExceeded", "invalid", "invalidQuery", "invalidUser":
			adbcErr.Code = adbc.StatusInvalidArgument
		case "backendError", "jobBackendError", "jobInternalError", "jobRateLimitExceeded", "quotaExceeded", "rateLimitExceeded", "resourceInUse", "resourcesExceeded":
			adbcErr.Code = adbc.StatusInternal
		case "duplicate":
			adbcErr.Code = adbc.StatusAlreadyExists
		case "notFound", "tableUnavailable":
			adbcErr.Code = adbc.StatusNotFound
		case "notImplemented":
			adbcErr.Code = adbc.StatusNotImplemented
		case "proxyAuthenticationRequired", "responseTooLarge":
			adbcErr.Code = adbc.StatusIO
		case "stopped":
			adbcErr.Code = adbc.StatusCancelled
		case "timeout":
			adbcErr.Code = adbc.StatusTimeout
		}
	} else {
		msg.WriteString(err.Error())
	}

	var authErr *auth.Error
	if statusCode <= 0 && errors.As(err, &authErr) {
		statusCode = authErr.Response.StatusCode
	}

	switch statusCode {
	case http.StatusBadRequest:
		adbcErr.Code = adbc.StatusInvalidArgument
	case http.StatusNotFound:
		adbcErr.Code = adbc.StatusNotFound
	case http.StatusUnauthorized:
		adbcErr.Code = adbc.StatusUnauthorized
	}

	adbcErr.Msg = msg.String()
	return adbcErr
}
