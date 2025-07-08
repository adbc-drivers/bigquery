// Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.

package bigquery

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"cloud.google.com/go/auth"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/googleapi"
)

func quoteIdentifier(ident string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(ident, "`", "\\`"))
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
