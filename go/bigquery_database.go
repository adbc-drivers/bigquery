// Copyright (c) 2025 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package bigquery

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/api/option"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	authType        string
	credentialsType option.CredentialsType
	credentials     string
	clientID        string
	clientSecret    string
	refreshToken    string

	impersonateTargetPrincipal string
	impersonateDelegates       []string
	impersonateScopes          []string
	impersonateLifetime        time.Duration

	// projectID is the catalog
	projectID string
	// datasetID is the schema
	datasetID       string
	tableID         string
	location        string
	quotaProject    string
	endpoint        string
	storageEndpoint string

	bulkIngestMethod      string
	bulkIngestCompression string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.ConnectionWithContext, error) {
	conn := &connectionImpl{
		ConnectionImplBase:         driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		authType:                   d.authType,
		credentialsType:            d.credentialsType,
		credentials:                d.credentials,
		clientID:                   d.clientID,
		clientSecret:               d.clientSecret,
		refreshToken:               d.refreshToken,
		impersonateTargetPrincipal: d.impersonateTargetPrincipal,
		impersonateDelegates:       d.impersonateDelegates,
		impersonateScopes:          d.impersonateScopes,
		impersonateLifetime:        d.impersonateLifetime,
		tableID:                    d.tableID,
		catalog:                    d.projectID,
		dbSchema:                   d.datasetID,
		location:                   d.location,
		endpoint:                   d.endpoint,
		storageEndpoint:            d.storageEndpoint,
		resultRecordBufferSize:     defaultQueryResultBufferSize,
		prefetchConcurrency:        defaultQueryPrefetchConcurrency,
		quotaProject:               d.quotaProject,
		bulkIngestMethod:           d.bulkIngestMethod,
		bulkIngestCompression:      d.bulkIngestCompression,
	}

	err := conn.newClient(ctx)
	if err != nil {
		return nil, err
	}

	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
}

func (d *databaseImpl) Close(ctx context.Context) error { return nil }

func (d *databaseImpl) GetOption(ctx context.Context, key string) (string, error) {
	key = remapOption(key)
	switch key {
	case OptionAuthType:
		return d.authType, nil
	case OptionAuthCredentialsType:
		return string(d.credentialsType), nil
	case OptionAuthCredentials:
		return d.credentials, nil
	case OptionAuthClientID:
		return d.clientID, nil
	case OptionAuthClientSecret:
		return d.clientSecret, nil
	case OptionAuthRefreshToken:
		return d.refreshToken, nil
	case OptionAuthQuotaProject:
		return d.quotaProject, nil
	case OptionLocation:
		return d.location, nil
	case OptionProjectID:
		return d.projectID, nil
	case OptionDatasetID:
		return d.datasetID, nil
	case OptionTableID:
		return d.tableID, nil
	case OptionEndpoint:
		return d.endpoint, nil
	case OptionStorageEndpoint:
		return d.storageEndpoint, nil
	case OptionImpersonateLifetime:
		if d.impersonateLifetime == 0 {
			// If no lifetime is set but impersonation is enabled, return the default
			if d.hasImpersonationOptions() {
				return (3600 * time.Second).String(), nil
			}
			return "", nil
		}
		return d.impersonateLifetime.String(), nil
	case OptionBulkIngestMethod:
		if d.bulkIngestMethod == "" {
			return OptionValueBulkIngestMethodLoad, nil
		}
		return d.bulkIngestMethod, nil
	case OptionBulkIngestCompression:
		if d.bulkIngestCompression == "" {
			return OptionValueCompressionNone, nil
		}
		return d.bulkIngestCompression, nil
	default:
		return d.DatabaseImplBase.GetOption(ctx, key)
	}
}

func (d *databaseImpl) SetOptions(ctx context.Context, options map[string]string) error {
	// Process "uri" first so that URI-parsed defaults (e.g. auth_type=ADC) are
	// set before any explicit options. Subsequent options will then override
	// those defaults, regardless of Go's non-deterministic map iteration order.
	if uri, ok := options["uri"]; ok {
		if err := d.SetOption(ctx, "uri", uri); err != nil {
			return err
		}
	}
	for k, v := range options {
		if k == "uri" {
			continue // already processed above
		}
		err := d.SetOption(ctx, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *databaseImpl) hasImpersonationOptions() bool {
	return d.impersonateTargetPrincipal != "" ||
		len(d.impersonateDelegates) > 0 ||
		len(d.impersonateScopes) > 0
}

func (d *databaseImpl) SetOption(ctx context.Context, key string, value string) error {
	key = remapOption(key)
	switch key {
	case "uri":
		params, err := ParseBigQueryURIToParams(value)
		if err != nil {
			return err
		}

		for paramKey, paramValue := range params {
			if err := d.SetOption(ctx, paramKey, paramValue); err != nil {
				return err
			}
		}
		return nil
	case OptionAuthType:
		value = remapOption(value)
		switch value {
		case OptionValueAuthTypeDefault,
			OptionValueAuthTypeJSONCredentialFile,
			OptionValueAuthTypeJSONCredentialString,
			OptionValueAuthTypeUserAuthentication,
			OptionValueAuthTypeAppDefaultCredentials,
			OptionValueAuthTypeAnonymous:
			d.authType = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] unknown database auth type value `%s`", value),
			}
		}
	case OptionAuthCredentialsType:
		value = remapOption(value)
		// N.B. ExternalAccountAuthorizedUser, GDCHServiceAccount aren't re-exported by google.golang.org/api/option
		switch value {
		case string(option.ServiceAccount):
			d.credentialsType = option.ServiceAccount
		case string(option.AuthorizedUser):
			d.credentialsType = option.AuthorizedUser
		case string(option.ImpersonatedServiceAccount):
			d.credentialsType = option.ImpersonatedServiceAccount
		case string(option.ExternalAccount):
			d.credentialsType = option.ExternalAccount
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] unknown %s=%s", key, value),
			}
		}
	case OptionAuthCredentials:
		d.credentials = value
	case OptionAuthClientID:
		d.clientID = value
	case OptionAuthClientSecret:
		d.clientSecret = value
	case OptionAuthRefreshToken:
		d.refreshToken = value
	case OptionAuthQuotaProject:
		d.quotaProject = value
	case OptionImpersonateTargetPrincipal:
		d.impersonateTargetPrincipal = value
	case OptionImpersonateDelegates:
		d.impersonateDelegates = strings.Split(value, ",")
	case OptionImpersonateScopes:
		d.impersonateScopes = strings.Split(value, ",")
	case OptionImpersonateLifetime:
		duration, err := time.ParseDuration(value)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid impersonate lifetime value `%s`: %v", value, err),
			}
		}
		d.impersonateLifetime = duration
	case OptionProjectID:
		d.projectID = value
	case OptionDatasetID:
		d.datasetID = value
	case OptionTableID:
		d.tableID = value
	case OptionEndpoint:
		d.endpoint = value
	case OptionStorageEndpoint:
		d.storageEndpoint = value
	case OptionLocation:
		d.location = value
	case OptionBulkIngestMethod:
		if value != OptionValueBulkIngestMethodLoad &&
			value != OptionValueBulkIngestMethodStorageWrite {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid bulk ingest method: %s (expected %s or %s)", value, OptionValueBulkIngestMethodLoad, OptionValueBulkIngestMethodStorageWrite),
			}
		}
		d.bulkIngestMethod = value
	case OptionBulkIngestCompression:
		if value != OptionValueCompressionNone &&
			value != OptionValueCompressionLZ4 &&
			value != OptionValueCompressionZSTD {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid bulk ingest compression: %s (expected %s, %s, or %s)", value, OptionValueCompressionNone, OptionValueCompressionLZ4, OptionValueCompressionZSTD),
			}
		}
		d.bulkIngestCompression = value
	default:
		return d.DatabaseImplBase.SetOption(ctx, key, value)
	}
	return nil
}

// ParseBigQueryURIToParams parses a BigQuery URI and returns the extracted parameters
func ParseBigQueryURIToParams(uri string) (map[string]string, error) {
	if uri == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[bq] URI cannot be empty",
		}
	}

	parsedURI, err := url.Parse(uri)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] invalid BigQuery URI format: %v", err),
		}
	}

	if parsedURI.Scheme != "bigquery" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] invalid BigQuery URI scheme: expected 'bigquery', got '%s'", parsedURI.Scheme),
		}
	}

	projectID := strings.TrimPrefix(parsedURI.Path, "/")
	if projectID == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[bq] project ID is required in URI path",
		}
	}

	params := make(map[string]string)
	params[OptionProjectID] = projectID

	// Handle host and port
	var endpoint string
	if parsedURI.Host != "" && parsedURI.Hostname() != "" {
		// Custom endpoint specified with valid hostname
		if parsedURI.Port() != "" {
			endpoint = parsedURI.Host
		} else {
			endpoint = fmt.Sprintf("%s:443", parsedURI.Hostname())
		}
	} else if parsedURI.Host != "" && parsedURI.Hostname() == "" && parsedURI.Port() != "" {
		// Port without hostname. use default host with custom port
		endpoint = fmt.Sprintf("bigquery.googleapis.com:%s", parsedURI.Port())
	} else {
		// No host specified, use default BigQuery endpoint
		endpoint = "bigquery.googleapis.com:443"
	}

	// Store endpoint as hostname:port (Google client library handles https:// internally)
	params[OptionEndpoint] = endpoint

	queryParams := parsedURI.Query()

	oauthTypeStr := queryParams.Get("OAuthType")
	if oauthTypeStr != "" {
		oauthType, err := strconv.Atoi(oauthTypeStr)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid OAuthType value: %s", oauthTypeStr),
			}
		}

		switch oauthType {
		case 0:
			params[OptionAuthType] = OptionValueAuthTypeAppDefaultCredentials
		case 1:
			params[OptionAuthType] = OptionValueAuthTypeJSONCredentialFile

			authCredentials := queryParams.Get("AuthCredentials")
			if authCredentials == "" {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "[bq] AuthCredentials required for service account authentication",
				}
			}

			decodedCreds, err := url.QueryUnescape(authCredentials)
			if err != nil {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[bq] invalid AuthCredentials format: %v", err),
				}
			}
			params[OptionAuthCredentials] = decodedCreds
			queryParams.Del("AuthCredentials")
		case 2:
			params[OptionAuthType] = OptionValueAuthTypeJSONCredentialString

			authCredentials := queryParams.Get("AuthCredentials")
			if authCredentials == "" {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "[bq] AuthCredentials required for service account authentication",
				}
			}

			decodedCreds, err := url.QueryUnescape(authCredentials)
			if err != nil {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[bq] invalid AuthCredentials format: %v", err),
				}
			}
			params[OptionAuthCredentials] = decodedCreds
			queryParams.Del("AuthCredentials")
		case 3:
			params[OptionAuthType] = OptionValueAuthTypeUserAuthentication

			clientID := queryParams.Get("AuthClientId")
			clientSecret := queryParams.Get("AuthClientSecret")
			refreshToken := queryParams.Get("AuthRefreshToken")
			if clientID == "" || clientSecret == "" || refreshToken == "" {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "[bq] AuthClientId, AuthClientSecret and AuthRefreshToken required for OAuth authentication",
				}
			}
			params[OptionAuthClientID] = clientID
			params[OptionAuthClientSecret] = clientSecret
			params[OptionAuthRefreshToken] = refreshToken
			queryParams.Del("AuthClientId")
			queryParams.Del("AuthClientSecret")
			queryParams.Del("AuthRefreshToken")
		default:
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid OAuthType value: %d", oauthType),
			}
		}
		queryParams.Del("OAuthType")
	} else {
		// if not provided default to ADC
		params[OptionAuthType] = OptionValueAuthTypeAppDefaultCredentials
	}

	parameterMap := map[string]string{
		"DatasetId":    OptionDatasetID,
		"Location":     OptionLocation,
		"TableId":      OptionTableID,
		"QuotaProject": OptionAuthQuotaProject,

		// Auth parameters - processed in OAuthType switch above, here for consistency
		"AuthCredentials":  OptionAuthCredentials,
		"AuthClientId":     OptionAuthClientID,
		"AuthClientSecret": OptionAuthClientSecret,
		"AuthRefreshToken": OptionAuthRefreshToken,

		"ImpersonateTargetPrincipal": OptionImpersonateTargetPrincipal,
		"ImpersonateDelegates":       OptionImpersonateDelegates,
		"ImpersonateScopes":          OptionImpersonateScopes,
		"ImpersonateLifetime":        OptionImpersonateLifetime,
	}

	// Process all query parameters to convert URI params to option constants
	for paramName, paramValues := range queryParams {
		if optionName, exists := parameterMap[paramName]; exists {
			params[optionName] = paramValues[0]
		} else {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] unknown parameter '%s' in URI", paramName),
			}
		}
	}

	return params, nil
}
