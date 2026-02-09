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
	datasetID    string
	tableID      string
	location     string
	quotaProject string
	endpoint     string

	bulkIngestMethod      string
	bulkIngestCompression string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
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

func (d *databaseImpl) Close() error { return nil }

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringAuthType:
		return d.authType, nil
	case OptionAuthCredentialsType:
		return string(d.credentialsType), nil
	case OptionStringAuthCredentials:
		return d.credentials, nil
	case OptionStringAuthClientID:
		return d.clientID, nil
	case OptionStringAuthClientSecret:
		return d.clientSecret, nil
	case OptionStringAuthRefreshToken:
		return d.refreshToken, nil
	case OptionStringAuthQuotaProject:
		return d.quotaProject, nil
	case OptionStringLocation:
		return d.location, nil
	case OptionStringProjectID:
		return d.projectID, nil
	case OptionStringDatasetID:
		return d.datasetID, nil
	case OptionStringTableID:
		return d.tableID, nil
	case OptionStringEndpoint:
		return d.endpoint, nil
	case OptionStringImpersonateLifetime:
		if d.impersonateLifetime == 0 {
			// If no lifetime is set but impersonation is enabled, return the default
			if d.hasImpersonationOptions() {
				return (3600 * time.Second).String(), nil
			}
			return "", nil
		}
		return d.impersonateLifetime.String(), nil
	case OptionStringBulkIngestMethod:
		if d.bulkIngestMethod == "" {
			return OptionValueBulkIngestMethodLoad, nil
		}
		return d.bulkIngestMethod, nil
	case OptionStringBulkIngestCompression:
		if d.bulkIngestCompression == "" {
			return OptionValueCompressionNone, nil
		}
		return d.bulkIngestCompression, nil
	default:
		return d.DatabaseImplBase.GetOption(key)
	}
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	for k, v := range options {
		err := d.SetOption(k, v)
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

func (d *databaseImpl) SetOption(key string, value string) error {
	switch key {
	case "uri":
		params, err := ParseBigQueryURIToParams(value)
		if err != nil {
			return err
		}

		for paramKey, paramValue := range params {
			if err := d.SetOption(paramKey, paramValue); err != nil {
				return err
			}
		}
		return nil
	case OptionStringAuthType:
		switch value {
		case OptionValueAuthTypeDefault,
			OptionValueAuthTypeJSONCredentialFile,
			OptionValueAuthTypeJSONCredentialString,
			OptionValueAuthTypeUserAuthentication,
			OptionValueAuthTypeAppDefaultCredentials:
			d.authType = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] unknown database auth type value `%s`", value),
			}
		}
	case OptionAuthCredentialsType:
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
	case OptionStringAuthCredentials:
		d.credentials = value
	case OptionStringAuthClientID:
		d.clientID = value
	case OptionStringAuthClientSecret:
		d.clientSecret = value
	case OptionStringAuthRefreshToken:
		d.refreshToken = value
	case OptionStringAuthQuotaProject:
		d.quotaProject = value
	case OptionStringImpersonateTargetPrincipal:
		d.impersonateTargetPrincipal = value
	case OptionStringImpersonateDelegates:
		d.impersonateDelegates = strings.Split(value, ",")
	case OptionStringImpersonateScopes:
		d.impersonateScopes = strings.Split(value, ",")
	case OptionStringImpersonateLifetime:
		duration, err := time.ParseDuration(value)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid impersonate lifetime value `%s`: %v", value, err),
			}
		}
		d.impersonateLifetime = duration
	case OptionStringProjectID:
		d.projectID = value
	case OptionStringDatasetID:
		d.datasetID = value
	case OptionStringTableID:
		d.tableID = value
	case OptionStringEndpoint:
		d.endpoint = value
	case OptionStringLocation:
		d.location = value
	case OptionStringBulkIngestMethod:
		if value != OptionValueBulkIngestMethodLoad &&
			value != OptionValueBulkIngestMethodStorageWrite {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid bulk ingest method: %s (expected %s or %s)", value, OptionValueBulkIngestMethodLoad, OptionValueBulkIngestMethodStorageWrite),
			}
		}
		d.bulkIngestMethod = value
	case OptionStringBulkIngestCompression:
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
		return d.DatabaseImplBase.SetOption(key, value)
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
	params[OptionStringProjectID] = projectID

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
	params[OptionStringEndpoint] = endpoint

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
			params[OptionStringAuthType] = OptionValueAuthTypeAppDefaultCredentials
		case 1:
			params[OptionStringAuthType] = OptionValueAuthTypeJSONCredentialFile

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
			params[OptionStringAuthCredentials] = decodedCreds
			queryParams.Del("AuthCredentials")
		case 2:
			params[OptionStringAuthType] = OptionValueAuthTypeJSONCredentialString

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
			params[OptionStringAuthCredentials] = decodedCreds
			queryParams.Del("AuthCredentials")
		case 3:
			params[OptionStringAuthType] = OptionValueAuthTypeUserAuthentication

			clientID := queryParams.Get("AuthClientId")
			clientSecret := queryParams.Get("AuthClientSecret")
			refreshToken := queryParams.Get("AuthRefreshToken")
			if clientID == "" || clientSecret == "" || refreshToken == "" {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "[bq] AuthClientId, AuthClientSecret and AuthRefreshToken required for OAuth authentication",
				}
			}
			params[OptionStringAuthClientID] = clientID
			params[OptionStringAuthClientSecret] = clientSecret
			params[OptionStringAuthRefreshToken] = refreshToken
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
		params[OptionStringAuthType] = OptionValueAuthTypeAppDefaultCredentials
	}

	parameterMap := map[string]string{
		"DatasetId":    OptionStringDatasetID,
		"Location":     OptionStringLocation,
		"TableId":      OptionStringTableID,
		"QuotaProject": OptionStringAuthQuotaProject,

		// Auth parameters - processed in OAuthType switch above, here for consistency
		"AuthCredentials":  OptionStringAuthCredentials,
		"AuthClientId":     OptionStringAuthClientID,
		"AuthClientSecret": OptionStringAuthClientSecret,
		"AuthRefreshToken": OptionStringAuthRefreshToken,

		"ImpersonateTargetPrincipal": OptionStringImpersonateTargetPrincipal,
		"ImpersonateDelegates":       OptionStringImpersonateDelegates,
		"ImpersonateScopes":          OptionStringImpersonateScopes,
		"ImpersonateLifetime":        OptionStringImpersonateLifetime,
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
