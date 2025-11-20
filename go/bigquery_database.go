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
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	authType     string
	credentials  string
	clientID     string
	clientSecret string
	refreshToken string

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
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	conn := &connectionImpl{
		ConnectionImplBase:         driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		authType:                   d.authType,
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
		resultRecordBufferSize:     defaultQueryResultBufferSize,
		prefetchConcurrency:        defaultQueryPrefetchConcurrency,
		quotaProject:               d.quotaProject,
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
	case OptionStringImpersonateLifetime:
		if d.impersonateLifetime == 0 {
			// If no lifetime is set but impersonation is enabled, return the default
			if d.hasImpersonationOptions() {
				return (3600 * time.Second).String(), nil
			}
			return "", nil
		}
		return d.impersonateLifetime.String(), nil
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
	case "uri", "url":
		params, err := ParseBigQueryURIToParams(value)
		if err != nil {
			return err
		}
		// Apply all parsed parameters
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
	case OptionStringLocation:
		d.location = value
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

	// Parse the URI
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] invalid BigQuery URI format: %v", err),
		}
	}

	// Validate scheme
	if parsedURI.Scheme != "bigquery" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] invalid BigQuery URI scheme: expected 'bigquery', got '%s'", parsedURI.Scheme),
		}
	}

	// Extract project ID from path
	projectID := strings.TrimPrefix(parsedURI.Path, "/")
	if projectID == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[bq] project ID is required in URI path",
		}
	}

	params := make(map[string]string)
	params[OptionStringProjectID] = projectID

	// Parameter mapping - allows short names in URI to map to full option names
	parameterMap := map[string]string{
		"DatasetId":    OptionStringDatasetID,
		"Location":     OptionStringLocation,
		"TableId":      OptionStringTableID,
		"QuotaProject": OptionStringAuthQuotaProject,

		"ImpersonateTargetPrincipal": OptionStringImpersonateTargetPrincipal,
		"ImpersonateDelegates":       OptionStringImpersonateDelegates,
		"ImpersonateScopes":          OptionStringImpersonateScopes,
		"ImpersonateLifetime":        OptionStringImpersonateLifetime,

		// Auth parameters (handled by OAuthType switch)
		"AuthCredentials":  OptionStringAuthCredentials,
		"AuthClientId":     OptionStringAuthClientID,
		"AuthClientSecret": OptionStringAuthClientSecret,
		"AuthRefreshToken": OptionStringAuthRefreshToken,
	}

	// Parse query parameters
	queryParams := parsedURI.Query()

	// OAuthType is optional, defaults to Application Default Credentials
	oauthTypeStr := queryParams.Get("OAuthType")
	if oauthTypeStr != "" {
		// Only process OAuthType if it's explicitly provided
		oauthType, err := strconv.Atoi(oauthTypeStr)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid OAuthType value: %s", oauthTypeStr),
			}
		}

		// Map OAuthType to auth type value
		switch oauthType {
		case 0:
			params[OptionStringAuthType] = OptionValueAuthTypeAppDefaultCredentials
		case 1:
			params[OptionStringAuthType] = OptionValueAuthTypeJSONCredentialFile
			// Require AuthCredentials for service account file
			if authCredentials := queryParams.Get("AuthCredentials"); authCredentials != "" {
				// URL decode the credentials
				if decodedCreds, err := url.QueryUnescape(authCredentials); err != nil {
					return nil, adbc.Error{
						Code: adbc.StatusInvalidArgument,
						Msg:  fmt.Sprintf("[bq] invalid AuthCredentials format: %v", err),
					}
				} else {
					if optionName, exists := parameterMap["AuthCredentials"]; exists {
						params[optionName] = decodedCreds
					}
				}
			} else {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "[bq] AuthCredentials required for service account authentication",
				}
			}
		case 2:
			params[OptionStringAuthType] = OptionValueAuthTypeJSONCredentialString
			// Require AuthCredentials for service account string
			if authCredentials := queryParams.Get("AuthCredentials"); authCredentials != "" {
				// URL decode the credentials
				if decodedCreds, err := url.QueryUnescape(authCredentials); err != nil {
					return nil, adbc.Error{
						Code: adbc.StatusInvalidArgument,
						Msg:  fmt.Sprintf("[bq] invalid AuthCredentials format: %v", err),
					}
				} else {
					if optionName, exists := parameterMap["AuthCredentials"]; exists {
						params[optionName] = decodedCreds
					}
				}
			} else {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "[bq] AuthCredentials required for service account authentication",
				}
			}
		case 3:
			params[OptionStringAuthType] = OptionValueAuthTypeUserAuthentication
			// Require OAuth credentials - use parameter map
			requiredAuthParams := []string{"AuthClientId", "AuthClientSecret", "AuthRefreshToken"}
			for _, paramName := range requiredAuthParams {
				value := queryParams.Get(paramName)
				if value == "" {
					return nil, adbc.Error{
						Code: adbc.StatusInvalidArgument,
						Msg:  fmt.Sprintf("[bq] %s required for OAuth authentication", paramName),
					}
				}
				if optionName, exists := parameterMap[paramName]; exists {
					params[optionName] = value
				}
			}
		case 4:
			params[OptionStringAuthType] = OptionValueAuthTypeDefault
		default:
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid OAuthType value: %d", oauthType),
			}
		}
	}
	// When OAuthType is not provided, don't set authType at all
	// This leaves it as empty string, which triggers ADC in newClient()

	// Process all other query parameters using the mapping
	for paramName, paramValues := range queryParams {
		if paramName == "OAuthType" ||
			paramName == "AuthCredentials" ||
			paramName == "AuthClientId" ||
			paramName == "AuthClientSecret" ||
			paramName == "AuthRefreshToken" {
			continue // Already processed by OAuthType switch
		}

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
