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
	"runtime/debug"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	OptionAuthType = "bigquery.auth_type"
	// https://pkg.go.dev/google.golang.org/api@v0.258.0/option#WithAuthCredentialsJSON
	// https://pkg.go.dev/google.golang.org/api@v0.258.0/internal/credentialstype#CredType
	OptionAuthCredentialsType = "bigquery.auth.credentials_type"
	OptionLocation            = "bigquery.location"
	OptionProjectID           = "bigquery.project_id"
	OptionDatasetID           = "bigquery.dataset_id"
	OptionEndpoint            = "bigquery.endpoint"
	OptionStorageEndpoint     = "bigquery.storage_endpoint"

	OptionValueAuthTypeDefault              = "auth_bigquery"
	OptionValueAuthTypeJSONCredentialFile   = "json_credential_file"
	OptionValueAuthTypeJSONCredentialString = "json_credential_string"
	OptionValueAuthTypeAnonymous            = "anonymous"
	OptionValueAuthTypeUserAuthentication   = "user_authentication"
	// WithAppDefaultCredentials instructs the driver to authenticate using
	// Application Default Credentials (ADC).
	OptionValueAuthTypeAppDefaultCredentials = "app_default_credentials"
	// WithJSONCredentials instructs the driver to authenticate using the
	// given JSON credentials. The value should be a byte array representing
	// the JSON credentials.
	OptionValueAuthTypeJSONCredentials = "json_credentials"
	// WithOAuthClientIDs instructs the driver to authenticate using the given
	// OAuth client ID and client secret. The value should be a string array
	// of length 2, where the first element is the client ID and the second
	// is the client secret.
	OptionValueAuthTypeOAuthClientIDs = "oauth_client_ids"

	OptionAuthCredentials  = "bigquery.auth.credentials"
	OptionAuthClientID     = "bigquery.auth.client_id"
	OptionAuthClientSecret = "bigquery.auth.client_secret"
	OptionAuthRefreshToken = "bigquery.auth.refresh_token"
	OptionAuthQuotaProject = "bigquery.auth.quota_project"

	// OptionQueryParameterMode specifies if the query uses positional syntax ("?")
	// or the named syntax ("@p"). It is illegal to mix positional and named syntax.
	// Default is OptionValueQueryParameterModePositional.
	OptionQueryParameterMode                = "bigquery.query.parameter_mode"
	OptionValueQueryParameterModeNamed      = "named"
	OptionValueQueryParameterModePositional = "positional"

	OptionQueryDestinationTable        = "bigquery.query.destination_table"
	OptionQueryDefaultProjectID        = "bigquery.query.default_project_id"
	OptionQueryDefaultDatasetID        = "bigquery.query.default_dataset_id"
	OptionQueryCreateDisposition       = "bigquery.query.create_disposition"
	OptionQueryWriteDisposition        = "bigquery.query.write_disposition"
	OptionQueryDisableQueryCache       = "bigquery.query.disable_query_cache"
	OptionQueryDisableFlattenedResults = "bigquery.query.disable_flattened_results"
	OptionQueryAllowLargeResults       = "bigquery.query.allow_large_results"
	OptionQueryPriority                = "bigquery.query.priority"
	OptionQueryMaxBillingTier          = "bigquery.query.max_billing_tier"
	OptionQueryMaxBytesBilled          = "bigquery.query.max_bytes_billed"
	OptionQueryUseLegacySQL            = "bigquery.query.use_legacy_sql"
	OptionQueryDryRun                  = "bigquery.query.dry_run"
	OptionQueryCreateSession           = "bigquery.query.create_session"
	OptionQueryJobTimeout              = "bigquery.query.job_timeout"

	OptionQueryResultBufferSize    = "bigquery.query.result_buffer_size"
	OptionQueryPrefetchConcurrency = "bigquery.query.prefetch_concurrency"

	defaultQueryResultBufferSize    = 200
	defaultQueryPrefetchConcurrency = 10

	AccessTokenEndpoint   = "https://accounts.google.com/o/oauth2/token"
	AccessTokenServerName = "google.com"

	// OptionImpersonateTargetPrincipal instructs the driver to impersonate the
	// given service account email.
	OptionImpersonateTargetPrincipal = "bigquery.impersonate.target_principal"

	// OptionImpersonateDelegates instructs the driver to impersonate using the
	// given comma-separated list of service account emails in the delegation
	// chain.
	OptionImpersonateDelegates = "bigquery.impersonate.delegates"

	// OptionImpersonateScopes instructs the driver to impersonate using the
	// given comma-separated list of OAuth 2.0 scopes.
	OptionImpersonateScopes = "bigquery.impersonate.scopes"

	// OptionImpersonateLifetime instructs the driver to impersonate for the
	// given duration (e.g. "3600s").
	OptionImpersonateLifetime = "bigquery.impersonate.lifetime"

	// OptionBulkIngestMethod specifies the bulk ingest implementation to use.
	// Default is "load" (Parquet + LoaderFrom). "storage_write" uses Storage Write API.
	OptionBulkIngestMethod                  = "bigquery.bulk_ingest.method"
	OptionValueBulkIngestMethodLoad         = "load"
	OptionValueBulkIngestMethodStorageWrite = "storage_write"

	// OptionBulkIngestCompression specifies compression for Storage Write API.
	// Only applies when using storage_write method.
	OptionBulkIngestCompression = "bigquery.bulk_ingest.compression"
	OptionValueCompressionNone  = "none"
	OptionValueCompressionLZ4   = "lz4"
	OptionValueCompressionZSTD  = "zstd"
)

var (
	infoVendorVersion string

	// Accept old option values, but document/encourage the new ones
	optionRemapping = map[string]string{
		"adbc.bigquery.sql.auth.client_id":                    OptionAuthClientID,
		"adbc.bigquery.sql.auth.client_secret":                OptionAuthClientSecret,
		"adbc.bigquery.sql.auth.quota_project":                OptionAuthQuotaProject,
		"adbc.bigquery.sql.auth.refresh_token":                OptionAuthRefreshToken,
		"adbc.bigquery.sql.auth_credentials":                  OptionAuthCredentials,
		"adbc.bigquery.sql.auth_type":                         OptionAuthType,
		"adbc.bigquery.sql.auth_type.anonymous":               OptionValueAuthTypeAnonymous,
		"adbc.bigquery.sql.auth_type.app_default_credentials": OptionValueAuthTypeAppDefaultCredentials,
		"adbc.bigquery.sql.auth_type.auth_bigquery":           OptionValueAuthTypeDefault,
		"adbc.bigquery.sql.auth_type.json_credential_file":    OptionValueAuthTypeJSONCredentialFile,
		"adbc.bigquery.sql.auth_type.json_credential_string":  OptionValueAuthTypeJSONCredentialString,
		"adbc.bigquery.sql.auth_type.json_credentials":        OptionValueAuthTypeJSONCredentials,
		"adbc.bigquery.sql.auth_type.oauth_client_ids":        OptionValueAuthTypeOAuthClientIDs,
		"adbc.bigquery.sql.auth_type.user_authentication":     OptionValueAuthTypeUserAuthentication,
		"adbc.bigquery.sql.dataset_id":                        OptionDatasetID,
		"adbc.bigquery.sql.endpoint":                          OptionEndpoint,
		"adbc.bigquery.sql.impersonate.delegates":             OptionImpersonateDelegates,
		"adbc.bigquery.sql.impersonate.lifetime":              OptionImpersonateLifetime,
		"adbc.bigquery.sql.impersonate.scopes":                OptionImpersonateScopes,
		"adbc.bigquery.sql.impersonate.target_principal":      OptionImpersonateTargetPrincipal,
		"adbc.bigquery.sql.location":                          OptionLocation,
		"adbc.bigquery.sql.project_id":                        OptionProjectID,
		"adbc.bigquery.sql.query.allow_large_results":         OptionQueryAllowLargeResults,
		"adbc.bigquery.sql.query.create_disposition":          OptionQueryCreateDisposition,
		"adbc.bigquery.sql.query.create_session":              OptionQueryCreateSession,
		"adbc.bigquery.sql.query.default_dataset_id":          OptionQueryDefaultDatasetID,
		"adbc.bigquery.sql.query.default_project_id":          OptionQueryDefaultProjectID,
		"adbc.bigquery.sql.query.destination_table":           OptionQueryDestinationTable,
		"adbc.bigquery.sql.query.disable_flattened_results":   OptionQueryDisableFlattenedResults,
		"adbc.bigquery.sql.query.disable_query_cache":         OptionQueryDisableQueryCache,
		"adbc.bigquery.sql.query.dry_run":                     OptionQueryDryRun,
		"adbc.bigquery.sql.query.job_timeout":                 OptionQueryJobTimeout,
		"adbc.bigquery.sql.query.max_billing_tier":            OptionQueryMaxBillingTier,
		"adbc.bigquery.sql.query.max_bytes_billed":            OptionQueryMaxBytesBilled,
		"adbc.bigquery.sql.query.parameter_mode":              OptionQueryParameterMode,
		"adbc.bigquery.sql.query.parameter_mode_named":        OptionValueQueryParameterModeNamed,
		"adbc.bigquery.sql.query.parameter_mode_positional":   OptionValueQueryParameterModePositional,
		"adbc.bigquery.sql.query.prefetch_concurrency":        OptionQueryPrefetchConcurrency,
		"adbc.bigquery.sql.query.priority":                    OptionQueryPriority,
		"adbc.bigquery.sql.query.result_buffer_size":          OptionQueryResultBufferSize,
		"adbc.bigquery.sql.query.use_legacy_sql":              OptionQueryUseLegacySQL,
		"adbc.bigquery.sql.query.write_disposition":           OptionQueryWriteDisposition,
		"adbc.bigquery.sql.storage_endpoint":                  OptionStorageEndpoint,
	}
)

// remapOption remaps old option keys/values to new ones, if applicable.
func remapOption(v string) string {
	if newV, ok := optionRemapping[v]; ok {
		return newV
	}
	return v
}

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch dep.Path {
			case "cloud.google.com/go/bigquery":
				infoVendorVersion = fmt.Sprintf("cloud.google.com/go/bigquery %s", dep.Version)
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new BigQuery driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) driverbase.Driver {
	info := driverbase.DefaultDriverInfo("BigQuery")
	info.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver Foundry Driver for BigQuery",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
		adbc.InfoVendorVersion:   infoVendorVersion,
	})
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.DatabaseWithContext, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}
	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		authType:         OptionValueAuthTypeDefault,
	}
	if err := db.SetOptions(ctx, opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}

func stringToTable(defaultProjectID, defaultDatasetID, value string) (*bigquery.Table, error) {
	parts := strings.Split(value, ".")
	table := &bigquery.Table{
		ProjectID: defaultProjectID,
		DatasetID: defaultDatasetID,
	}
	switch len(parts) {
	case 1:
		table.TableID = parts[0]
	case 2:
		table.DatasetID = parts[0]
		table.TableID = parts[1]
	case 3:
		table.ProjectID = parts[0]
		table.DatasetID = parts[1]
		table.TableID = parts[2]
	default:
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] invalid table reference format, expected `[[ProjectId.]DatasetId.]TableId`, got: `%s`", value),
		}
	}
	return table, nil
}

func stringToTableCreateDisposition(value string) (bigquery.TableCreateDisposition, error) {
	v := bigquery.TableCreateDisposition(value)
	switch v {
	case bigquery.CreateIfNeeded, bigquery.CreateNever:
		return v, nil
	default:
		return v, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown table create disposition value `%s`", v),
		}
	}
}

func stringToTableWriteDisposition(value string) (bigquery.TableWriteDisposition, error) {
	v := bigquery.TableWriteDisposition(value)
	switch v {
	case bigquery.WriteAppend, bigquery.WriteTruncate, bigquery.WriteEmpty:
		return v, nil
	default:
		return v, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown table write disposition value `%s`", v),
		}
	}
}

func stringToQueryPriority(value string) (bigquery.QueryPriority, error) {
	v := bigquery.QueryPriority(value)
	switch v {
	case bigquery.BatchPriority, bigquery.InteractivePriority:
		return v, nil
	default:
		return v, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown priority value `%s`", v),
		}
	}
}

func tableToString(value *bigquery.Table) string {
	if value == nil {
		return ""
	} else {
		return fmt.Sprintf("%s.%s.%s", value.ProjectID, value.DatasetID, value.TableID)
	}
}
