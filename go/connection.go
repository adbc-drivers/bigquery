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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google/externalaccount"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"resty.dev/v3"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	authType              string
	credentialsType       option.CredentialsType
	credentials           string
	clientID              string
	clientSecret          string
	refreshToken          string
	accessToken           string
	accessTokenEndpoint   string
	accessTokenServerName string
	quotaProject          string

	// External-account (Workload Identity Federation) options.
	externalAccountAudience         string
	externalAccountImpersonationURL string
	externalAccountRequestURL       string
	externalAccountRequestData      string

	impersonateTargetPrincipal string
	impersonateDelegates       []string
	impersonateScopes          []string
	impersonateLifetime        time.Duration

	// the default location to use for all BigQuery requests
	location string
	// catalog is the same as the project id in BigQuery
	catalog string
	// dbSchema is the same as the dataset id in BigQuery
	dbSchema string
	// endpoint is the custom BigQuery API endpoint
	endpoint        string
	storageEndpoint string

	sessionID *string

	resultRecordBufferSize int
	prefetchConcurrency    int

	bulkIngestMethod      string
	bulkIngestCompression string

	client *bigquery.Client
	// clientStorageApiDisabled is a parallel client that does NOT have the
	// Storage Read API enabled. Used when a statement requests
	// OptionBoolUseStorageApiDisabledClient, since the Storage API returns
	// nulls for pseudo-columns like _PARTITIONDATE/_PARTITIONTIME.
	// Initialized lazily on first use.
	clientStorageApiDisabled *bigquery.Client
}

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	catalogPattern, err := driverbase.PatternToRegexp(catalogFilter)
	if err != nil {
		return nil, err
	}
	if catalogPattern == nil {
		catalogPattern = driverbase.AcceptAll
	}

	// Connections to BQ are scoped to a particular Project, which corresponds to catalog-level namespacing.
	// TODO: Consider enumerating projects with ResourceManager API, but this may not be "idiomatic" usage.
	project := c.client.Project()
	res := make([]string, 0)
	if catalogPattern.MatchString(project) {
		res = append(res, project)
	}

	return res, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	schemaPattern, err := driverbase.PatternToRegexp(schemaFilter)
	if err != nil {
		return nil, err
	}
	if schemaPattern == nil {
		schemaPattern = driverbase.AcceptAll
	}

	it := c.client.Datasets(ctx)
	it.ProjectID = catalog

	res := make([]string, 0)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if schemaPattern.MatchString(ds.DatasetID) {
			res = append(res, ds.DatasetID)
		}

	}

	return res, nil
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	tablePattern, err := driverbase.PatternToRegexp(tableFilter)
	if err != nil {
		return nil, err
	}
	if tablePattern == nil {
		tablePattern = driverbase.AcceptAll
	}

	it := c.client.DatasetInProject(catalog, schema).Tables(ctx)

	res := make([]driverbase.TableInfo, 0)
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if !tablePattern.MatchString(table.TableID) {
			continue
		}

		md, err := table.Metadata(ctx, bigquery.WithMetadataView(bigquery.BasicMetadataView))
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusInternal, err, "get table metadata for %s.%s.%s", catalog, schema, table.TableID)
		}

		var constraints []driverbase.ConstraintInfo
		if md.TableConstraints != nil {
			constraints = make([]driverbase.ConstraintInfo, 0)
			if md.TableConstraints.PrimaryKey != nil {
				constraints = append(constraints, driverbase.ConstraintInfo{
					// BigQuery Primary Keys are unnamed
					ConstraintType:        driverbase.PrimaryKey,
					ConstraintColumnNames: driverbase.RequiredList(md.TableConstraints.PrimaryKey.Columns),
				})
			}

			for _, fk := range md.TableConstraints.ForeignKeys {
				var columnUsage []driverbase.ConstraintColumnUsage
				columnNames := make([]string, len(fk.ColumnReferences))
				if len(fk.ColumnReferences) > 0 {
					columnUsage = make([]driverbase.ConstraintColumnUsage, len(fk.ColumnReferences))
				}
				for i, ref := range fk.ColumnReferences {
					columnNames[i] = ref.ReferencingColumn
					columnUsage[i] = driverbase.ConstraintColumnUsage{
						ForeignKeyCatalog:  new(fk.ReferencedTable.ProjectID),
						ForeignKeyDbSchema: new(fk.ReferencedTable.DatasetID),
						ForeignKeyTable:    fk.ReferencedTable.TableID,
						ForeignKeyColumn:   ref.ReferencedColumn,
					}
				}
				constraints = append(constraints, driverbase.ConstraintInfo{
					ConstraintName:        new(fk.Name),
					ConstraintType:        driverbase.ForeignKey,
					ConstraintColumnNames: columnNames,
					ConstraintColumnUsage: columnUsage,
				})
			}
		}

		var columns []driverbase.ColumnInfo
		if includeColumns {
			columnPattern, err := driverbase.PatternToRegexp(columnFilter)
			if err != nil {
				return nil, err
			}
			if columnPattern == nil {
				columnPattern = driverbase.AcceptAll
			}

			columns = make([]driverbase.ColumnInfo, 0)
			for pos, fieldschema := range md.Schema {
				if columnPattern.MatchString(fieldschema.Name) {
					xdbcIsNullable := "YES"
					xdbcNullable := int16(1)
					if fieldschema.Required {
						xdbcIsNullable = "NO"
						xdbcNullable = 0
					}

					xdbcColumnSize := fieldschema.MaxLength
					if xdbcColumnSize == 0 {
						xdbcColumnSize = fieldschema.Precision
					}

					var xdbcCharOctetLength int32
					if fieldschema.Type == bigquery.BytesFieldType {
						xdbcCharOctetLength = int32(fieldschema.MaxLength)
					}

					field, err := buildField(fieldschema, 0)
					if err != nil {
						return nil, err
					}
					xdbcDataType := driverbase.ToXdbcDataType(field.Type)

					columns = append(columns, driverbase.ColumnInfo{
						ColumnName:          fieldschema.Name,
						OrdinalPosition:     new(int32(pos + 1)),
						Remarks:             new(fieldschema.Description),
						XdbcDataType:        new(int16(field.Type.ID())),
						XdbcTypeName:        new(string(fieldschema.Type)),
						XdbcNullable:        new(xdbcNullable),
						XdbcSqlDataType:     new(int16(xdbcDataType)),
						XdbcIsNullable:      new(xdbcIsNullable),
						XdbcDecimalDigits:   new(int16(fieldschema.Scale)),
						XdbcColumnSize:      new(int32(xdbcColumnSize)),
						XdbcCharOctetLength: new(xdbcCharOctetLength),
						XdbcScopeCatalog:    new(catalog),
						XdbcScopeSchema:     new(schema),
						XdbcScopeTable:      new(table.TableID),
					})
				}
			}
		}

		res = append(res, driverbase.TableInfo{
			TableName:        table.TableID,
			TableType:        string(md.Type),
			TableConstraints: constraints,
			TableColumns:     columns,
		})
	}

	return res, nil
}

// oauthErrorResponse is the OAuth 2.0 error body an IdP returns on a
// rejected token request.
type oauthErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

const (
	idpMaxRetries     = 5
	idpRetryWaitMin   = 500 * time.Millisecond
	idpRetryWaitMax   = 8 * time.Second
	idpRequestTimeout = 30 * time.Second
)

// idpHTTPClient is shared across all IdP token requests. Retries use resty's
// default capped exponential backoff with jitter. SetAllowNonIdempotentRetry
// is required because the token request is a POST (resty skips POST retries
// by default), and re-requesting a token is safe.
var idpHTTPClient = resty.New().
	SetTimeout(idpRequestTimeout).
	SetRetryCount(idpMaxRetries).
	SetRetryWaitTime(idpRetryWaitMin).
	SetRetryMaxWaitTime(idpRetryWaitMax).
	SetAllowNonIdempotentRetry(true)

// idpTokenSupplier implements externalaccount.SubjectTokenSupplier by POSTing
// request_data to request_url and returning the access_token.
type idpTokenSupplier struct {
	requestURL  string
	requestData string
}

// SubjectToken is invoked by the externalaccount token source to fetch the
// IdP subject token.
// See https://pkg.go.dev/golang.org/x/oauth2/google/externalaccount#SubjectTokenSupplier
func (s *idpTokenSupplier) SubjectToken(ctx context.Context, _ externalaccount.SupplierOptions) (string, error) {
	resp, err := idpHTTPClient.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetHeader("Content-Type", "application/x-www-form-urlencoded").
		SetBody(s.requestData).
		Post(s.requestURL)
	if err != nil {
		return "", adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("network error communicating with identity provider: %v", err),
		}
	}
	body := resp.Bytes()

	if resp.StatusCode() == http.StatusTooManyRequests {
		return "", adbc.Error{
			Code: adbc.StatusIO,
			Msg:  "identity provider is rate-limiting token requests (HTTP 429); reduce concurrency or raise the provider's token rate limit",
		}
	}
	if !resp.IsSuccess() {
		// Prefer the standard OAuth 2.0 error body; fall back to a truncated raw body.
		var oauthErr oauthErrorResponse
		if json.Unmarshal(body, &oauthErr) == nil && oauthErr.Error != "" {
			desc := oauthErr.ErrorDescription
			if desc == "" {
				desc = "no description provided"
			}
			return "", adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("identity provider rejected the token request (HTTP %d): %s - %s", resp.StatusCode(), oauthErr.Error, desc),
			}
		}
		// Cap the raw body so a large HTML error page doesn't flood the message.
		raw := string(body)
		if len(raw) > 256 {
			raw = raw[:253] + "..."
		}
		return "", adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("identity provider token request failed (HTTP %d): %s", resp.StatusCode(), raw),
		}
	}

	var tokenResponse bigQueryTokenResponse
	if err := json.Unmarshal(body, &tokenResponse); err != nil {
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("could not parse identity provider token response as JSON (%v); the token_endpoint request_url may be returning an HTML proxy or error page instead of a token", err),
		}
	}
	if tokenResponse.AccessToken == "" {
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "access_token missing from identity provider token response; check the token_endpoint request_url and request_data and that the provider issues an OIDC access token",
		}
	}
	return tokenResponse.AccessToken, nil
}

type bigQueryTokenResponse struct {
	AccessToken      string `json:"access_token"`
	ExpiresIn        int    `json:"expires_in"`
	Scope            string `json:"scope"`
	TokenType        string `json:"token_type"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
	ErrorURI         string `json:"error_uri"`
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentCatalog(ctx context.Context) (string, error) {
	return c.catalog, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentDbSchema(ctx context.Context) (string, error) {
	return c.dbSchema, nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentCatalog(ctx context.Context, value string) error {
	// TODO: is this really possible? we may have to recreate client
	c.catalog = value
	return nil
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(ctx context.Context, value string) error {
	sanitized, err := sanitizeDataset(value)
	if err != nil {
		return err
	}
	c.dbSchema = sanitized
	// Check that the dataset exists
	if _, err := c.client.DatasetInProject(c.catalog, c.dbSchema).Metadata(ctx); err != nil {
		return errToAdbcErr(adbc.StatusInvalidArgument, err, "set current db schema to %s", quoteIdentifier(value))
	}
	return nil
}

// ListTableTypes implements driverbase.TableTypeLister.
func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{
		string(bigquery.RegularTable),
		string(bigquery.ViewTable),
		string(bigquery.ExternalTable),
		string(bigquery.MaterializedView),
		string(bigquery.Snapshot),
	}, nil
}

func (c *connectionImpl) exec(ctx context.Context, stmt string, config func(*bigquery.QueryConfig)) (*bigquery.JobStatus, error) {
	query := c.client.Query(stmt)
	query.DefaultProjectID = c.catalog
	query.DefaultDatasetID = c.dbSchema
	if c.sessionID != nil {
		query.ConnectionProperties = append(query.ConnectionProperties, &bigquery.ConnectionProperty{
			Key:   "session_id",
			Value: *c.sessionID,
		})
	}
	if config != nil {
		config(&query.QueryConfig)
	}

	// Do NOT use errToAdbcErr here since we don't have context. Expect
	// the caller to do that.
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}
	status, err := safeWaitForJob(ctx, c.Logger, job)
	if err != nil {
		return nil, err
	} else if err := status.Err(); err != nil {
		return nil, err

	}
	return status, nil
}

// SetAutocommit implements driverbase.AutocommitSetter.
func (c *connectionImpl) SetAutocommit(ctx context.Context, enabled bool) error {
	if enabled {
		if c.sessionID == nil {
			// This should never happen
			return adbc.Error{
				Code: adbc.StatusInvalidState,
				Msg:  "[bq] SetAutocommit to `false` called, but sessionID is not set",
			}
		}

		_, err := c.exec(ctx, "COMMIT TRANSACTION", nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err, "commit transaction")
		}
		_, err = c.exec(ctx, fmt.Sprintf("CALL BQ.ABORT_SESSION('%s')", *c.sessionID), nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err, "close session")
		}
		c.sessionID = nil
		return nil
	} else {
		if c.sessionID != nil {
			// This should never happen
			return adbc.Error{
				Code: adbc.StatusInvalidState,
				Msg:  "[bq] SetAutocommit to `true` called, but sessionID is already set",
			}
		}

		status, err := c.exec(ctx, "SELECT 1", func(config *bigquery.QueryConfig) {
			config.CreateSession = true
		})
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err, "create session")
		} else if status.Statistics.SessionInfo == nil {
			return adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[bq] could not create session: no session info in job status",
			}
		}
		c.sessionID = &status.Statistics.SessionInfo.SessionID

		_, err = c.exec(ctx, "BEGIN TRANSACTION", nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err, "begin transaction")
		}
		return nil
	}
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Commit(ctx context.Context) error {
	_, err := c.exec(ctx, "COMMIT TRANSACTION", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err, "commit transaction")
	}
	_, err = c.exec(ctx, "BEGIN TRANSACTION", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err, "begin transaction")
	}
	return nil
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Rollback(ctx context.Context) error {
	_, err := c.exec(ctx, "ROLLBACK TRANSACTION", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err, "rollback transaction")
	}
	_, err = c.exec(ctx, "BEGIN TRANSACTION", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err, "begin transaction")
	}
	return nil
}

// Close closes this connection and releases any associated resources.
func (c *connectionImpl) Close(ctx context.Context) error {
	var firstErr error
	if c.client != nil {
		if err := c.client.Close(); err != nil {
			firstErr = errToAdbcErr(adbc.StatusIO, err, "close client")
		}
	}
	if c.clientStorageApiDisabled != nil {
		if err := c.clientStorageApiDisabled.Close(); err != nil && firstErr == nil {
			firstErr = errToAdbcErr(adbc.StatusIO, err, "close storage-api-disabled client")
		}
	}
	return firstErr
}

// Metadata methods
// Generally these methods return an array.RecordReader that
// can be consumed to retrieve metadata about the database as Arrow
// data. The returned metadata has an expected schema given in the
// doc strings of the specific methods. Schema fields are nullable
// unless otherwise marked. While no Statement is used in these
// methods, the result set may count as an active statement to the
// driver for the purposes of concurrency management (e.g. if the
// driver has a limit on concurrent active statements and it must
// execute a SQL query internally in order to implement the metadata
// method).
//
// Some methods accept "search pattern" arguments, which are strings
// that can contain the special character "%" to match zero or more
// characters, or "_" to match exactly one character. (See the
// documentation of DatabaseMetaData in JDBC or "Pattern Value Arguments"
// in the ODBC documentation.) Escaping is not currently supported.
// GetObjects gets a hierarchical view of all catalogs, database schemas,
// tables, and columns.
//
// The result is an Arrow Dataset with the following schema:
//
//	Field Name                  | Field Type
//	----------------------------|----------------------------
//	catalog_name                | utf8
//	catalog_db_schemas          | list<DB_SCHEMA_SCHEMA>
//
// DB_SCHEMA_SCHEMA is a Struct with the fields:
//
//	Field Name                  | Field Type
//	----------------------------|----------------------------
//	db_schema_name              | utf8
//	db_schema_tables            | list<TABLE_SCHEMA>
//
// TABLE_SCHEMA is a Struct with the fields:
//
//	Field Name                  | Field Type
//	----------------------------|----------------------------
//	table_name                  | utf8 not null
//	table_type                  | utf8 not null
//	table_columns               | list<COLUMN_SCHEMA>
//	table_constraints           | list<CONSTRAINT_SCHEMA>
//
// COLUMN_SCHEMA is a Struct with the fields:
//
//	Field Name                  | Field Type          | Comments
//	----------------------------|---------------------|---------
//	column_name                 | utf8 not null       |
//	ordinal_position            | int32               | (1)
//	remarks                     | utf8                | (2)
//	xdbc_data_type              | int16               | (3)
//	xdbc_type_name              | utf8                | (3)
//	xdbc_column_size            | int32               | (3)
//	xdbc_decimal_digits         | int16               | (3)
//	xdbc_num_prec_radix         | int16               | (3)
//	xdbc_nullable               | int16               | (3)
//	xdbc_column_def             | utf8                | (3)
//	xdbc_sql_data_type          | int16               | (3)
//	xdbc_datetime_sub           | int16               | (3)
//	xdbc_char_octet_length      | int32               | (3)
//	xdbc_is_nullable            | utf8                | (3)
//	xdbc_scope_catalog          | utf8                | (3)
//	xdbc_scope_schema           | utf8                | (3)
//	xdbc_scope_table            | utf8                | (3)
//	xdbc_is_autoincrement       | bool                | (3)
//	xdbc_is_generatedcolumn     | utf8                | (3)
//
// 1. The column's ordinal position in the table (starting from 1).
// 2. Database-specific description of the column.
// 3. Optional Value. Should be null if not supported by the driver.
//    xdbc_values are meant to provide JDBC/ODBC-compatible metadata
//    in an agnostic manner.
//
// CONSTRAINT_SCHEMA is a Struct with the fields:
//
//	Field Name                  | Field Type          | Comments
//	----------------------------|---------------------|---------
//	constraint_name             | utf8                |
//	constraint_type             | utf8 not null       | (1)
//	constraint_column_names     | list<utf8> not null | (2)
//	constraint_column_usage     | list<USAGE_SCHEMA>  | (3)
//
// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
// 2. The columns on the current table that are constrained, in order.
// 3. For FOREIGN KEY only, the referenced table and columns.
//
// USAGE_SCHEMA is a Struct with fields:
//
//	Field Name                  | Field Type
//	----------------------------|----------------------------
//	fk_catalog                  | utf8
//	fk_db_schema                | utf8
//	fk_table                    | utf8 not null
//	fk_column_name              | utf8 not null
//
// For the parameters: If nil is passed, then that parameter will not
// be filtered by at all. If an empty string, then only objects without
// that property (ie: catalog or db schema) will be returned.
//
// tableName and columnName must be either nil (do not filter by
// table name or column name) or non-empty.
//
// All non-empty, non-nil strings should be a search pattern (as described
// earlier).

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return c.getTableSchemaWithFilter(ctx, catalog, dbSchema, tableName, nil)
}

// NewStatement initializes a new statement object tied to this connection
func (c *connectionImpl) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	return &statement{
		alloc:                  c.Alloc,
		cnxn:                   c,
		parameterMode:          OptionValueQueryParameterModePositional,
		resultRecordBufferSize: c.resultRecordBufferSize,
		prefetchConcurrency:    c.prefetchConcurrency,
		ingest:                 driverbase.NewBulkIngestOptions(),
		queryConfig: bigquery.QueryConfig{
			DefaultProjectID: c.catalog,
			DefaultDatasetID: c.dbSchema,
		},
	}, nil
}

func (c *connectionImpl) GetOption(ctx context.Context, key string) (string, error) {
	key = remapOption(key)
	switch key {
	case OptionAuthType:
		return c.authType, nil
	case OptionAuthCredentialsType:
		return string(c.credentialsType), nil
	case OptionAuthCredentials:
		return c.credentials, nil
	case OptionAuthClientID:
		return c.clientID, nil
	case OptionAuthClientSecret:
		return c.clientSecret, nil
	case OptionAuthRefreshToken:
		return c.refreshToken, nil
	case OptionStringAuthAccessToken:
		return c.accessToken, nil
	case OptionStringAuthAccessTokenEndpoint:
		return c.accessTokenEndpoint, nil
	case OptionStringAuthAccessTokenServerName:
		return c.accessTokenServerName, nil
	case OptionStringAuthExternalAccountAudience:
		return c.externalAccountAudience, nil
	case OptionStringAuthExternalAccountImpersonationURL:
		return c.externalAccountImpersonationURL, nil
	case OptionStringAuthExternalAccountRequestURL:
		return c.externalAccountRequestURL, nil
	case OptionStringAuthExternalAccountRequestData:
		return c.externalAccountRequestData, nil
	case OptionAuthQuotaProject:
		return c.quotaProject, nil
	case OptionProjectID:
		return c.catalog, nil
	case OptionDatasetID:
		return c.dbSchema, nil
	case OptionImpersonateLifetime:
		if c.impersonateLifetime == 0 {
			// If no lifetime is set but impersonation is enabled, return the default
			if c.hasImpersonationOptions() {
				return (3600 * time.Second).String(), nil
			}
			return "", nil
		}
		return c.impersonateLifetime.String(), nil
	case OptionBulkIngestMethod:
		if c.bulkIngestMethod == "" {
			return OptionValueBulkIngestMethodLoad, nil
		}
		return c.bulkIngestMethod, nil
	case OptionBulkIngestCompression:
		if c.bulkIngestCompression == "" {
			return OptionValueCompressionNone, nil
		}
		return c.bulkIngestCompression, nil
	default:
		return c.ConnectionImplBase.GetOption(ctx, key)
	}
}

func (c *connectionImpl) SetOption(ctx context.Context, key string, value string) error {
	key = remapOption(key)
	switch key {
	case OptionAuthType:
		value = remapOption(value)
		c.authType = value
	case OptionAuthCredentialsType:
		// N.B. ExternalAccountAuthorizedUser, GDCHServiceAccount aren't re-exported by google.golang.org/api/option
		switch value {
		case string(option.ServiceAccount):
			c.credentialsType = option.ServiceAccount
		case string(option.AuthorizedUser):
			c.credentialsType = option.AuthorizedUser
		case string(option.ImpersonatedServiceAccount):
			c.credentialsType = option.ImpersonatedServiceAccount
		case string(option.ExternalAccount):
			c.credentialsType = option.ExternalAccount
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] unknown %s=%s", key, value),
			}
		}
	case OptionAuthCredentials:
		c.credentials = value
	case OptionAuthClientID:
		c.clientID = value
	case OptionAuthClientSecret:
		c.clientSecret = value
	case OptionAuthRefreshToken:
		c.refreshToken = value
	case OptionStringAuthAccessToken:
		c.accessToken = value
	case OptionStringAuthAccessTokenEndpoint:
		c.accessTokenEndpoint = value
	case OptionStringAuthAccessTokenServerName:
		c.accessTokenServerName = value
	case OptionStringAuthExternalAccountAudience:
		c.externalAccountAudience = value
	case OptionStringAuthExternalAccountImpersonationURL:
		c.externalAccountImpersonationURL = value
	case OptionStringAuthExternalAccountRequestURL:
		c.externalAccountRequestURL = value
	case OptionStringAuthExternalAccountRequestData:
		c.externalAccountRequestData = value
	case OptionAuthQuotaProject:
		c.quotaProject = value
	case OptionImpersonateTargetPrincipal:
		c.impersonateTargetPrincipal = value
	case OptionImpersonateDelegates:
		c.impersonateDelegates = strings.Split(value, ",")
	case OptionImpersonateScopes:
		c.impersonateScopes = strings.Split(value, ",")
	case OptionImpersonateLifetime:
		dur, err := time.ParseDuration(value)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("Invalid duration string for %s: %s", OptionImpersonateLifetime, err.Error()),
			}
		}
		c.impersonateLifetime = dur
	case OptionBulkIngestMethod:
		if value != OptionValueBulkIngestMethodLoad &&
			value != OptionValueBulkIngestMethodStorageWrite {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid bulk ingest method: %s (expected %s or %s)", value, OptionValueBulkIngestMethodLoad, OptionValueBulkIngestMethodStorageWrite),
			}
		}
		c.bulkIngestMethod = value
	case OptionBulkIngestCompression:
		if value != OptionValueCompressionNone &&
			value != OptionValueCompressionLZ4 &&
			value != OptionValueCompressionZSTD {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid bulk ingest compression: %s (expected %s, %s, or %s)", value, OptionValueCompressionNone, OptionValueCompressionLZ4, OptionValueCompressionZSTD),
			}
		}
		c.bulkIngestCompression = value
	default:
		return c.ConnectionImplBase.SetOption(ctx, key, value)
	}
	return nil
}

func (c *connectionImpl) GetOptionInt(ctx context.Context, key string) (int64, error) {
	key = remapOption(key)
	switch key {
	case OptionQueryResultBufferSize:
		return int64(c.resultRecordBufferSize), nil
	case OptionQueryPrefetchConcurrency:
		return int64(c.prefetchConcurrency), nil
	default:
		return c.ConnectionImplBase.GetOptionInt(ctx, key)
	}
}

func (c *connectionImpl) SetOptionInt(ctx context.Context, key string, value int64) error {
	key = remapOption(key)
	switch key {
	case OptionQueryResultBufferSize:
		c.resultRecordBufferSize = int(value)
		return nil
	case OptionQueryPrefetchConcurrency:
		c.prefetchConcurrency = int(value)
		return nil
	default:
		return c.ConnectionImplBase.SetOptionInt(ctx, key, value)
	}
}

// authOptions builds the slice of google.golang.org/api/option client
// options reflecting the connection's configured auth type and
// impersonation/quota settings. Exposed so auxiliary clients (secondary
// BigQuery client without Storage Read API, Dataproc, GCS, …) can be
// created with the same credentials.
func (c *connectionImpl) authOptions(ctx context.Context) ([]option.ClientOption, error) {
	authOptions := []option.ClientOption{}

	switch c.authType {
	case OptionValueAuthTypeJSONCredentialFile:
		credType := c.credentialsType
		if credType == "" {
			detected, err := detectCredentialTypeFromFile(c.credentials)
			if err != nil {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[bq] failed to detect credential type from file %q: %s", c.credentials, err.Error()),
				}
			}
			credType = detected
		}
		c.credentialsType = credType
		c.Logger.Debug("Using JSON credential file", "file", c.credentials, "credentialType", string(credType))
		authOptions = append(authOptions, option.WithAuthCredentialsFile(credType, c.credentials))
	case OptionValueAuthTypeJSONCredentialString:
		credType := c.credentialsType
		if credType == "" {
			detected, err := detectCredentialTypeFromJSON([]byte(c.credentials))
			if err != nil {
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("[bq] failed to detect credential type from JSON: %s", err.Error()),
				}
			}
			credType = detected
		}
		c.credentialsType = credType
		c.Logger.Debug("Using JSON credential string", "credentialType", string(credType))
		authOptions = append(authOptions, option.WithAuthCredentialsJSON(credType, []byte(c.credentials)))
	case OptionValueAuthTypeUserAuthentication:
		if c.clientID == "" {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] `%s` parameter is empty", OptionAuthClientID),
			}
		}
		if c.clientSecret == "" {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] `%s` parameter is empty", OptionAuthClientSecret),
			}
		}
		if c.refreshToken == "" {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] `%s` parameter is empty", OptionAuthRefreshToken),
			}
		}
		c.Logger.Debug("Using user OAuth authentication")
		if c.accessTokenEndpoint == "" {
			c.accessTokenEndpoint = AccessTokenEndpoint
		}
		if c.accessTokenServerName == "" {
			c.accessTokenServerName = AccessTokenServerName
		}
		authOptions = append(authOptions, option.WithTokenSource(c))
	case OptionValueAuthTypeTemporaryAccessToken:
		if c.accessToken == "" {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] `%s` parameter is empty", OptionStringAuthAccessToken),
			}
		}
		c.Logger.Debug("Using temporary access token authentication")
		authOptions = append(authOptions, option.WithTokenSource(
			oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: c.accessToken,
				TokenType:   "Bearer",
			}),
		))
	case OptionValueAuthTypeExternalAccount:
		if c.externalAccountAudience == "" {
			return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: fmt.Sprintf("[bq] `%s` parameter is empty", OptionStringAuthExternalAccountAudience)}
		}
		if c.externalAccountRequestURL == "" {
			return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: fmt.Sprintf("[bq] `%s` parameter is empty", OptionStringAuthExternalAccountRequestURL)}
		}
		if c.externalAccountRequestData == "" {
			return nil, adbc.Error{Code: adbc.StatusInvalidArgument, Msg: fmt.Sprintf("[bq] `%s` parameter is empty", OptionStringAuthExternalAccountRequestData)}
		}
		cfg := externalaccount.Config{
			Audience:                       c.externalAccountAudience,
			SubjectTokenType:               DefaultSubjectTokenType,
			TokenURL:                       DefaultSTSTokenURL,
			ServiceAccountImpersonationURL: c.externalAccountImpersonationURL,
			Scopes:                         c.impersonateScopes,
			SubjectTokenSupplier: &idpTokenSupplier{
				requestURL:  c.externalAccountRequestURL,
				requestData: c.externalAccountRequestData,
			},
		}
		ts, err := externalaccount.NewTokenSource(ctx, cfg)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] failed to create external-account token source: %s", err.Error()),
			}
		}
		c.Logger.Debug("Using external-account (WIF) authentication")
		authOptions = append(authOptions, option.WithTokenSource(ts))
	case OptionValueAuthTypeAppDefaultCredentials, OptionValueAuthTypeDefault, "":
		c.Logger.Debug("Using Application Default Credentials (ADC)", "authType", c.authType)
		// Use Application Default Credentials (default behavior)
		// No additional options needed - ADC is used by default
	case OptionValueAuthTypeAnonymous:
		c.Logger.Debug("Using anonymous authentication")
		authOptions = append(authOptions, option.WithoutAuthentication())
	default:
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[bq] unknown auth type: %s", c.authType),
		}
	}

	if c.quotaProject != "" {
		authOptions = append(authOptions, option.WithQuotaProject(c.quotaProject))
	}

	if c.hasImpersonationOptions() {
		if c.impersonateTargetPrincipal == "" {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] `%s` parameter is empty for impersonation", OptionImpersonateTargetPrincipal),
			}
		}

		var lifetime time.Duration
		if c.impersonateLifetime != 0 {
			lifetime = c.impersonateLifetime
		} else {
			lifetime = 3600 * time.Second
		}

		impCfg := impersonate.CredentialsConfig{
			TargetPrincipal: c.impersonateTargetPrincipal,
			Delegates:       c.impersonateDelegates,
			Scopes:          c.impersonateScopes,
			Lifetime:        lifetime,
		}
		tokenSource, err := impersonate.CredentialsTokenSource(ctx, impCfg)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] failed to create impersonated token source: %s", err.Error()),
			}
		}
		authOptions = []option.ClientOption{option.WithTokenSource(tokenSource)}
	}

	return authOptions, nil
}

func (c *connectionImpl) newClient(ctx context.Context) error {
	if c.catalog == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[bq] ProjectID is empty",
		}
	}

	authOptions, err := c.authOptions(ctx)
	if err != nil {
		return err
	}

	bigQueryAuthOptions := authOptions
	if c.endpoint != "" {
		bigQueryAuthOptions = append(bigQueryAuthOptions, option.WithEndpoint(c.endpoint))
	}

	client, err := bigquery.NewClient(ctx, c.catalog, bigQueryAuthOptions...)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "create client")
	}

	if c.location != "" {
		client.Location = c.location
	}

	// Use original authOptions without custom endpoint for Storage Read API
	storageAuthOptions := authOptions
	if c.storageEndpoint != "" {
		storageAuthOptions = append(storageAuthOptions, option.WithEndpoint(c.storageEndpoint))
		// assume insecure since the purpose of this is to use the emulator, which does not use TLS
		storageAuthOptions = append(storageAuthOptions, option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	}
	err = client.EnableStorageReadClient(ctx, storageAuthOptions...)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "enable storage read client")
	}

	c.client = client
	return nil
}

// table returns a Table handle for the (project, dataset, table) triple,
// bound to this connection's BigQuery client.
func (c *connectionImpl) table(project, dataset, table string) *bigquery.Table {
	return c.client.DatasetInProject(project, dataset).Table(table)
}

// datasetInProject returns a Dataset handle bound to this connection's client.
func (c *connectionImpl) datasetInProject(projectID, datasetID string) *bigquery.Dataset {
	return c.client.DatasetInProject(projectID, datasetID)
}

// getOrCreateStorageApiDisabledClient lazily constructs (and caches) a
// BigQuery client that has NOT had its Storage Read API enabled. Required by
// queries that select pseudo-columns like _PARTITIONDATE/_PARTITIONTIME,
// which return null over the Storage Read API.
func (c *connectionImpl) getOrCreateStorageApiDisabledClient(ctx context.Context) (*bigquery.Client, error) {
	if c.clientStorageApiDisabled != nil {
		return c.clientStorageApiDisabled, nil
	}
	authOptions, err := c.authOptions(ctx)
	if err != nil {
		return nil, err
	}
	if c.endpoint != "" {
		authOptions = append(authOptions, option.WithEndpoint(c.endpoint))
	}
	client, err := bigquery.NewClient(ctx, c.catalog, authOptions...)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err, "create non-storage-api client")
	}
	if c.location != "" {
		client.Location = c.location
	}
	c.clientStorageApiDisabled = client
	return client, nil
}

func (c *connectionImpl) hasImpersonationOptions() bool {
	return c.impersonateTargetPrincipal != "" ||
		len(c.impersonateDelegates) > 0 ||
		len(c.impersonateScopes) > 0
}

var (
	// Dataset:
	//
	// https://cloud.google.com/bigquery/docs/datasets#dataset-naming
	//
	// When you create a dataset in BigQuery, the dataset name must be unique for each project.
	// The dataset name can contain the following:
	//   - Up to 1,024 characters.
	//   - Letters (uppercase or lowercase), numbers, and underscores.
	// Dataset names are case-sensitive by default. mydataset and MyDataset can coexist in the same project,
	// unless one of them has case-sensitivity turned off.
	// Dataset names cannot contain spaces or special characters such as -, &, @, or %.
	datasetRegex = regexp.MustCompile("^[a-zA-Z0-9_-]")
)

func sanitizeDataset(value string) (string, error) {
	if value == "" {
		return value, nil
	}

	if datasetRegex.MatchString(value) {
		if len(value) > 1024 {
			return "", adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "[bq] Dataset name cannot exceed 1024 characters",
			}
		}
		return value, nil
	}

	return "", adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("[bq] invalid characters in dataset `%s`", value),
	}
}

// encodeJson serializes v as a JSON string. Returns "" when v is empty or
// serialization fails. Used to publish list/map metadata (Clustering.Fields,
// ExternalDataConfig.SourceURIs, Labels, ResourceTags, PrimaryKey.Columns)
// as Arrow schema metadata values.
func encodeJson[S ~[]E | ~map[string]E, E any](v S) string {
	ret := ""
	if len(v) > 0 {
		encoded, err := json.Marshal(v)
		if err == nil {
			ret = string(encoded)
		}
	}
	return ret
}

func (c *connectionImpl) getTableSchemaWithFilter(ctx context.Context, catalog *string, dbSchema *string, tableName string, columnName *string) (*arrow.Schema, error) {
	if catalog == nil {
		catalog = &c.catalog
	}

	if dbSchema == nil {
		dbSchema = &c.dbSchema
	}

	md, err := c.client.DatasetInProject(*catalog, *dbSchema).Table(tableName).Metadata(ctx)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusUnknown, err, "get metadata for table %s.%s.%s", quoteIdentifier(*catalog), quoteIdentifier(*dbSchema), quoteIdentifier(tableName))
	}

	metadata := make(map[string]string)
	metadata["Name"] = md.Name
	metadata["Location"] = md.Location
	metadata["Description"] = md.Description
	// md.Schema: the table Schema is defined at the bottom using md.Schema
	if md.MaterializedView != nil {
		metadata["MaterializedView.EnableRefresh"] = strconv.FormatBool(md.MaterializedView.EnableRefresh)
		metadata["MaterializedView.LastRefreshTime"] = md.MaterializedView.LastRefreshTime.Format(time.RFC3339Nano)
		metadata["MaterializedView.Query"] = md.MaterializedView.Query
		metadata["MaterializedView.RefreshInterval"] = md.MaterializedView.RefreshInterval.String()
		metadata["MaterializedView.AllowNonIncrementalDefinition"] = strconv.FormatBool(md.MaterializedView.AllowNonIncrementalDefinition)
		if md.MaxStaleness != nil {
			metadata["MaterializedView.MaxStaleness"] = md.MaxStaleness.String()
		}
	}
	metadata["ViewQuery"] = md.ViewQuery
	metadata["UseLegacySQL"] = strconv.FormatBool(md.UseLegacySQL)
	metadata["UseStandardSQL"] = strconv.FormatBool(md.UseStandardSQL)
	if md.TimePartitioning != nil {
		// "DAY", "HOUR", "MONTH", "YEAR"
		metadata["TimePartitioning.Type"] = string(md.TimePartitioning.Type)
		if md.TimePartitioning.Expiration != 0 {
			metadata["TimePartitioning.Expiration"] = md.TimePartitioning.Expiration.String()
		}
		if md.TimePartitioning.Field != "" {
			metadata["TimePartitioning.Field"] = md.TimePartitioning.Field
		}
	}
	if md.RangePartitioning != nil {
		if md.RangePartitioning.Field != "" {
			metadata["RangePartitioning.Field"] = md.RangePartitioning.Field
		}
		if md.RangePartitioning.Range != nil {
			metadata["RangePartitioning.Range.Start"] = strconv.FormatInt(md.RangePartitioning.Range.Start, 10)
			metadata["RangePartitioning.Range.End"] = strconv.FormatInt(md.RangePartitioning.Range.End, 10)
			metadata["RangePartitioning.Range.Interval"] = strconv.FormatInt(md.RangePartitioning.Range.Interval, 10)
		}
	}
	// Downstream consumers expect this key to always be present, so emit
	// it unconditionally rather than gating on the bool being true.
	metadata["RequirePartitionFilter"] = strconv.FormatBool(md.RequirePartitionFilter)
	if md.Clustering != nil {
		metadata["Clustering.Fields"] = encodeJson[[]string, string](md.Clustering.Fields)
	}
	metadata["ExpirationTime"] = md.ExpirationTime.Format(time.RFC3339Nano)
	metadata["Labels"] = encodeJson[map[string]string, string](md.Labels)
	// TODO: ExternalDataConfig
	if md.ExternalDataConfig != nil {
		metadata["ExternalDataConfig.SourceFormat"] = string(md.ExternalDataConfig.SourceFormat)
		metadata["ExternalDataConfig.SourceURIs"] = encodeJson[[]string, string](md.ExternalDataConfig.SourceURIs)
		// TODO: Schema
		metadata["ExternalDataConfig.AutoDetect"] = strconv.FormatBool(md.ExternalDataConfig.AutoDetect)
		metadata["ExternalDataConfig.Compression"] = string(md.ExternalDataConfig.Compression)
		metadata["ExternalDataConfig.IgnoreUnknownValues"] = strconv.FormatBool(md.ExternalDataConfig.IgnoreUnknownValues)
		metadata["ExternalDataConfig.MaxBadRecords"] = strconv.FormatInt(md.ExternalDataConfig.MaxBadRecords, 10)
		// TODO: Options, do we need this? It looks like it contains the same thing as ExternalDataConfig?
		if md.ExternalDataConfig.HivePartitioningOptions != nil {
			metadata["ExternalDataConfig.HivePartitioningOptions.Mode"] = string(md.ExternalDataConfig.HivePartitioningOptions.Mode)
			metadata["ExternalDataConfig.HivePartitioningOptions.SourceURIPrefix"] = md.ExternalDataConfig.HivePartitioningOptions.SourceURIPrefix
			metadata["ExternalDataConfig.HivePartitioningOptions.RequirePartitionFilter"] = strconv.FormatBool(md.ExternalDataConfig.HivePartitioningOptions.RequirePartitionFilter)
		}
		metadata["ExternalDataConfig.DecimalTargetTypes"] = encodeJson[[]bigquery.DecimalTargetType, bigquery.DecimalTargetType](md.ExternalDataConfig.DecimalTargetTypes)
		metadata["ExternalDataConfig.ConnectionID"] = md.ExternalDataConfig.ConnectionID
		metadata["ExternalDataConfig.ReferenceFileSchemaURI"] = md.ExternalDataConfig.ReferenceFileSchemaURI
		metadata["ExternalDataConfig.MetadataCacheMode"] = string(md.ExternalDataConfig.MetadataCacheMode)
	}
	if md.EncryptionConfig != nil {
		metadata["EncryptionConfig.KMSKeyName"] = md.EncryptionConfig.KMSKeyName
	}
	metadata["FullID"] = md.FullID
	metadata["Type"] = string(md.Type)
	metadata["CreationTime"] = md.CreationTime.Format(time.RFC3339Nano)
	metadata["LastModifiedTime"] = md.LastModifiedTime.Format(time.RFC3339Nano)
	metadata["NumBytes"] = strconv.FormatInt(md.NumBytes, 10)
	metadata["NumLongTermBytes"] = strconv.FormatInt(md.NumLongTermBytes, 10)
	metadata["NumRows"] = strconv.FormatUint(md.NumRows, 10)
	if md.SnapshotDefinition != nil {
		metadata["SnapshotDefinition.BaseTableReference"] = md.SnapshotDefinition.BaseTableReference.FullyQualifiedName()
		metadata["SnapshotDefinition.SnapshotTime"] = md.SnapshotDefinition.SnapshotTime.Format(time.RFC3339Nano)
	}
	if md.CloneDefinition != nil {
		metadata["CloneDefinition.BaseTableReference"] = md.CloneDefinition.BaseTableReference.FullyQualifiedName()
		metadata["CloneDefinition.CloneTime"] = md.CloneDefinition.CloneTime.Format(time.RFC3339Nano)
	}
	if md.StreamingBuffer != nil {
		metadata["StreamingBuffer.EstimatedBytes"] = strconv.FormatUint(md.StreamingBuffer.EstimatedBytes, 10)
		metadata["StreamingBuffer.EstimatedRows"] = strconv.FormatUint(md.StreamingBuffer.EstimatedRows, 10)
		metadata["StreamingBuffer.OldestEntryTime"] = md.StreamingBuffer.OldestEntryTime.Format(time.RFC3339Nano)
	}
	metadata["ETag"] = md.ETag
	metadata["DefaultCollation"] = md.DefaultCollation
	if md.TableConstraints != nil {
		if md.TableConstraints.PrimaryKey != nil {
			metadata["TableConstraints.PrimaryKey.Columns"] = encodeJson[[]string, string](md.TableConstraints.PrimaryKey.Columns)
		}
		// TODO: TableConstraints.ForeignKeys, how do we represent list of structs?
	}
	metadata["ResourceTags"] = encodeJson[map[string]string, string](md.ResourceTags)
	tableMetadata := arrow.MetadataFrom(metadata)

	fields := make([]arrow.Field, len(md.Schema))
	for i, schema := range md.Schema {
		f, err := buildField(schema, 0)
		if err != nil {
			return nil, err
		}
		fields[i] = f
	}
	schema := arrow.NewSchema(fields, &tableMetadata)

	return schema, nil
}

func buildField(schema *bigquery.FieldSchema, level uint) (arrow.Field, error) {
	field := arrow.Field{Name: schema.Name}
	metadata := make(map[string]string)
	metadata["Description"] = schema.Description
	metadata["Repeated"] = strconv.FormatBool(schema.Repeated)
	metadata["Required"] = strconv.FormatBool(schema.Required)
	field.Nullable = !schema.Required
	metadata["Type"] = string(schema.Type)

	richSqlType := string(schema.Type)

	if schema.PolicyTags != nil {
		policyTagList, err := json.Marshal(schema.PolicyTags)
		if err != nil {
			return arrow.Field{}, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("[bq] failed to marshal policy tags for field `%s`: %s", schema.Name, err),
			}
		}
		metadata["PolicyTags"] = string(policyTagList)
	}

	// https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details
	switch schema.Type {
	case bigquery.StringFieldType:
		metadata["MaxLength"] = strconv.FormatInt(schema.MaxLength, 10)
		metadata["Collation"] = schema.Collation
		field.Type = arrow.BinaryTypes.String
	case bigquery.BytesFieldType:
		metadata["MaxLength"] = strconv.FormatInt(schema.MaxLength, 10)
		field.Type = arrow.BinaryTypes.Binary
	case bigquery.IntegerFieldType:
		field.Type = arrow.PrimitiveTypes.Int64
	case bigquery.FloatFieldType:
		field.Type = arrow.PrimitiveTypes.Float64
		// NOTE: FloatFieldType is "FLOAT", which is invalid as a literal in BigQuery
		richSqlType = "FLOAT64"
	case bigquery.BooleanFieldType:
		field.Type = arrow.FixedWidthTypes.Boolean
	case bigquery.TimestampFieldType:
		field.Type = arrow.FixedWidthTypes.Timestamp_us
	case bigquery.RecordFieldType:
		nestedFields := make([]arrow.Field, len(schema.Schema))
		nestedRichSqlTypes := make([]string, len(schema.Schema))
		for i, nestedSchema := range schema.Schema {
			f, err := buildField(nestedSchema, level+1)
			if err != nil {
				return arrow.Field{}, err
			}
			nestedFields[i] = f

			fieldRichSqlType, found := f.Metadata.GetValue("BIGQUERY:type")
			if found {
				nestedRichSqlTypes[i] = fmt.Sprintf("%s %s", quoteIdentifier(f.Name), fieldRichSqlType)
			}
		}
		structType := arrow.StructOf(nestedFields...)
		richSqlType = fmt.Sprintf("STRUCT<%s>", strings.Join(nestedRichSqlTypes, ", "))
		if structType == nil {
			return arrow.Field{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("Cannot create a struct schema for record `%s`", schema.Name),
			}
		}
		field.Type = structType

	case bigquery.DateFieldType:
		field.Type = arrow.FixedWidthTypes.Date32
	case bigquery.TimeFieldType:
		field.Type = arrow.FixedWidthTypes.Time64us
	case bigquery.DateTimeFieldType:
		field.Type = &arrow.TimestampType{Unit: arrow.Microsecond}
	case bigquery.NumericFieldType:
		// While BigQuery tracks precision/scale, in practice the data
		// only ever has precision 38 and scale 9.  Hardcode those to
		// be accurate.
		// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
		field.Type = &arrow.Decimal128Type{
			Precision: 38,
			Scale:     9,
		}
	case bigquery.RangeFieldType:
		var childType arrow.DataType
		switch schema.RangeElementType.Type {
		case bigquery.DateFieldType:
			childType = arrow.FixedWidthTypes.Date32
			richSqlType = "RANGE<DATE>"
		case bigquery.DateTimeFieldType:
			childType = &arrow.TimestampType{Unit: arrow.Microsecond}
			richSqlType = "RANGE<DATETIME>"
		case bigquery.TimestampFieldType:
			childType = arrow.FixedWidthTypes.Timestamp_us
			richSqlType = "RANGE<TIMESTAMP>"
		default:
			return arrow.Field{}, adbc.Error{
				Code: adbc.StatusNotImplemented,
				Msg:  fmt.Sprintf("[bq] %s is not supported in range", schema.RangeElementType.Type),
			}
		}
		field.Type = arrow.StructOf(
			arrow.Field{Name: "start", Type: childType, Nullable: true},
			arrow.Field{Name: "end", Type: childType, Nullable: true})
	case bigquery.GeographyFieldType:
		field.Type = arrow.BinaryTypes.String
		metadata["ARROW:extension:name"] = "geoarrow.wkt"
		metadata["ARROW:extension:metadata"] = `{"crs": "EPSG:4326", "crs_type": "authority_code", "edges": "spherical"}`
	case bigquery.BigNumericFieldType:
		// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
		field.Type = &arrow.Decimal256Type{
			Precision: 76,
			Scale:     38,
		}
	case bigquery.JSONFieldType:
		field.Type = arrow.BinaryTypes.String
		metadata["ARROW:extension:name"] = "arrow.json"
	case bigquery.IntervalFieldType:
		field.Type = arrow.FixedWidthTypes.MonthDayNanoInterval
	default:
		return arrow.Field{}, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("Google SQL type `%s` is not supported yet", schema.Type),
		}
	}

	if schema.Repeated {
		// Keep most of the metadata on the top level field, but the
		// extension has to go on the child
		var childMetadata map[string]string
		if ext, ok := metadata["ARROW:extension:name"]; ok {
			childMetadata = map[string]string{"ARROW:extension:name": ext}
			delete(metadata, "ARROW:extension:name")
		}
		field.Type = arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     field.Type,
			Nullable: !schema.Required,
			Metadata: arrow.MetadataFrom(childMetadata),
		})
		richSqlType = fmt.Sprintf("ARRAY<%s>", richSqlType)
	}

	// derive the standard type string from the field
	metadata["BIGQUERY:type"] = richSqlType

	if level == 0 {
		metadata["DefaultValueExpression"] = schema.DefaultValueExpression
	}
	field.Metadata = arrow.MetadataFrom(metadata)
	return field, nil
}

func (c *connectionImpl) Token() (*oauth2.Token, error) {
	token, err := c.getAccessToken()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	return &oauth2.Token{
		AccessToken:  token.AccessToken,
		TokenType:    "Bearer",
		RefreshToken: c.refreshToken,
		Expiry:       now.Add(time.Second * time.Duration(token.ExpiresIn)),
	}, nil
}

func (c *connectionImpl) getAccessToken() (*bigQueryTokenResponse, error) {
	params := url.Values{}
	params.Add("grant_type", "refresh_token")
	params.Add("client_id", c.clientID)
	params.Add("client_secret", c.clientSecret)
	params.Add("refresh_token", c.refreshToken)
	endpoint := c.accessTokenEndpoint
	if endpoint == "" {
		endpoint = AccessTokenEndpoint
	}
	serverName := c.accessTokenServerName
	if serverName == "" {
		serverName = AccessTokenServerName
	}
	req, err := http.NewRequest("POST", endpoint, bytes.NewBufferString(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{ServerName: serverName},
	}
	client := &http.Client{
		Transport: tr,
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err, "get access token")
	}
	defer func(Body io.ReadCloser) {
		bodyErr := Body.Close()
		if bodyErr != nil {
			err = bodyErr
		}
	}(resp.Body)

	contents, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err, "get access token")
	}

	var tokenResponse bigQueryTokenResponse
	err = json.Unmarshal(contents, &tokenResponse)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err, "get access token")
	}

	if tokenResponse.Error != "" {
		msg := fmt.Sprintf("[bq] OAuth token error: %s", tokenResponse.Error)
		if tokenResponse.ErrorDescription != "" {
			msg += ": " + tokenResponse.ErrorDescription
		}
		if isReauthError(tokenResponse.ErrorDescription) {
			msg += ". " + reauthGuidance
		}
		if tokenResponse.ErrorURI != "" {
			msg += " (see: " + tokenResponse.ErrorURI + ")"
		}
		return nil, adbc.Error{
			Code: adbc.StatusUnauthorized,
			Msg:  msg,
		}
	}

	return &tokenResponse, nil
}

func detectCredentialTypeFromFile(filename string) (option.CredentialsType, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return "", fmt.Errorf("read credential file: %w", err)
	}
	return detectCredentialTypeFromJSON(data)
}

func detectCredentialTypeFromJSON(data []byte) (option.CredentialsType, error) {
	var f struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return "", fmt.Errorf("parse credential JSON: %w", err)
	}
	if f.Type == "" {
		return "", fmt.Errorf("missing 'type' field in credential JSON")
	}
	return option.CredentialsType(f.Type), nil
}
