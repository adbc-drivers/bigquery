---
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
{}
---

{{ cross_reference|safe }}
# BigQuery Driver {{ version }}

{{ heading|safe }}

This driver provides access to [BigQuery][bigquery], a data warehouse offered by Google Cloud.

:::{note}
This project is not affiliated with Google.
:::

## Installation

The BigQuery driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install bigquery
```

## Pre-requisites

Using the BigQuery driver requires some setup before you can connect:

1. Create a [Google Cloud account](http://console.cloud.google.com)
1. Install the [Google Cloud CLI](https://cloud.google.com/cli) (for managing credentials)
1. Authenticate with Google Cloud
    - Run `gcloud auth application-default login`
1. Create, find, or reuse a project and dataset (record these for later)

## Connecting

To connect, replace `my-gcp-project` and `my-gcp-dataset` below with the appropriate values for your situation and run the following:

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
  driver="bigquery",
  db_kwargs={
      "adbc.bigquery.sql.project_id": "my-gcp-project",
      "adbc.bigquery.sql.dataset_id": "my-gcp-dataset"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers. See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

The driver supports connecting with individual options or connection strings.

## Connection String Format

BigQuery URI syntax:

```
bigquery://[Host]:[Port]/ProjectID?OAuthType=[AuthValue]&[Key]=[Value]&[Key]=[Value]...
```

The format follows a similar approach to the [Simba BigQuery JDBC Connection String Format](https://storage.googleapis.com/simba-bq-release/jdbc/Simba%20Google%20BigQuery%20JDBC%20Connector%20Install%20and%20Configuration%20Guide_1.6.3.1004.pdf).

Components:

- `Scheme`: `bigquery://` (required)
- `Host`: BigQuery API endpoint (optional, defaults to `bigquery.googleapis.com`)
- `Port`: TCP port (optional, defaults to 443)
- `ProjectID`: Google Cloud Project ID (required)
- `OAuthType`: Authentication type number (optional, defaults to `0`)
  - `0` - Application Default Credentials (default)
  - `1` - Service Account JSON File
  - `2` - Service Account JSON String
  - `3` - User Authentication (OAuth)

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`.
:::

:::{note}
Overriding the endpoint via the URI will lead to HTTPS always being assumed.
:::

Additional parameters can be provided as options, and some options may also be passed as query parameters. The query parameters supported by the [Simba BigQuery JDBC Connector](https://storage.googleapis.com/simba-bq-release/jdbc/Simba%20Google%20BigQuery%20JDBC%20Connector%20Install%20and%20Configuration%20Guide_1.6.3.1004.pdf) are generally supported. See [Options][#options] below for more information.

:::{note}
The query parameter is case sensitive.
:::

Examples:

- `bigquery:///my-project-123` (uses Application Default Credentials)
- `bigquery://bigquery.googleapis.com/my-project-123?OAuthType=1&AuthCredentials=/path/to/key.json`
- `bigquery:///my-project-123?OAuthType=3&AuthClientId=123.apps.googleusercontent.com&AuthClientSecret=secret&AuthRefreshToken=token`
- `bigquery://bigquery.googleapis.com/my-project-123?OAuthType=0&DatasetId=analytics&Location=US`
- `bigquery:///my-project-123?OAuthType=2&AuthCredentials=%7B%22type%22%3A%22service_account%22...%7D&DatasetId=data_warehouse`

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

### Query Statistics

A selection of query statistics and other metadata is supplied in the metadata of the Arrow schema of the result set.

### Storage Write API Support

:::{warning}
This functionality is experimental and is not currently recommended for production.
:::

The BigQuery driver supports using the [Storage Write API](https://docs.cloud.google.com/bigquery/docs/write-api) to ingest data. It can be enabled by setting `bigquery.bulk_ingest.method` (see [](#ingest-options) below).

## Options

### Connection Options

`bigquery.auth_type`
: **Values:** `auth_bigquery`, `json_credential_file`, `json_credential_string`, `anonymous`, `user_authentication`, `app_default_credentials`, `json_credentials`, `oauth_client_ids`. **Default:** `auth_bigquery`. **URI Parameter:** `OAuthType`

  How to authenticate.

`bigquery.auth.credentials`
: **Type:** string. **Default:** (empty) **URI Parameter:** `AuthCredentials`

  If using `json_credential_string` authentication, then this is the JSON blob containing the credentials. If using `json_credential_file`, then it is the path to the credentials file.

`bigquery.auth.credentials_type`
: **Values:** `service_account`, `authorized_user`, `impersonated_service_account`, `external_account`. **Default:** (empty)

  If using `json_credential_string` or `json_credential_file` authentication, this is the type of the credentials contained within. If not specified, it will be detected.

`bigquery.auth.client_id`
: **Type:** string. **Default:** (empty) **URI Parameter:** `AuthClientId`

  If using OAuth user authentication, this is the OAuth client ID.

`bigquery.auth.client_secret`
: **Type:** string. **Default:** (empty) **URI Parameter:** `AuthClientSecret`

  If using OAuth user authentication, this is the OAuth client secret.

`bigquery.auth.quota_project`
: **Type:** string. **Default:** (empty) **URI Parameter:** `QuotaProject`

  The project to use for quota.

`bigquery.auth.refresh_token`
: **Type:** string. **Default:** (empty) **URI Parameter:** `AuthRefreshToken`

  If using OAuth user authentication, this is the OAuth refresh token.

`bigquery.dataset_id`
: **Type:** string. **Default:** (empty) **URI Parameter:** `DatasetId`

  The dataset to connect to.

`bigquery.endpoint`
: **Type:** string. **Default:** (empty)

  Override the location of the API endpoint for BigQuery; this can be used to connect to an emulator instead. It should be `http://host:port`.

`bigquery.impersonate.delegates`
: **Type:** string (comma-separated list). **Default:** (empty) **URI Parameter:** `ImpersonateDelegates`

  The service account email addresses in the delegation chain. Each service account must have roles/iam.serviceAccountTokenCreator for the next service account in the chain.

`bigquery.impersonate.lifetime`
: **Type:** string (Go duration string, e.g. `300ms`). **Default:** (empty) **URI Parameter:** `ImpersonateLifetime`

  The amount of time until the impersonated token expires.

`bigquery.impersonate.scopes`
: **Type:** string (comma-separated list). **Default:** (empty) **URI Parameter:** `ImpersonateScopes`

  The scopes that the impersonated credential should have.

`bigquery.impersonate.target_principal`
: **Type:** string. **Default:** (empty) **URI Parameter:** `ImpersonateTargetPrincipal`

  The principal to impersonate.

`bigquery.location`
:**Type:** string. **Default:** (none)  **URI Parameter:** `Location`

  The project location.

`bigquery.project_id`
: **Type:** string.

  The project to connect to.

`bigquery.storage_endpoint`
: **Type:** string. **Default:** (empty)

  Override the location of the gRPC API endpoint for BigQuery Storage; this can be used to connect to an emulator instead. It should be a `host:port`.

### Ingest Options

`bigquery.bulk_ingest.compression`
: **Values:** `none`, `lz4`, `zstd`. **Default:** `none`

  When using the Storage Write API to ingest data, what kind of compression (if any) to apply to the Arrow data.

  Can also be set on initial connection.

`bigquery.bulk_ingest.method`
: **Values:** `load`, `storage_write`. **Default:** `load`

  How to ingest data.

  - `load`: [run a batch load](https://docs.cloud.google.com/bigquery/docs/batch-loading-data)
  - `storage_write`: (**experimental**) use the [Storage Write API](https://docs.cloud.google.com/bigquery/docs/write-api)

  Can also be set on initial connection.

### Query Options

`bigquery.query.allow_large_results`
: **Type:** boolean. **Default:** false

  Allow arbitrarily large query results, at the cost of query performance (even if the result set is not large). For more information, see the [BigQuery documentation](https://cloud.google.com/bigquery/querying-data#largequeryresults).

`bigquery.query.create_disposition`
: **Values:** `CREATE_IF_NEEDED`, `CREATE_NEVER`. **Default:** `CREATE_IF_NEEDED`

  Control when to create the destination table.

`bigquery.query.create_session`
: **Type:** boolean. **Default:** false

  Create a new session.

`bigquery.query.default_dataset_id`
: **Type:** string. **Default:** the current catalog of the connection

  The dataset in which to locate unqualified table names.

  This is more easily accessible through ADBC's current catalog option.

`bigquery.query.default_project_id`
: **Type:** string. **Default:** the current catalog of the connection

  The dataset in which to locate unqualified table names.

  This is more easily accessible through ADBC's current schema option.

`bigquery.query.destination_table`
: **Type:** string. **Default:** create a temporary table

  The destination table for query results. If empty (the default), query results will be written to a temporary table.

`bigquery.query.disable_flattened_results`
: **Type:** boolean. **Default:** false

  Whether to flatten nested data. While the driver passes through this option to the BigQuery API, it is not relevant as the driver fetches results in Arrow format (which naturally supports nested data).

`bigquery.query.disable_query_cache`
: **Type:** boolean. **Default:** false

  Disable fetching results from the query cache. For more information, see the [BigQuery documentation](https://cloud.google.com/bigquery/querying-data#querycaching).

`bigquery.query.dry_run`
: **Type:** boolean. **Default:** false

  Do not run the query, but return the result set schema and other query metadata.

  This is more easily accessible through ADBC's ExecuteSchema.

`bigquery.query.job_timeout`
: **Type:** integer. **Default:** 0

  If nonzero, request that BigQuery apply a best-effort timeout (in milliseconds) to the job. BigQuery considers this option experimental, and it may be removed without notice.

`bigquery.query.max_billing_tier`
: **Type:** integer. **Default:** 0 (use the project default)

  The maximum billing tier for this query. Queries that use resources beyond this tier will fail.

`bigquery.query.max_bytes_billed`
: **Type:** integer. **Default:** 0 (use the project default)

  Limit the number of bytes that can be filled for this job. Queries that would exceed this limit will fail without incurring a charge.

`bigquery.query.parameter_mode`
: **Values:** `named`, `positional`. **Default:** `positional`

  Whether the query uses positional (`?`) or named (`@p`) parameters. A query cannot mix both kinds.

`bigquery.query.prefetch_concurrency`
: **Type:** integer. **Default:** 10

  This option currently has no effect (unless you set it to 0, which will cause the reader to hang).

`bigquery.query.priority`
: **Values:** `BATCH`, `INTERACTIVE`. **Default:** `INTERACTIVE`

  The priority with which to schedule the query. For more information, see the [BigQuery documentation](https://cloud.google.com/bigquery/querying-data#batchqueries).

`bigquery.query.result_buffer_size`
: **Type:** integer. **Default:** 200

  The maximum number of Arrow record batches to buffer in memory.

`bigquery.query.use_legacy_sql`
: **Type:** boolean. **Default:** false

  Use BigQuery's [legacy SQL dialect](https://docs.cloud.google.com/bigquery/docs/reference/legacy-sql), which is not recommended.

`bigquery.query.write_disposition`
: **Values:** `WRITE_APPEND`, `WRITE_TRUNCATE`, `WRITE_TRUNCATE_DATA`, `WRITE_EMPTY`. **Default:** `WRITE_EMPTY`

  What to do about existing data in the target table.

  - `WRITE_APPEND`: upon successful completion of the job, atomically append to the existing table
  - `WRITE_TRUNCATE`: upon successful completion of the job, atomically overwrite existing data
  - `WRITE_TRUNCATE_DATA`: overwrite the data, but keep existing constraints and schema
  - `WRITE_EMPTY`: fail if the destination table already contains data

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v1.11.0](./v1.11.0.md)
- [v1.10.0](./v1.10.0.md)
- [v1.0.0](./v1.0.0.md)

{{ footnotes|safe }}

[bigquery]: https://cloud.google.com/bigquery/
