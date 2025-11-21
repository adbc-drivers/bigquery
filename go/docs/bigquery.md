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
# Google BigQuery Driver {{ version }}

:::{note}
This project is not associated with Google.
:::

{{ version_header|safe }}

This driver provides access to [Google BigQuery][bigquery], a data warehouse
offered by Google Cloud.

## Installation & Quickstart

The driver can be installed with `dbc`.

To use the driver:

1. Authenticate with Google Cloud (e.g. via `gcloud auth application-default
   login`).
1. Provide the database options `adbc.bigquery.sql.project_id` and
   `adbc.bigquery.sql.dataset_id`.

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
- Additional Parameters: Connection configuration as key-value pairs. For a complete list of available parameters, see the [Simba BigQuery JDBC Connector Configuration Options](https://storage.googleapis.com/simba-bq-release/jdbc/Simba%20Google%20BigQuery%20JDBC%20Connector%20Install%20and%20Configuration%20Guide_1.6.3.1004.pdf).

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`.
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

{{ footnotes|safe }}

[bigquery]: https://cloud.google.com/bigquery/
