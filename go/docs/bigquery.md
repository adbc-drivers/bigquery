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

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.

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

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.1.1](./v0.1.1.md)

{{ footnotes|safe }}

[bigquery]: https://cloud.google.com/bigquery/
