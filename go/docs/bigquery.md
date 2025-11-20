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
      "adbc.bigquery.sql.dataset_id": "my-gcp-datase"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v0.1.1](./v0.1.1.md)

{{ footnotes|safe }}

[bigquery]: https://cloud.google.com/bigquery/
