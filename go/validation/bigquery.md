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

This driver provides access to [BigQuery][bigquery], a data warehouse
offered by Google Cloud.

## Installation & Quickstart

The driver can be installed with `dbc`.

To use the driver:

1. Authenticate with Google Cloud (e.g. via `gcloud auth application-default
   login`).
1. Provide the database options `adbc.bigquery.sql.project_id` and
   `adbc.bigquery.sql.dataset_id`.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

[bigquery]: https://cloud.google.com/bigquery/
