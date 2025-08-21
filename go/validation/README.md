<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Validation Suite Setup

The following must be set up.

- Google Cloud Project
- A dataset, stored in `BIGQUERY_DATASET_ID`
- A secondary dataset, stored in `BIGQUERY_SECONDARY_DATASET_ID`

Additionally, you must be logged in to Google Cloud via `gcloud auth
application-default login`.

## Using clusters in CI

Follow the instructions here:
https://github.com/google-github-actions/auth?tab=readme-ov-file#indirect-wif

Authorize _all branches_ on this repo to access the cluster, not just `main`.
This means PRs from individuals still can't be tested, but PRs from
contributors (who do not use a fork) can be.
