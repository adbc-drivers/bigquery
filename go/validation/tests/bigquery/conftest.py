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

import os

import pytest


def pytest_generate_tests(metafunc) -> None:
    metafunc.parametrize(
        "driver",
        [pytest.param("bigquery:", id="bigquery")],
        scope="module",
        indirect=["driver"],
    )


@pytest.fixture(scope="session")
def bigquery_project() -> str:
    """BigQuery project ID. Example: GOOGLE_CLOUD_PROJECT=my-project-123"""
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        pytest.skip("Must set GOOGLE_CLOUD_PROJECT environment variable")
    return project


@pytest.fixture(scope="session")
def bigquery_dataset() -> str:
    """BigQuery dataset ID. Example: BIGQUERY_DATASET_ID=test_dataset"""
    dataset = os.environ.get("BIGQUERY_DATASET_ID")
    if not dataset:
        pytest.skip("Must set BIGQUERY_DATASET_ID environment variable")
    return dataset




