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

import adbc_drivers_validation.tests.query as query_tests
import pytest
from adbc_drivers_validation import model

from . import bigquery, utils


def pytest_generate_tests(metafunc) -> None:
    all_quirks = [bigquery.get_quirks(metafunc.config.getoption("vendor_version"))]

    if metafunc.definition.name == "test_query_direct":
        combinations = []
        for quirks in all_quirks:
            driver_param = f"{quirks.name}:{quirks.short_version}"
            for query in quirks.query_set.queries.values():
                if not isinstance(query.query, model.SelectQuery):
                    continue
                if query.query.bind_query(quirks) is not None:
                    continue
                combinations.append(
                    pytest.param(
                        driver_param,
                        query,
                        id=f"{driver_param}:{query.name}",
                        marks=query.pytest_marks,
                    )
                )
        metafunc.parametrize(
            "driver,query",
            combinations,
            scope="module",
            indirect=["driver"],
        )
        return

    return query_tests.generate_tests(all_quirks, metafunc)


class TestQuery(query_tests.TestQuery):
    @utils.retry_rate_limit
    def test_query(self, driver, conn, query, query_setup) -> None:
        super().test_query(driver, conn, query, query_setup)

    @utils.retry_rate_limit
    def test_query_direct(self, driver, conn, query, query_setup) -> None:
        modified = model.Query(
            name=f"{query.name}:direct",
            query=query.query,
            metadata_paths=[
                {
                    "setup": {
                        "statement": {
                            "options": {
                                "bigquery.query.job_creation_mode": "optional",
                                "bigquery.query.results_format": "arrow",
                            },
                        },
                    },
                    "tags": {
                        "broken-vendor": None,
                        "variant": "Job Creation Optional",
                    },
                },
                *query.metadata_paths,
            ],
        )
        fake_quirks = bigquery.BigQueryQuirks()
        fake_quirks.features = fake_quirks.features.with_values(
            metadata_type_name=False
        )
        # TODO: handle JSON (no extension type so we don't know to inject it)
        if query.name == "type/select/json":
            pytest.skip()
        super().test_query(fake_quirks, conn, modified, query_setup)

    @utils.retry_rate_limit
    def test_execute_schema(self, driver, conn, query, query_setup) -> None:
        super().test_execute_schema(driver, conn, query, query_setup)

    @utils.retry_rate_limit
    def test_get_table_schema(self, driver, conn, query, query_setup) -> None:
        super().test_get_table_schema(driver, conn, query, query_setup)
