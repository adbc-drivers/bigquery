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

import adbc_drivers_validation.tests.ingest

from . import bigquery, utils


def pytest_generate_tests(metafunc) -> None:
    return adbc_drivers_validation.tests.ingest.generate_tests(
        bigquery.QUIRKS,
        metafunc,
        ingest_mode_queries={"ingest/string", "ingest/string:storagewrite"},
    )


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    @utils.retry_rate_limit
    def test_create(self, driver, conn, query) -> None:
        super().test_create(driver, conn, query)

    @utils.retry_rate_limit
    def test_append(self, driver, conn, query) -> None:
        super().test_create(driver, conn, query)

    @utils.retry_rate_limit
    def test_append_fail(self, driver, conn, query) -> None:
        super().test_append_fail(driver, conn, query)

    @utils.retry_rate_limit
    def test_createappend(self, driver, conn, query) -> None:
        super().test_createappend(driver, conn, query)

    @utils.retry_rate_limit
    def test_replace(self, driver, conn, query) -> None:
        super().test_replace(driver, conn, query)

    @utils.retry_rate_limit
    def test_replace_noop(self, driver, conn, query) -> None:
        super().test_replace_noop(driver, conn, query)

    @utils.retry_rate_limit
    def test_not_null(self, driver, conn) -> None:
        super().test_not_null(driver, conn)

    @utils.retry_rate_limit
    def test_schema(self, driver, conn) -> None:
        super().test_schema(driver, conn)

    @utils.retry_rate_limit
    def test_catalog(self, driver, conn) -> None:
        super().test_catalog(driver, conn)
