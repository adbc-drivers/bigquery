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

from . import bigquery, utils


def pytest_generate_tests(metafunc) -> None:
    return query_tests.generate_tests(bigquery.QUIRKS, metafunc)


class TestQuery(query_tests.TestQuery):
    @utils.retry_rate_limit
    def test_execute_schema(self, driver, conn, query) -> None:
        super().test_execute_schema(driver, conn, query)

    @utils.retry_rate_limit
    def test_get_table_schema(self, driver, conn_factory, query) -> None:
        super().test_get_table_schema(driver, conn_factory, query)
