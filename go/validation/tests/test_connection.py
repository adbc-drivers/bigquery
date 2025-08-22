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

import time

import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.connection as connection_tests

from .bigquery import BigQueryQuirks


def pytest_generate_tests(metafunc) -> None:
    return connection_tests.generate_tests(BigQueryQuirks(), metafunc)


class TestConnection(connection_tests.TestConnection):
    def test_get_table_schema(self, driver, conn, query) -> None:
        # BigQuery tends to be flaky when we reuse the same table
        for i in range(5):
            try:
                super().test_get_table_schema(driver, conn, query)
            except adbc_driver_manager.dbapi.ProgrammingError as e:
                if "Exceeded rate limits" in str(e):
                    time.sleep(min(15, 2 ** (i + 2)))
                    continue
                else:
                    raise
            else:
                break
