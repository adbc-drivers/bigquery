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


import adbc_drivers_validation.tests.connection as connection_tests

from . import bigquery, utils


def pytest_generate_tests(metafunc) -> None:
    return connection_tests.generate_tests(bigquery.QUIRKS, metafunc)


class TestConnection(connection_tests.TestConnection):
    @utils.retry_rate_limit
    def test_get_objects_catalog(self, conn, driver) -> None:
        super().test_get_objects_catalog(conn, driver)

    @utils.retry_rate_limit
    def test_get_objects_schema(self, conn, driver) -> None:
        super().test_get_objects_schema(conn, driver)

    @utils.retry_rate_limit
    def test_get_objects_table_not_exist(self, conn, driver) -> None:
        super().test_get_objects_table_not_exist(conn, driver)

    @utils.retry_rate_limit
    def test_get_objects_table_present(self, conn, driver, get_objects_table) -> None:
        super().test_get_objects_table_present(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_table_invalid_catalog(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_table_invalid_catalog(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_table_invalid_schema(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_table_invalid_schema(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_table_invalid_table(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_table_invalid_table(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_table_exact_table(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_table_exact_table(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_column_not_exist(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_column_not_exist(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_column_present(self, conn, driver, get_objects_table) -> None:
        super().test_get_objects_column_present(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_column_filter_column_name(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_column_filter_column_name(
            conn, driver, get_objects_table
        )

    @utils.retry_rate_limit
    def test_get_objects_column_filter_table_name(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_column_filter_table_name(
            conn, driver, get_objects_table
        )

    @utils.retry_rate_limit
    def test_get_objects_column_filter_catalog(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_column_filter_catalog(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_column_filter_schema(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_column_filter_schema(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_column_filter_table(
        self, conn, driver, get_objects_table
    ) -> None:
        super().test_get_objects_column_filter_table(conn, driver, get_objects_table)

    @utils.retry_rate_limit
    def test_get_objects_column_xdbc(self, conn, driver, get_objects_table) -> None:
        super().test_get_objects_column_xdbc(conn, driver, get_objects_table)
