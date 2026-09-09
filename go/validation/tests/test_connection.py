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


import uuid

import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.connection as connection_tests
import pytest

from . import bigquery, utils


def pytest_generate_tests(metafunc) -> None:
    quirks = [bigquery.get_quirks(metafunc.config.getoption("vendor_version"))]
    return connection_tests.generate_tests(quirks, metafunc)


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


@pytest.mark.parametrize(
    "option",
    ["bigquery.impersonate.delegates", "bigquery.impersonate.scopes"],
)
def test_impersonate_empty_value(driver, driver_path, db_kwargs, option) -> None:
    # An explicitly empty delegate/scope list means "none". Splitting it
    # naively yields a single empty entry, which reads as "impersonation is
    # configured" and then fails for a missing target principal.
    kwargs = dict(db_kwargs)
    kwargs[option] = ""

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path, db_kwargs=kwargs, autocommit=True
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)

        # No impersonation was configured, so no lifetime is reported.
        lifetime = conn.adbc_connection.get_option("bigquery.impersonate.lifetime")
        assert lifetime == "", lifetime


def test_get_table_schema_table_metadata(driver, conn) -> None:
    # Table-level BigQuery metadata published on the Arrow schema.
    table = f"validation_table_metadata_{uuid.uuid4().hex[:10]}"
    with conn.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {table} (d DATE, a INT64) PARTITION BY d CLUSTER BY a"
            " OPTIONS(require_partition_filter=true)"
        )

    try:
        metadata = conn.adbc_get_table_schema(table).metadata

        # Preserve legacy keys
        assert b"CreationTime" in metadata
        assert b"DefaultCollation" in metadata
        assert b"Description" in metadata
        assert b"ETag" in metadata
        assert b"FullID" in metadata
        assert b"LastModifiedTime" in metadata
        assert b"Location" in metadata
        assert b"Name" in metadata
        assert b"NumBytes" in metadata
        assert b"NumLongTermBytes" in metadata
        assert b"NumRows" in metadata
        assert metadata[b"TimePartitioning.Type"] == b"DAY"
        assert metadata[b"TimePartitioning.Field"] == b"d"

        # New keys
        assert metadata[b"BIGQUERY:clustering:fields"] == b'["a"]'
        assert metadata[b"BIGQUERY:require_partition_filter"] == b"true"
        assert metadata[b"BIGQUERY:time_partitioning:field"] == b"d"
        assert metadata[b"BIGQUERY:expiration_time"]
        assert b"BIGQUERY:resource_tags" in metadata
        assert b"BIGQUERY:view_query" in metadata
        assert metadata[b"BIGQUERY:use_legacy_sql"] == b"false"
        assert metadata[b"BIGQUERY:use_standard_sql"] == b"false"
    finally:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")


def test_get_table_schema_view_metadata(driver, conn) -> None:
    view = f"validation_view_metadata_{uuid.uuid4().hex[:10]}"
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE VIEW {view} AS SELECT 1 AS x")

    try:
        metadata = conn.adbc_get_table_schema(view).metadata

        # Preserve legacy keys
        assert b"CreationTime" in metadata
        assert b"DefaultCollation" in metadata
        assert b"Description" in metadata
        assert b"ETag" in metadata
        assert b"FullID" in metadata
        assert b"LastModifiedTime" in metadata
        assert b"Location" in metadata
        assert b"Name" in metadata
        assert b"NumBytes" in metadata
        assert b"NumLongTermBytes" in metadata
        assert b"NumRows" in metadata

        # New keys
        assert metadata[b"BIGQUERY:view_query"] == b"SELECT 1 AS x"
        assert metadata[b"BIGQUERY:type"] == b"VIEW"
        assert metadata[b"BIGQUERY:require_partition_filter"] == b"false"
        assert b"BIGQUERY:clustering:fields" not in metadata
    finally:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP VIEW IF EXISTS {view}")
