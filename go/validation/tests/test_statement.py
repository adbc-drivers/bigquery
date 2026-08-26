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

import adbc_drivers_validation.tests.statement as statement_tests
import pytest

from . import bigquery, utils


def pytest_generate_tests(metafunc) -> None:
    quirks = [bigquery.get_quirks(metafunc.config.getoption("vendor_version"))]
    return statement_tests.generate_tests(quirks, metafunc)


class TestStatement(statement_tests.TestStatement):
    @utils.retry_rate_limit
    def test_rows_affected(self, driver, conn) -> None:
        super().test_rows_affected(driver, conn)


def test_dry_run(driver, conn) -> None:
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_options(**{"adbc.bigquery.sql.query.dry_run": True})
        cursor.execute("SELECT 1 AS a, 'foobar' as b")
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "a"
        assert cursor.description[1][0] == "b"

        cursor.execute("SELECT 1 AS a, 'foobar' as b", parameters=[(1,), (2,)])
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "a"
        assert cursor.description[1][0] == "b"

        cursor.execute("SELECT 1 AS a, 'foobar' as b", parameters=[(1,), (2,)])
        schema = cursor.fetchallarrow().schema
        assert schema.metadata[b"BIGQUERY:Statistics:Query:StatementType"] == b"SELECT"


def test_script_no_results(driver, conn) -> None:
    # Regression test for https://github.com/dbt-labs/dbt-core/issues/16081
    # That bug never made it into the Driver Foundry driver, but guard against
    # it regardless.
    target_table = f"test_script_no_results_{uuid.uuid4().hex}"

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {target_table} (idx INT, val INT)")
            cursor.execute(f"""
            CREATE TEMP TABLE staging AS SELECT 1 AS idx, 2 AS val;
            MERGE INTO {target_table} AS DEST
            USING (SELECT * FROM staging) AS SOURCE
            ON DEST.idx = SOURCE.idx
            WHEN MATCHED THEN
                UPDATE SET val = SOURCE.val
            WHEN NOT MATCHED THEN
                INSERT (idx, val)
                VALUES (SOURCE.idx, SOURCE.val);
            DROP TABLE IF EXISTS staging;
            """)
            assert not cursor.description
            schema = cursor.fetch_arrow_table().schema
            assert (
                schema.metadata[b"BIGQUERY:Statistics:Query:StatementType"] == b"SCRIPT"
            )

            cursor.execute(f"SELECT val FROM {target_table} WHERE idx = 1")
            assert cursor.fetchone() == (2,)
    finally:
        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=target_table)


@pytest.mark.xfail
def test_script_results(driver, conn) -> None:
    # This should at least read the first result set, but currently it is not supported.
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1; SELECT 'foobar'")
        table = cursor.fetch_arrow_table()
        schema = table.schema
        assert schema.metadata[b"BIGQUERY:Statistics:Query:StatementType"] == b"SCRIPT"

        assert len(table) == 1, repr(table)


STORAGE_API_DISABLED = "bigquery.query.use_storage_api_disabled_client"


def test_use_storage_api_disabled_client_option_roundtrip(driver, conn) -> None:
    with conn.cursor() as cursor:
        statement = cursor.adbc_statement
        assert statement.get_option(STORAGE_API_DISABLED) == "false"
        statement.set_options(**{STORAGE_API_DISABLED: "true"})
        assert statement.get_option(STORAGE_API_DISABLED) == "true"


def test_use_storage_api_disabled_client_pseudo_columns(driver, conn) -> None:
    # Ingestion-time partitioning is what exposes _PARTITIONDATE and
    # _PARTITIONTIME. Routing through the row-based reader must return their
    # values rather than nulls.
    table = f"validation_pseudo_columns_{uuid.uuid4().hex[:10]}"
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table} (a INT64) PARTITION BY _PARTITIONDATE")
        cursor.execute(f"INSERT INTO {table} (a) VALUES (7)")

    try:
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_options(**{STORAGE_API_DISABLED: "true"})
            cursor.execute(
                f"SELECT a, _PARTITIONDATE AS pd, _PARTITIONTIME AS pt FROM {table}"
            )
            rows = cursor.fetch_arrow_table().to_pylist()

        assert len(rows) == 1
        # INT64 columns need an integer builder on the row-based path.
        assert rows[0]["a"] == 7
        assert rows[0]["pd"] is not None
        assert rows[0]["pt"] is not None
    finally:
        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=table)


@pytest.mark.xfail(
    reason="the row-based reader emits one IPC stream per 1000-row batch, "
    "so the consumer stops at end-of-stream and later rows are dropped"
)
def test_use_storage_api_disabled_client_row_count(driver, conn) -> None:
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_options(**{STORAGE_API_DISABLED: "true"})
        cursor.execute("SELECT x FROM UNNEST(GENERATE_ARRAY(1, 1001)) AS x")
        assert len(cursor.fetch_arrow_table()) == 1001
