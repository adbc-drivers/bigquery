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

import json
import uuid

import adbc_drivers_validation.tests.statement as statement_tests

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


def test_script_results(driver, conn) -> None:
    # This should read the first result set
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1; SELECT 'foobar'")
        table = cursor.fetch_arrow_table()
        schema = table.schema
        assert schema.metadata[b"BIGQUERY:Statistics:Query:StatementType"] == b"SCRIPT"

        assert len(table) == 1, repr(table)


def _apply_column_descriptions(conn, table: str, descriptions: dict) -> None:
    # This path is dispatched from ExecuteQuery instead of running SQL, so it
    # goes through the raw statement rather than cursor.execute().
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_options(
            **{
                "bigquery.query.destination_table": table,
                "bigquery.table.update_columns_description": json.dumps(descriptions),
            }
        )
        cursor.adbc_statement.execute_query()


def _column_descriptions(conn, table: str) -> dict:
    return {
        field.name: (field.metadata or {}).get(b"Description", b"").decode()
        for field in conn.adbc_get_table_schema(table)
    }


def test_update_table_columns_description(driver, conn) -> None:
    table = f"validation_column_desc_{uuid.uuid4().hex[:10]}"
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table} (a INT64, b STRING)")

    try:
        _apply_column_descriptions(
            conn, table, {"a": "the a column", "b": "the b column"}
        )
        assert _column_descriptions(conn, table) == {
            "a": "the a column",
            "b": "the b column",
        }
    finally:
        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=table)


def test_update_table_columns_description_partial(driver, conn) -> None:
    # Columns absent from the map keep whatever description they had.
    table = f"validation_column_desc_partial_{uuid.uuid4().hex[:10]}"
    with conn.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {table} "
            "(a INT64 OPTIONS(description='original a'), b STRING)"
        )

    try:
        _apply_column_descriptions(conn, table, {"b": "only b"})
        assert _column_descriptions(conn, table) == {
            "a": "original a",
            "b": "only b",
        }
    finally:
        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=table)


def test_update_table_columns_description_after_ddl(driver, conn) -> None:
    # The update is a blind write (no ETag), so it is not rejected by a stale
    # ETag after a metadata-changing DDL.
    table = f"validation_column_desc_ddl_{uuid.uuid4().hex[:10]}"
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table} (a INT64)")

    try:
        _apply_column_descriptions(conn, table, {"a": "first"})
        with conn.cursor() as cursor:
            cursor.execute(f"ALTER TABLE {table} SET OPTIONS(description='tbl')")
        _apply_column_descriptions(conn, table, {"a": "second"})
        assert _column_descriptions(conn, table) == {"a": "second"}
    finally:
        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=table)
