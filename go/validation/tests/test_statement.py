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


QUERY_ID_KEY = b"BIGQUERY:query_id"


def test_query_id_metadata(driver, conn) -> None:
    # The job ID that produced the result set is published on the result
    # schema so callers can correlate a result set with a BigQuery job.
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 AS a")
        first = cursor.fetch_arrow_table().schema.metadata[QUERY_ID_KEY]
        assert first

        cursor.execute("SELECT 1 AS a")
        second = cursor.fetch_arrow_table().schema.metadata[QUERY_ID_KEY]
        assert second

        # Each execution is a distinct job, so it gets a distinct ID.
        assert first != second


def test_query_id_metadata_bound(driver, conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute("SELECT ? AS a", parameters=(1,))
        assert cursor.fetch_arrow_table().schema.metadata[QUERY_ID_KEY]


@pytest.mark.xfail(reason="the dry-run path does not populate the job ID")
def test_query_id_metadata_dry_run(driver, conn) -> None:
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_options(**{"bigquery.query.dry_run": "true"})
        cursor.execute("SELECT 1 AS a")
        assert cursor.fetch_arrow_table().schema.metadata[QUERY_ID_KEY]
