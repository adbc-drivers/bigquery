# Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.
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
