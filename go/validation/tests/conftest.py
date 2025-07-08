# Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.

import os
from pathlib import Path

import adbc_driver_manager.dbapi
import adbc_drivers_validation.model
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    manual_test,
    pytest_collection_modifyitems,
)

from .bigquery import BigQueryQuirks


@pytest.fixture(scope="module")
def driver(request) -> adbc_drivers_validation.model.DriverQuirks:
    driver = request.param
    assert driver == "bigquery"
    return BigQueryQuirks()


@pytest.fixture(scope="module")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    # Assume shared library is in the repo root
    return str(Path(__file__).parent.parent.parent / f"libadbc_driver_{driver.name}.so")


@pytest.fixture(scope="module")
def conn(
    request,
    driver: adbc_drivers_validation.model.DriverQuirks,
    driver_path: str,
) -> adbc_driver_manager.dbapi.Connection:
    db_kwargs = {}
    conn_kwargs = {}

    for k, v in driver.setup.database.items():
        if isinstance(v, adbc_drivers_validation.model.FromEnv):
            if v.env not in os.environ:
                pytest.skip(f"Must set {v.env}")
            db_kwargs[k] = os.environ[v.env]
        else:
            db_kwargs[k] = v

    autocommit = True
    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs=db_kwargs,
        conn_kwargs=conn_kwargs,
        autocommit=autocommit,
    ) as conn:
        make_cursor = conn.cursor

        # Inject the default args here
        def _cursor(*args, **kwargs) -> adbc_driver_manager.dbapi.Cursor:
            return make_cursor(adbc_stmt_kwargs=driver.setup.statement)

        conn.cursor = _cursor
        yield conn
