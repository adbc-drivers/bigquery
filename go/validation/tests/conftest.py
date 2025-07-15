# Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.

from pathlib import Path

import adbc_drivers_validation.model
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    manual_test,
    pytest_collection_modifyitems,
)

from .bigquery import BigQueryQuirks


@pytest.fixture(scope="session")
def driver(request) -> adbc_drivers_validation.model.DriverQuirks:
    driver = request.param
    assert driver == "bigquery"
    return BigQueryQuirks()


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    # Assume shared library is in the repo root
    return str(Path(__file__).parent.parent.parent / f"libadbc_driver_{driver.name}.so")
