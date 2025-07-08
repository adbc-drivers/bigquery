# Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.

from adbc_drivers_validation.tests.ingest import (
    TestIngest,  # noqa: F401
    generate_tests,
)

from .bigquery import BigQueryQuirks


def pytest_generate_tests(metafunc) -> None:
    return generate_tests(BigQueryQuirks(), metafunc)
