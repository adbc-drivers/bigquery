# Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.
import argparse
from pathlib import Path

import adbc_drivers_validation.generate_documentation as generate_documentation

from .bigquery import BigQueryQuirks

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    generate_documentation.generate(
        BigQueryQuirks(),
        Path("validation-report.xml").resolve(),
        args.output.resolve(),
    )
