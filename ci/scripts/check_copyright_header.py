#!/usr/bin/env python3
# Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.

import datetime
import fnmatch
import os
import re
import sys

ignore_patterns = [
    "*go.sum",
    "*LICENSE*",
    "*NOTICE*",
    "*pixi.lock",
    "*.json",
    "bigquery/pkg/adbc.h",
    "bigquery/pkg/driver.go",
    "bigquery/pkg/utils.c",
    "bigquery/pkg/utils.h",
    "validation/queries/*/*.json",
    "validation/queries/*/*.sql",
]


def main():
    year = datetime.datetime.now().year
    header = re.compile(
        rf"Copyright \(c\) ([0-9]+-)?{year} Columnar Technologies, Inc\. +All rights reserved\."
    )

    exitcode = 0
    for path in sys.argv[1:]:
        if any(fnmatch.fnmatch(path, pattern) for pattern in ignore_patterns):
            continue

        if os.path.getsize(path) == 0:
            continue

        with open(path, "r") as f:
            lineno = 0
            found = False

            try:
                for line in f:
                    if header.search(line):
                        found = True
                        break
                    lineno += 1
                    if lineno >= 2:
                        break
            except UnicodeDecodeError:
                print(f"Cannot read {path} as text, skipping")
                exitcode = 1

            if not found:
                print(f"Missing copyright header in {path}")
                exitcode = 1
    return exitcode


if __name__ == "__main__":
    sys.exit(main())
