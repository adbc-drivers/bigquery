# Copyright (c) 2025-2026 ADBC Drivers Contributors
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

import collections
import functools
from pathlib import Path

from adbc_drivers_validation import model


class BigQueryQuirks(model.DriverQuirks):
    name = "bigquery"
    driver = "adbc_driver_bigquery"
    driver_name = "ADBC Driver Foundry Driver for BigQuery"
    vendor_name = "BigQuery"
    # BigQuery doesn't really have a public facing version, so use the client
    # version instead
    vendor_version = "cloud.google.com/go/bigquery v1.73.1"
    short_version = "1.73.1"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        # TODO(lidavidm): this is a bit weird; it does work, but we'd need two
        # GCP projects to test it.
        connection_set_current_catalog=False,
        connection_set_current_schema=True,
        connection_transactions=True,
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_schema=True,
        # N.B. while technically supported, this is only inside "multi
        # statement" queries which is not very useful to us
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        statement_rows_affected_ddl=True,
        current_catalog=model.FromEnv("GOOGLE_CLOUD_PROJECT"),
        current_schema=model.FromEnv("BIGQUERY_DATASET_ID"),
        secondary_schema=model.FromEnv("BIGQUERY_SECONDARY_DATASET_ID"),
        supported_xdbc_fields=[
            "xdbc_data_type",
            "xdbc_type_name",
            "xdbc_nullable",
            "xdbc_sql_data_type",
            "xdbc_decimal_digits",
            "xdbc_column_size",
            "xdbc_char_octet_length",
            "xdbc_scope_catalog",
            "xdbc_scope_schema",
            "xdbc_scope_table",
        ],
    )
    setup = model.DriverSetup(
        database={
            "adbc.bigquery.sql.project_id": model.FromEnv("GOOGLE_CLOUD_PROJECT"),
            "adbc.bigquery.sql.dataset_id": model.FromEnv("BIGQUERY_DATASET_ID"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    @functools.cached_property
    def query_set(self) -> model.QuerySet:
        queries = super().query_set.queries.copy()

        ingest = collections.defaultdict(list)
        for name, query in queries.items():
            if not isinstance(query.query, model.IngestQuery):
                continue
            ingest[query.arrow_type_name].append(query)

        def _tests_storage_write(q):
            if (setup := q.metadata().setup.statement) is None:
                return False
            method = setup.options.get("bigquery.bulk_ingest.method")
            return method is not None and method.apply == "storage_write"

        for name, type_queries in ingest.items():
            if any(_tests_storage_write(q) for q in type_queries):
                continue

            for q in type_queries:
                name = f"{q.name}:storagewrite"
                metadata = [
                    {
                        "setup": {
                            "statement": {
                                "options": {
                                    "bigquery.bulk_ingest.method": {
                                        "apply": "storage_write",
                                        "revert": "load",
                                    }
                                }
                            }
                        },
                        "tags": {
                            "broken-vendor": None,
                        },
                    }
                ]
                metadata.extend(q.metadata_paths or [])
                queries[name] = model.Query(
                    name=name,
                    query=q.query,
                    metadata_paths=metadata,
                )

        return model.QuerySet(queries=queries)

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        return "Not found: Table" in str(error) and table_name in str(error)

    def quote_one_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"

    @property
    def sample_ddl_constraints(self) -> list[str]:
        return [
            "CREATE TABLE constraint_primary (z INT, a INT, b STRING, PRIMARY KEY (a) NOT ENFORCED)",
            "CREATE TABLE constraint_primary_multi (z INT, a INT, b STRING, PRIMARY KEY (b, a) NOT ENFORCED)",
            "CREATE TABLE constraint_primary_multi2 (z INT, a STRING, b INT, PRIMARY KEY (a, b) NOT ENFORCED)",
            "CREATE TABLE constraint_foreign (z INT, a INT, b INT, FOREIGN KEY (b) REFERENCES constraint_primary(a) NOT ENFORCED)",
            "CREATE TABLE constraint_foreign_multi (z INT, a INT, b INT, c STRING, FOREIGN KEY (c, b) REFERENCES constraint_primary_multi2(a, b) NOT ENFORCED)",
            # Ensure the driver doesn't misinterpret column IDs as indices
            "ALTER TABLE constraint_primary DROP COLUMN z",
            "ALTER TABLE constraint_primary_multi DROP COLUMN z",
            "ALTER TABLE constraint_primary_multi2 DROP COLUMN z",
            "ALTER TABLE constraint_foreign DROP COLUMN z",
            "ALTER TABLE constraint_foreign_multi DROP COLUMN z",
        ]


QUIRKS = [BigQueryQuirks()]
