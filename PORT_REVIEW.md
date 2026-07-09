# Port review: `dbt-labs/arrow-adbc` → `bigquery-adbc`

Every commit in `dbt-labs/arrow-adbc` (fork) that isn't in
`apache/arrow-adbc:main` and touches `go/adbc/driver/bigquery/`, plus
what happened to it here.

## How I did the port (streamlined recipe)

The two repos have different layouts (`go/adbc/driver/bigquery/*.go` vs
`go/*.go`) and different APIs (context-based methods, split
`bulk_ingest.go` / `util.go` / `connection_statistics.go`), so a direct
`git cherry-pick` doesn't apply. This is what actually worked, distilled
from doing it once:

**1. Enumerate the dbt-only commits.**
```bash
cd ~/sdf/arrow-adbc
git fetch upstream --no-tags     # upstream = apache/arrow-adbc
git log --no-merges --format="%h %an|%aI|%s" \
    origin/main --not upstream/main -- go/adbc/driver/bigquery/
```

**2. Triage each commit into one of four buckets.** For every candidate:

- Grep the target repo for the load-bearing symbol (option constant,
  struct field, feature helper). If present → **skip: already in new**.
- Read the diff. If it patches lines that were rewritten in the new
  driver → **skip: moot**.
- If a later commit in the list replaces it → **skip: superseded**.
- Comment/import/whitespace only → **skip: trivial**.
- Otherwise → **port**.

Grepping the option-string namespace on both sides is usually enough to
decide:

```bash
cd ~/sdf/arrow-adbc/go/adbc/driver/bigquery
grep -rhoE '"adbc\.bigquery[a-z_.]*"' *.go | grep -v _test.go | sort -u > /tmp/legacy_options.txt

cd ~/sdf/bigquery-adbc/go
grep -rhoE '"adbc\.bigquery[a-z_.]*"' *.go | grep -v _test.go | sort -u > /tmp/new_options.txt

comm -23 /tmp/legacy_options.txt /tmp/new_options.txt   # only in legacy → candidates
comm -13 /tmp/legacy_options.txt /tmp/new_options.txt   # only in new    → confirms driver ahead
```

**3. Extract original author metadata for each commit worth porting.**
```bash
cd ~/sdf/arrow-adbc
git show -s --format="%an <%ae>|%aI|%s" <sha>
```

**4. Port one feature at a time on the target branch, in oldest-first
order.** For each commit:

1. Add the feature's slice of code (option constants → struct fields →
   Get/Set handlers → ExecuteQuery branch → new helper file if needed).
2. `go build ./... && go vet ./... && go test ./...`
3. Commit with the original author + date preserved, and reference the
   arrow-adbc SHA/PR in the message body:
   ```bash
   git commit \
     --author="Anna Lee <anna.lee@dbtlabs.com>" \
     --date="2025-10-24T11:45:25-04:00" \
     -m "$(cat <<'EOF'
   feat(go): add option to link failed jobs

   [what/why/how]

   Ports arrow-adbc commit d8b3f2a8b (Add option to link failed jobs, #80).
   EOF
   )"
   ```

**Adaptation cheatsheet** for legacy → new differences:

| Legacy shape | New driver shape |
| --- | --- |
| Fields inline on the legacy `statement` struct | Same fields on the new `statement` struct in `statement.go` |
| Free helpers piled into monolithic `statement.go` | Split into `table_ops.go`, `python_models.go`, `csv_ingest.go`, `row_based_iterator.go` |
| `newDataprocBatchClient` etc. hand-built in `connection.go` | Extract `authOptions(ctx)` on `connectionImpl` first, then reuse it in `python_models.go` |
| `SetOption(key, value)` (no ctx) | `SetOption(ctx, key, value)` (context-based) |
| `Open() (adbc.Connection, error)` | `Open(ctx) (adbc.ConnectionWithContext, error)` |
| Feature turns on a global path in `runQuery` | Signal via `context.WithValue(ctx, ContextKey…, true)` from the statement, then `runQuery` inspects |

**5. Make every commit look like a clean cherry-pick.** After the port,
run a single `filter-branch` that (a) sets committer = author on every
commit, and (b) strips any automation `Co-Authored-By:` trailer:

```bash
cd ~/sdf/bigquery-adbc
FILTER_BRANCH_SQUELCH_WARNING=1 git filter-branch -f \
  --env-filter '
    export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"
    export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
    export GIT_COMMITTER_DATE="$GIT_AUTHOR_DATE"
  ' \
  --msg-filter '
    awk "
      /^Co-Authored-By: Claude/ { skip=1; next }
      skip && NF==0 { skip=0; next }
      { skip=0; print }
    "
  ' \
  <base_sha>..HEAD
```

**6. Verify.**
```bash
cd ~/sdf/bigquery-adbc/go
go build ./... && go vet ./... && go test ./...
go build -tags driverlib -buildmode=c-shared -o /tmp/lib.so ./pkg
```

For live smoke-testing against real BigQuery via fs's driver-loader override:

```bash
cd ~/sdf/bigquery-adbc/go
go build -tags driverlib -buildmode=c-shared -o pkg/libadbc_driver_bigquery.so ./pkg

cd ~/sdf/fs/fs/sa/crates/dbt-xdbc
ADBC_DRIVER_TESTS=1 \
DISABLE_CDN_DRIVER_CACHE=true \
DISABLE_AUTO_DRIVER_REBUILD=true \
ADBC_REPOSITORY=~/sdf/bigquery-adbc/go/pkg \
cargo test statement_execute_bigquery -- --nocapture
```

**7. Handle upstream rebases.** When
`adbc-drivers/bigquery:main` moves forward, fast-forward the fork's main
and rebase the working branch onto it:

```bash
git fetch upstream main
git checkout main
git merge --ff-only upstream/main
git push origin main

git checkout <working-branch>
git rebase main
```

`go.mod` / `go.sum` conflicts are the common case. Don't hand-edit
indirect versions — let `go mod tidy` reconcile:

```bash
git checkout --theirs -- go.mod go.sum
go mod tidy
git add go.mod go.sum
git -c core.editor=true rebase --continue
```

## Legend

- **Port** — a corresponding commit landed on this branch.
- **Skip (already in new)** — the new driver already had the feature
  (upstreamed to Apache and pulled in when this repo forked).
- **Skip (moot)** — the fix targeted code rewritten in the new driver;
  the bug can't happen here.
- **Skip (superseded)** — a later commit in the list makes this one
  redundant.
- **Skip (trivial)** — comment/import/whitespace cleanup with no
  observable behavior change.

## Ported commits (18)

Ordered by original author date (oldest first). Every port preserves the
original author's name/email/date so a `git format-patch` produces a clean
patch attributable to them.

| # | Legacy PR | Original SHA | Author | Original date | Notes |
|---|-----------|--------------|--------|---------------|-------|
| 1 | [#5](https://github.com/dbt-labs/arrow-adbc/pull/5) | `5cebc7d1a` | Craig Squire | 2025-04-19 | Configurable OAuth token endpoint + `TEMPORARY_ACCESS_TOKEN` auth type. Bundles the initial fork's `access_token`/`access_token_endpoint`/`access_token_server_name` fields (originally split across the `#11` adapter interface commit and `#5`). |
| 2 | [#29](https://github.com/dbt-labs/arrow-adbc/pull/29) | `400da64e5` | Mila Page | 2025-06-24 | Preliminary CSV file ingest support (dbt seeds). New file `csv_ingest.go`. Adds `adbc.bigquery.ingest.{csv_filepath,csv_delimiter,csv_schema}` options + `SetOptionBytes` handler for IPC-encoded schema. |
| 3 | [#34](https://github.com/dbt-labs/arrow-adbc/pull/34) | `13711b856` | Xuliang (Harry) Sun | 2025-06-30 | `adbc.bigquery.table.update_columns_description` — JSON `{col: desc}` map, `Table.Update` on the destination table. New file `table_ops.go`. |
| 4 | [#37](https://github.com/dbt-labs/arrow-adbc/pull/37) | `e51c22694` | Xuliang (Harry) Sun | 2025-07-10 | Authorized views support — adds view as `AccessEntry` on source datasets; idempotent. |
| 5 | [#67](https://github.com/dbt-labs/arrow-adbc/pull/67) | `735f692d7` | Lucas Valente | 2025-09-23 | Extra table-metadata keys on `GetTableSchema`. Adds `ViewQuery`, `UseLegacySQL`, `UseStandardSQL`, `Clustering.Fields`, `ExpirationTime`, the full `ExternalDataConfig.*` family (12 keys), `EncryptionConfig.KMSKeyName`, `StreamingBuffer.{EstimatedBytes,EstimatedRows,OldestEntryTime}`, `TableConstraints.PrimaryKey.Columns`, and `ResourceTags`. Introduces the generic `encodeJson[S,E]` helper and refactors the inline `Labels` JSON encoding to use it. Behavior change: `RequirePartitionFilter` is now emitted unconditionally (previously gated) — downstream consumers depend on the key's presence. |
| 6 | [#76](https://github.com/dbt-labs/arrow-adbc/pull/76) | `86e382780` | Lucas Valente | 2025-10-03 | `adbc.bigquery.sql.query.labels` — JSON object of BigQuery job labels. Used by fs adapter for invocation-id / model-name propagation. |
| 7 | [#80](https://github.com/dbt-labs/arrow-adbc/pull/80) | `d8b3f2a8b` | Anna Lee | 2025-10-24 | `adbc.bigquery.sql.query.link_failed_job` — actually wraps `runQuery` errors with a BigQuery web-console URL when set. Threaded through `runPlainQuery`, `queryRecordWithSchemaCallback`, `newRecordReader`. |
| 8 | [#86](https://github.com/dbt-labs/arrow-adbc/pull/86) | `a0ac7f004` | Anna Lee | 2025-11-12 | Publishes `BIGQUERY:query_id` on the Arrow schema metadata using the executing job's ID. `runQuery` sets `query.JobID = job.ID()`; `metadataFromJobStatistics` gains a `jobID` param; `ipcReaderFromArrowIterator`, `makeDryRunReader`, and `ExecuteSchema` all thread it through. |
| 9 | [#94](https://github.com/dbt-labs/arrow-adbc/pull/94) | `fad437ac7` | Zoltan Ersek | 2025-11-24 | Python models via Dataproc (serverless batch + cluster job) + GCS write. New file `python_models.go`. Pulls in `cloud.google.com/go/dataproc/v2`, `cloud.google.com/go/storage`, `gopkg.in/yaml.v3`, `google.golang.org/protobuf/encoding/protojson`. |
| 10 | [#96](https://github.com/dbt-labs/arrow-adbc/pull/96) part 1 | `a79555b0f` | Xuliang (Harry) Sun | 2025-12-08 | `use_storage_api_disabled_client` — full path. `ContextKeyUseStorageApiDisabledClient` propagates the intent into `runQuery`; when set, `runQuery` wraps `bigquery.RowIterator` in a new `RowBasedArrowIterator` (see `row_based_iterator.go`) that batches rows into Arrow record batches so pseudo-columns like `_PARTITIONDATE` return values instead of nulls. |
| 11 | [#96](https://github.com/dbt-labs/arrow-adbc/pull/96) part 2 | `a79555b0f` | Xuliang (Harry) Sun | 2025-12-08 | `adbc.bigquery.copy_table.{source,destination,write_disposition}` — server-side BigQuery `Copier`. Split into its own commit for reviewability. |
| 12 | [#97](https://github.com/dbt-labs/arrow-adbc/pull/97) | `9ebc9900f` | Zoltan Ersek | 2025-12-08 | Python models via Vertex AI Notebooks (bigframes). Adds `notebook_execute_job.*` options and Notebook client creation. Pulls in `cloud.google.com/go/aiplatform`. |
| 13 | [#108](https://github.com/dbt-labs/arrow-adbc/pull/108) | `51466a2dc` | Xuliang (Harry) Sun | 2026-02-11 | `adbc.bigquery.table.update_description` — table-level description update (no schema change). |
| 14 | [#120](https://github.com/dbt-labs/arrow-adbc/pull/120) | `1850d6103` | Mila Page | 2026-03-30 | Backward-compat alias `adbc.bigquery.sql.api_endpoint` → `adbc.bigquery.sql.endpoint`. Lets fs's `dbt-xdbc/src/bigquery.rs` keep using the legacy name unchanged. |
| 15 | [#121](https://github.com/dbt-labs/arrow-adbc/pull/121) | `ace9cd8a1` | Mila Page | 2026-03-31 | `bigQueryFieldTypeFromMetadata` + IPC `BIGQUERY:type` override in `arrowFieldToBigQueryField`. Round-trips DATETIME/NUMERIC/BIGNUMERIC/JSON/GEOGRAPHY/INTERVAL through Arrow. Supersedes `#118`. |
| 16 | [#127](https://github.com/dbt-labs/arrow-adbc/pull/127) | `0c7a2146e` | Lucas Valente | 2026-04-24 | Two-line fix: `BIGQUERY:type` metadata for FloatFieldType emits `FLOAT64` instead of the invalid literal `FLOAT`. |
| 17 | [#128](https://github.com/dbt-labs/arrow-adbc/pull/128) | `e8c77a4c5` | Ragesh Ganeshkumar | 2026-05-11 | `adbc.bigquery.table.update_columns_policy_tags` — JSON `{col: [tag_id]}` map. RECORD columns skipped (BigQuery restriction). Shares code path with `#34`. |
| 18 | [#134](https://github.com/dbt-labs/arrow-adbc/pull/134) | `e7a344229` | Ragesh Ganeshkumar | 2026-06-09 | External-account (Workload Identity Federation) auth. Adds `EXTERNAL_ACCOUNT` auth type + 4 options + `idpTokenSupplier` (Google STS token exchange). Pulls in `resty.dev/v3@v3.0.0-beta.6` and `golang.org/x/oauth2/google/externalaccount`. |

## Skipped commits (22)

| Legacy PR | SHA | Author | Reason | Rationale |
|-----------|-----|--------|--------|-----------|
| [#118](https://github.com/dbt-labs/arrow-adbc/pull/118) | `9e0ce9056` | Mila Page | Superseded | The TZ-less→DATETIME heuristic is already in the new driver's `bulk_ingest.arrowFieldToBigQueryField`. The other half (metadata mapper) is fully subsumed by `#121` which was ported. |
| [#91](https://github.com/dbt-labs/arrow-adbc/pull/91) | `4079e1747` | Jason Lin | Moot | The legacy bug — splitting `""` into `[""]` and passing to `option.WithScopes` — can't happen in the new driver because `impersonate.CredentialsConfig` is only populated when `len(impersonateScopes) > 0`. |
| [#87](https://github.com/dbt-labs/arrow-adbc/pull/87) | `6c4e98d53` | Jason Lin | Not a feature | Merge commit ("Merge with ADBC 21"). |
| [#81](https://github.com/dbt-labs/arrow-adbc/pull/81) | `a2b80040e` | Anna Lee | Already in new | `option.WithQuotaProject(c.quotaProject)` already in `connection.newClient`; `adbc.bigquery.sql.auth.quota_project` option constant present. |
| [#79](https://github.com/dbt-labs/arrow-adbc/pull/79) | `a769e759f` | Lucas Valente | Already in new | `buildField` already sets `metadata["BIGQUERY:type"] = richSqlType`. |
| [#75](https://github.com/dbt-labs/arrow-adbc/pull/75) | `9a400e9b8` | Zoltan Ersek | Already in new | Full `impersonate.CredentialsConfig` path present with target-principal / delegates / scopes / lifetime. |
| [#74](https://github.com/dbt-labs/arrow-adbc/pull/74) | `86b355a0c` | Felipe Oliveira Carvalho | Already in new | `buildField` handles `bigquery.IntervalFieldType → arrow.FixedWidthTypes.MonthDayNanoInterval`. |
| [#73](https://github.com/dbt-labs/arrow-adbc/pull/73) | `36fc1b207` | Zoltan Ersek | Already in new | Earlier version of service-account impersonation, later refined by `#75`. Both superseded upstream. |
| [#71](https://github.com/dbt-labs/arrow-adbc/pull/71) | `4209f8560` | Jason Lin | Moot | New `buildField` hardcodes NUMERIC `Decimal128{38,9}` and BIGNUMERIC `Decimal256{76,38}`. The bug — "precision 0 meant unset" — can't happen. |
| [#62](https://github.com/dbt-labs/arrow-adbc/pull/62) | `857a4c790` | Zoltan Ersek | Moot (baked into `#34` port) | Fixed a bug in the legacy `executeUpdateTableColumnsMetadata`. The port of `#34` copied the fixed form directly. |
| [#60](https://github.com/dbt-labs/arrow-adbc/pull/60) | `9a58ed830` | Mila Page | Trivial | Removes an orphaned comment. |
| [#55](https://github.com/dbt-labs/arrow-adbc/pull/55) | `5a0dfcfc7` | Mila Page | Moot / baked into `#29` port | Legacy switched CSV-ingest schema plumbing to `SetOptionBytes` + IPC. The new CSV-ingest port (`#29`) uses the same shape from the start. |
| [#54](https://github.com/dbt-labs/arrow-adbc/pull/54) | `754f805db` | Xuliang (Harry) Sun | Moot | Legacy fix to timestamp arrow type. New `bulk_ingest.go` timestamp handling is TZ-driven; bug doesn't reproduce. |
| [#49](https://github.com/dbt-labs/arrow-adbc/pull/49) | `55f7e2d90` | Chase Walden | Moot | Legacy fix for empty `emptyArrowIterator` schema. New driver constructs `emptyArrowIterator{iter.Schema}` from the start. |
| [#47](https://github.com/dbt-labs/arrow-adbc/pull/47) | `353412725` | Xuliang (Harry) Sun | Moot | Legacy fix for parsing repeated records with nested fields. `buildField` is a full rewrite in the new driver — the buggy lines don't exist. |
| [#46](https://github.com/dbt-labs/arrow-adbc/pull/46) | `d6da57de8` | Lucas Valente | Already in new | `OptionStringLocation` + `location` field on `databaseImpl` present. |
| [#35](https://github.com/dbt-labs/arrow-adbc/pull/35) | `73382540d` | Xuliang (Harry) Sun | Moot | Reverts a typo introduced in `#34`. The port of `#34` didn't introduce the typo. |
| [#33](https://github.com/dbt-labs/arrow-adbc/pull/33) | `883c5882a` | Xuliang (Harry) Sun | Already in new | `OptionStringQueryDestinationTable` constant + `stringToTable` handling present. |
| [#32](https://github.com/dbt-labs/arrow-adbc/pull/32) | `55619c53a` | Mila Page | Moot / subsumed | Fully subsumed by `arrowFieldToBigQueryField` in `bulk_ingest.go` + the `bigQueryFieldTypeFromMetadata` mapper from `#121`. |
| [#31](https://github.com/dbt-labs/arrow-adbc/pull/31) | `dbe40e187` | Xuliang (Harry) Sun | Already in new | `stringToTable` parses `[[project.]dataset.]table` in the new driver's `driver.go`. |
| [#30](https://github.com/dbt-labs/arrow-adbc/pull/30) | `51b04e896` | Mila Page | Trivial | Import cleanup. |
| [#11](https://github.com/dbt-labs/arrow-adbc/pull/11) | `6a57518fe` | Mila Page | Superseded (infrastructure) | Adapter driver-interface fields for the legacy driverbase. The new driver uses `adbc-drivers/driverbase-go/driverbase`, a full rewrite. The `access_token`/endpoint fields introduced here are captured by the ported `#5` commit. |
| [#2697](https://github.com/dbt-labs/arrow-adbc/pull/2697) | `f1b83eec8` | Felipe Oliveira Carvalho | Already in new | `getTableSchemaWithFilter` already emits `TimePartitioning.*` / `RangePartitioning.*` / `Clustering.Fields` keys. |
| [#1](https://github.com/dbt-labs/arrow-adbc/pull/1) | `3e7abae5f` | Milos Gligoric | Moot | Legacy fix: use row count not schema for empty iterator. New `runQuery` already uses `iter.TotalRows > 0`. |

Totals: **18 ported, 23 skipped**. All TODOs from the previous
iteration (`link_failed_job` error wrapping, `BIGQUERY:query_id`
metadata, `use_storage_api_disabled_client` row-based iterator, and
the `#67` extra-metadata keys the fs adapter depends on) are now part
of the ported set.

## Known caveats

- **Storage-API-disabled client** is created lazily on the first
  pseudo-column query, differing from the legacy driver (eager). If
  startup latency of that path matters, move construction into
  `newClient` after the main client is ready.
- **`rowsToArrowRecordBatch`** currently handles only `DATE` and
  `TIMESTAMP` — enough for the intended pseudo-column use cases.
  Extend when other types come up.
- **Python-models flow** (`#94`, `#97`) is ported verbatim but hasn't
  been re-tested end-to-end here (requires Dataproc / Vertex AI
  environment + a dbt python-models repro). Auth/permission paths share
  code with the BigQuery client so should work; smoke-test in staging
  before rolling out.
