# Project state

Last updated: 2026-07-31. Milestone 4 is complete and **live-verified**
against a real Snowflake trial account: infrastructure created, the
validated 25-block dataset loaded into RAW (exact reconciliation, zero
duplicate-key violations), and `dbt run`/`dbt test` both executed
successfully against CORE (8/8 models, 26/26 tests). Project is paused
here — see section 7 for the exact resume point (unchanged from before:
still a teaching walkthrough, not implementation).

## 1. Agreed architecture

- **Python** — extraction only (Esplora API client, atomic idempotent
  writes to local files).
- **DuckDB** — local exploration/validation lens, queried directly against
  raw JSON/JSONL and (later) derived Parquet. Not a production warehouse
  and not a second copy of the data.
- **Snowflake** — dbt's target, wired up in Milestone 4 and now
  **live-verified**: `snowflake/setup.sql` (database, warehouse, 3
  schemas, file format, stage, 4 RAW tables) was executed against a real
  trial account, and the validated 25-block dataset was loaded into RAW
  via `btc_ingest/snowflake_loader.py` with an exact row-count and
  natural-key reconciliation. Config stays env-var-driven and isolated;
  no credentials are ever committed.
- **dbt** — staging models (`dbt/models/staging/`) and core models
  (`dbt/models/core/`) are **live-verified**: `dbt run` and `dbt test`
  both executed successfully against the real warehouse (8/8 models,
  26/26 tests), and CORE row counts were independently reconciled
  against RAW and local Parquet. `dbt docs generate`/`serve` has not been
  run yet. The bounded UTXO model is still deferred to Milestone 5.
- **Airflow 3, standalone mode** (SQLite + LocalExecutor) — eventual
  orchestration. No Kubernetes, Celery, Redis, or separate Postgres
  container. Parallelism will later be capped (e.g. parallelism=2, max 2
  active tasks per DAG) to fit the Codespace's ~2 CPU / 8GB budget.
- Esplora API base URL is configurable: mempool.space primary,
  blockstream.info fallback (same response shape, no code branching
  needed).
- Raw ingestion is idempotent and identified by **both** block height and
  block hash — height alone is never assumed to permanently and uniquely
  identify a block. Reorg handling is deliberately deferred by ingesting
  blocks with many confirmations (currently ~100 behind tip).
- Raw data layout: one directory per block height (`data/raw/blocks/<zero
  padded height>/`) containing `block.json`, `txs.jsonl`, `_meta.json`.
  Content is written unmodified from the API (no fields added, removed,
  or transformed); splitting the API's paginated arrays into one JSON
  object per line is a file-serialization choice, not a content change.
- Any future derived UTXO model represents outputs unspent **within the
  loaded block window**, not Bitcoin's global UTXO set, and must be named
  and documented to make that boundary explicit.

## 2. Milestone roadmap

1. **One block** — fetch + explore locally with DuckDB. **Done.**
2. **Ten consecutive blocks** — extend extractor to a small range, first
   Parquet flatten, validate multi-block chain linkage with DuckDB. **Done.**
3. **25 consecutive blocks** — DuckDB checks formalized as a formal
   pass/fail data-quality gate; inputs referencing transactions outside
   the loaded window classified as an expected boundary condition, not a
   failure. **Done.**
4. **Snowflake landing + dbt staging/core models** — RAW landing tables,
   dbt staging/core models, tests, and docs all written; loader refuses
   to run without a passing Milestone 3 validation report. **Done and
   live-verified**: infrastructure created, 25-block dataset loaded
   (100/100 partitions, exact reconciliation, zero duplicate-key
   violations), `dbt run`/`dbt test` both passed (8/8 models, 26/26
   tests), CORE reconciled against RAW and local Parquet, warehouse
   suspended after each session — see section 4 for full results.
5. Airflow 3 standalone DAG + the bounded UTXO derived model.
6. (Deferred, not part of the "first five") Rolling 100–300 block window,
   scheduled incremental ingestion, revisit Snowflake usage/cost at scale.

**No milestone beyond the current one is approved until explicitly
authorized. Milestone 5 has not been approved.**

## 3. Implementation history

### What Milestone 1 implemented

- `btc_ingest/config.py` — API base URL (env-overridable), page size,
  paths, zero-pad width, default depth behind tip (100).
- `btc_ingest/esplora.py` — Esplora client: tip height, height→hash,
  block metadata, paginated block transactions with retry; pagination
  merging factored out as a pure, network-free function.
- `btc_ingest/extract.py` — atomic whole-block write (temp dir under the
  same parent, then `os.replace` into place) so a directory only exists
  under its final name once `block.json`, `txs.jsonl`, and `_meta.json`
  are all fully written; idempotent skip if a block directory is already
  complete.
- `scripts/fetch_block.py` — CLI: `--height`, `--behind-tip`,
  `--api-base-url`, `--force`.
- `scripts/explore_block.py` — DuckDB script reading `block.json` /
  `txs.jsonl` directly via glob (already multi-block-ready, unchanged
  code will work once Milestone 2 adds more blocks).
- `tests/test_extract.py` — 7 tests: pagination termination on a short
  page, pagination termination on an evenly-divisible page count via HTTP
  404, content-fidelity round-trip (no fields added/removed), atomic
  all-or-nothing write on simulated failure, `is_complete` correctness,
  idempotent skip on re-ingest.
- No Airflow, Snowflake, or dbt installed or configured yet.

### What Milestone 2 added

- `btc_ingest/config.py` — added `DATA_PARQUET_DIR`, `DEFAULT_RANGE_COUNT`
  (10), `INTER_BLOCK_DELAY_SECONDS` (politeness pause between blocks in a
  range fetch).
- `btc_ingest/extract.py` — `is_complete()` strengthened to also validate
  `tx_count_fetched == tx_count_reported` from `_meta.json`, not just file
  existence; added `resolve_block_range()` (pure — takes `tip_height` as a
  parameter rather than fetching it, so it's testable without network),
  `ingest_block_range()` / `BlockRangeResult` (sequential, per-block
  atomic/idempotent, one failure can't corrupt another block, records
  fetched/skipped/failed), and `read_raw_block()` to load a previously
  ingested block back into Python objects.
- `btc_ingest/flatten.py` (new) — pure, network-free functions
  (`flatten_block`, `flatten_transactions`, `flatten_inputs`,
  `flatten_outputs`, `vsize_from_weight`) that turn raw JSON into row
  dicts. Fields genuinely absent in the source (coinbase `prevout`,
  addressless OP_RETURN outputs) are passed through as `None`, never
  invented.
- `btc_ingest/parquet_build.py` (new) — `write_parquet_partition_atomic`
  (stages rows as NDJSON, loads through DuckDB's JSON reader for type
  inference, writes to a temp Parquet file, `os.replace`s into place —
  one height's partition never touches another's) and
  `build_block_parquet_partitions` (flattens one block into its four
  dataset partitions).
- `scripts/fetch_blocks.py` (new) — range CLI: `--start-height`/
  `--end-height`, `--start-height`/`--count`, or `--behind-tip`/`--count`
  (default: 10 blocks ending 100 behind tip). Prints fetched/skipped/failed
  and exits non-zero if the requested range isn't fully complete afterward.
- `scripts/build_parquet.py` (new) — builds Parquet partitions for every
  complete raw block on disk, or a specific height/range.
- `scripts/explore_block.py` — extended (same file, same glob pattern,
  which already worked across multiple blocks) with 8 more queries: chain
  linkage across the window, tx count/fees by block, avg/max block weight,
  avg tx size/weight/vsize, input/output count distribution per
  transaction, "foreign" inputs (referencing a transaction outside the
  window), and outputs created-and-spent within the window (explicitly
  labeled as not a UTXO set).
- Tests: `tests/test_range.py`, `tests/test_flatten.py`,
  `tests/test_parquet_build.py` (new), plus one added test in
  `tests/test_extract.py` for the strengthened completeness check.
  28 tests total, all passing.
- Still no Airflow, Snowflake, or dbt.

### What Milestone 3 added

- `btc_ingest/config.py` — added `DATA_REPORTS_DIR` (`data/reports/`,
  under the already git-ignored `data/`).
- `btc_ingest/parquet_build.py` — added `parquet_partitions_complete()`
  so a height's Parquet can be skipped when already up to date (raw data
  is written once and never mutated, so existence is a sufficient
  staleness check).
- `scripts/build_parquet.py` — rewritten to skip up-to-date partitions
  unless `--force`, isolate one height's build failure from the rest
  (try/except per height, continues), report created/skipped/
  missing-raw/failed separately, and exit non-zero if an explicitly
  requested range ends up incomplete.
- `btc_ingest/validate.py` (new) — the formal data-quality gate: 33 SQL
  checks against a DuckDB connection with `blocks`/`transactions`/
  `inputs`/`outputs` registered (real Parquet in production, tiny
  synthetic tables in tests, so no network/files needed for tests).
  Checks span block completeness, chain linkage, transaction/input/
  output integrity, monetary consistency, and size/weight/vsize
  invariants. Each check gets severity PASS / EXPECTED_BOUNDARY / WARN /
  FAIL; only FAIL affects `overall_status`. `run_validation()` assembles
  the full report (requested/observed heights, per-check results,
  boundary metrics with counts+percentages, size/weight/vsize summary
  stats, severity totals, overall status); `write_report_json()` and
  `print_report()` handle output.
- `scripts/validate_dataset.py` (new) — CLI wrapping `run_validation()`
  against real Parquet, prints the report, writes JSON to
  `data/reports/validation_<start>-<end>.json`, exits non-zero only on
  overall FAIL.
- `scripts/summarize_dataset.py` (new) — analytical summary (not
  pass/fail): fees/fee-rate by block, avg/max block weight, avg tx
  size/weight/vsize, input/output totals, % of inputs resolving inside
  the window, % of outputs spent later within the window, output counts
  by script type, addressless-output count, largest transactions by
  weight, highest fee-rate transactions (coinbase excluded).
- Tests: `tests/test_validate.py` (new, 14 tests against a small
  synthetic 3-block fixture), plus one added test in
  `tests/test_parquet_build.py` for `parquet_partitions_complete`.
  43 tests total, all passing.
- Still no Airflow, Snowflake, or dbt.

### What Milestone 4 added

- `requirements.txt` — added `python-dotenv`, `snowflake-connector-python`,
  `dbt-core`, `dbt-snowflake`.
- `.env.example` (new) — the 7 required Snowflake env var names, no
  values. `.env` itself is git-ignored.
- `snowflake/setup.sql` (new) — hand-written, readable DDL (not
  generated from Python): database `BTC_WAREHOUSE`; schemas `RAW`/
  `STAGING`/`CORE`; warehouse `BTC_WAREHOUSE_WH` (XSMALL, auto-suspend
  60s, auto-resume, created suspended); Parquet file format and internal
  stage; four typed `RAW` landing tables matching `btc_ingest/flatten.py`
  exactly, plus `LOADED_AT`/`SOURCE_FILE_NAME` (deliberately no
  `SOURCE_BLOCK_HEIGHT` — `block_height` is already a column on every one
  of these tables).
- `btc_ingest/snowflake_loader.py` (new) — `SnowflakeConfig.from_env()`
  (clear error listing exactly which env vars are missing);
  `load_validation_report()` (the quality-gate enforcement point — raises
  unless a validation report for the exact requested range exists and
  shows `overall_status: PASS`); pure SQL-generating functions
  (`generate_put_sql`, `generate_copy_into_sql`, `generate_merge_sql`)
  producing the PUT -> COPY INTO temp table -> MERGE idempotent-load
  pattern per dataset per height; `load_height_range()` orchestrates all
  of this, isolating one (height, dataset) failure from the rest.
- `scripts/load_snowflake.py` (new) — CLI wrapping the loader; loads
  `.env` via `python-dotenv`, reports loaded/failed, exits non-zero on
  any failure.
- `dbt/` (new project) — `dbt_project.yml`; `profiles.yml.example`
  (env_var-only, safe to commit); `macros/generate_schema_name.sql` (so
  `+schema: staging`/`core` map directly instead of dbt's default
  `<profile_schema>_<custom>` concatenation); `macros/test_nonnegative.sql`
  and `test_positive.sql` (small hand-rolled generic tests, avoiding a
  `dbt_utils` package dependency); `models/sources/sources.yml` (4 RAW
  sources); 4 staging models (thin casts/renames, epoch timestamp ->
  `TIMESTAMP_NTZ`, `SEQUENCE` renamed to `sequence_number` to dodge the
  reserved word); 4 core models (grain-enforcing surrogate keys —
  `block_id`, `input_id`, `output_id` — for the composite natural keys;
  `txid` alone is already transactions' natural key); 5 singular
  cross-model tests mirroring `btc_ingest/validate.py`'s checks
  (block/tx count match, one coinbase per block, internal input
  references resolve uniquely, fee arithmetic, vsize formula) with the
  same bounded-window exception for foreign input references.
- `tests/test_snowflake_loader.py` (new, 15 tests, no network) —
  config/credential errors, SQL-generation correctness for every dataset,
  the validation-report gate (missing report / FAIL status / PASS),
  and `load_height_range` orchestration against a mocked connection:
  correct PUT/COPY/MERGE/DROP call counts, a missing local partition
  recorded as a failure without crashing, and one dataset's simulated
  failure not stopping the other three.
  58 tests total, all passing.
- `.gitignore` — added `profiles.yml` (defensive; the real template is
  `profiles.yml.example`), `dbt/target/`, `dbt/dbt_packages/`, `dbt/logs/`.
- `dbt/models/staging/stg_blocks.sql` — fixed during live verification: a
  quoted-identifier case mismatch against `RAW.BLOCKS`'s `"TIMESTAMP"`
  column. See section 5 for the full story.
- Airflow remains uninstalled.

## 4. Verified live-data results

**Milestone 1** — block **959330** (mainnet, chain tip 959430 at fetch
time, 100 blocks behind tip) was fetched from `https://mempool.space/api`:
`tx_count_reported` and `tx_count_fetched` both **5175** (208 pages),
coinbase transaction identified, fee distribution computed across 5174
ordinary transactions (min 10 sats, avg ~256.9 sats, max 100000 sats,
total 1,328,954 sats). This raw block directory was later deleted (it's
regenerable and git-ignored) once it no longer fit cleanly into the
Milestone 2 range — the verified result stands regardless.

**Milestone 2** — 10 consecutive mainnet blocks, **heights 959744–959753**
(chain tip 959853 at fetch time, range ending 100 behind tip), fetched
from `https://mempool.space/api` and confirmed complete (`tx_count_fetched
== tx_count_reported` for all 10):

- Transactions: **51,907** total, of which **10** are coinbase (exactly
  one per block, as expected).
- Inputs: **77,968** total (77,958 ordinary + 10 coinbase).
- Outputs: **115,732** total.
- Chain linkage: each block's `previousblockhash` matches the prior
  block's hash for all 9 internal links in the window (the first block's
  predecessor is outside the window, as expected — not a break).
- "Foreign" inputs (referencing a transaction not present in the loaded
  window): **30,490** of 77,958 ordinary inputs — about 39%, expected at
  this small window size since most spent outputs were created earlier
  than our 10-block slice.
- Outputs created **and** spent within the window: **47,468** of 115,732
  created — the remainder are not "unspent," they're simply not matched
  by a spending input inside this narrow slice. Explicitly not a UTXO set.
- All four Parquet datasets built: 40 partition files total (4 datasets ×
  10 heights) under `data/parquet/`.
- Both raw data (`data/raw/`) and partitioned Parquet (`data/parquet/`)
  live entirely under `data/`, which `.gitignore` excludes wholesale —
  neither raw JSON/JSONL nor Parquet files are ever committed.

**Milestone 3** — 25 consecutive mainnet blocks, **heights
959744–959768** (chain tip 959905 at fetch time), extending Milestone 2's
window by reusing its 10 already-complete blocks unchanged and fetching
15 new ones (959754–959768):

- Blocks: **25**. Transactions: **123,125**. Inputs: **189,166**.
  Outputs: **277,218**. Coinbase transactions: **25** (exactly one per
  block).
- Ordinary inputs resolving to a transaction inside the window
  ("internal"): **125,933** of 189,141 (66.58%). Referencing a
  transaction outside the window ("foreign", expected boundary):
  **63,208** (33.42%).
- Outputs spent by an input within the window: **125,933** of 277,218
  (45.43%). Outputs with no address decoded (non-standard scripts, e.g.
  OP_RETURN): **87,506** (31.57%).
- **Formal quality gate: 33/33 checks resolved cleanly — 29 PASS, 4
  EXPECTED_BOUNDARY, 0 WARN, 0 FAIL. Overall: PASS.** JSON report at
  `data/reports/validation_959744-959768.json`.
- The two independently-written scripts (`validate_dataset.py`'s
  boundary metrics and `summarize_dataset.py`'s window-resolution
  percentages) agree exactly on the same underlying counts — a useful
  cross-check that both are computing the same thing correctly.

**Milestone 4 — live-verified** against a real Snowflake trial account
(2026-07-31), after an initial code-only pass (2026-07-28) that had no
account available. Full live results:

*Infrastructure (`snowflake/setup.sql`):*
- Database `BTC_WAREHOUSE`; schemas `RAW`, `STAGING`, `CORE`.
- Warehouse `BTC_WAREHOUSE_WH`: **X-Small**, `auto_suspend=60`,
  `auto_resume=true` — confirmed via `SHOW WAREHOUSES`.
- File format `PARQUET_FORMAT`; internal stage `PARQUET_STAGE`.
- Four typed RAW tables: `BLOCKS`, `TRANSACTIONS`, `TRANSACTION_INPUTS`,
  `TRANSACTION_OUTPUTS`.
- Executed via the Snowflake connector's own comment/string-aware
  statement splitter, not hand-rolled parsing — see section 5 for why.

*RAW load (`scripts/load_snowflake.py`, heights 959744–959768):*
- **100/100 (height, dataset) partitions loaded, 0 skipped, 0 failed** —
  ~4m14s elapsed.
- RAW row counts reconcile exactly against local Parquet: **25 blocks,
  123,125 transactions, 189,166 transaction_inputs, 277,218
  transaction_outputs.**
- Natural-key duplicate checks — blocks (`block_height+block_hash`),
  transactions (`txid`), inputs (`txid+input_index`), outputs
  (`txid+output_index`) — **zero violations on all four.**
- Warehouse explicitly suspended after load, confirmed `SUSPENDED`.

*dbt run / dbt test:*
- First `dbt run` attempt failed on `stg_blocks` (quoted-identifier case
  mismatch, see section 5); fixed; full re-run succeeded: **8/8 models
  (4 staging views, 4 core tables), 0 errors, 0 warnings.**
- `dbt test`: **26/26 tests passed, 0 warnings** — all 5 singular
  cross-model tests (block/tx-count match, one-coinbase-per-block,
  internal-input-resolution, fee arithmetic, vsize formula) and every
  generic column test (uniqueness, not-null, relationships, the custom
  `nonnegative`/`positive` tests).
- CORE row counts independently reconciled via direct SQL (not just
  dbt's own log) against both RAW and local Parquet: **25 / 123,125 /
  189,166 / 277,218 — exact match at both layers.**
- Warehouse explicitly suspended again after the dbt session, confirmed
  `SUSPENDED`.

*Not yet done:* `dbt docs generate`/`dbt docs serve` (the documentation
site) has not been run. Everything else in Milestone 4's original scope
has been executed and verified live.

## 5. Edge cases discovered

**Esplora pagination 404 (Milestone 1).** Block 959330's transaction count
(5175) was an exact multiple of the page size (25). Esplora's
`/block/:hash/txs/:start_index` endpoint returns **HTTP 404**, not an
empty JSON array, once `start_index` reaches the total transaction count.
The first implementation treated this as fatal and crashed on the real
block. Fixed with a `NotFoundError` in `btc_ingest/esplora.py` (raised
without retry — a 404 here is not transient) caught specifically inside
`get_block_txs`'s page fetcher and treated as end-of-pagination. Covered
by `test_get_block_txs_treats_404_as_end_of_pagination`.

**DuckDB UNNEST WITH ORDINALITY column reference (Milestone 2).** In the
"outputs spent within window" query, `UNNEST(vout) WITH ORDINALITY AS
o(out, idx)` produces `idx` as its own column, not a field on the `out`
struct — an early draft wrote `out.idx` and DuckDB raised a binder error
("could not find key idx in struct"). Fixed by referencing `idx` directly.
Caught during live-data verification, not by a unit test — the SQL
exploration script isn't part of the automated suite the way the pure
flatten/range functions are.

**Real data anomaly, not a bug (Milestone 3).** Block **959762** has only
**35 transactions**, versus 4,000–6,600 for every other block in the
25-block window — weight 27,059 and size 10,760 bytes, roughly two
orders of magnitude smaller than its neighbors. Verified genuine, not a
fetch defect: the block's own `block.json` reports `tx_count: 35`, and
our `tx_count_fetched` matches it exactly. Real mainnet blocks
occasionally come in this small (e.g. a block found quickly after the
previous one, before the mempool/template had time to fill). All quality
checks passed for this block same as any other — no special-casing was
needed.

**dbt 1.12 test-argument deprecations (Milestone 4).** The first draft of
`dbt/models/core/_core.yml` wrote `relationships` test arguments (`to`,
`field`) directly under the test name, and the custom `nonnegative` test's
`where` argument directly under the test name rather than nested under
`config:`. Both compiled and ran, but `dbt parse` emitted deprecation
warnings (`MissingArgumentsPropertyInGenericTestDeprecation`,
`PropertyMovedToConfigDeprecation`) — this dbt version wants relationship
arguments nested under `arguments:` and test-level `where` filters nested
under `config:`. Fixed in all four occurrences; `dbt parse` now runs with
zero warnings. Caught by actually running `dbt parse` against the real
installed dbt version, not by guessing at syntax.

**Snowflake quoted-identifier case sensitivity — two separate bugs, both
caught during live Milestone 4 verification (2026-07-31).**

1. *Manual `setup.sql` execution.* The first attempt to run
   `snowflake/setup.sql` used a one-off Python script with a naive regex
   to strip `--` SQL comments before splitting statements on `;`.
   Several `COMMENT = '...'` values in the file intentionally contain a
   literal `--` as a text separator (e.g. `'...educational project --
   Milestone 4'`), and the regex corrupted those string literals,
   producing unrelated cascading syntax errors. Fixed by using the
   Snowflake connector's own comment/string-aware statement splitter
   (`snowflake.connector.util_text.split_statements`) instead of
   hand-rolled parsing. No changes were needed to `snowflake/setup.sql`
   itself — the bug was entirely in the throwaway execution script.
2. *dbt model.* `dbt/models/staging/stg_blocks.sql` referenced the
   epoch-timestamp column as `"timestamp"` (lowercase, quoted).
   `snowflake/setup.sql` created it as `"TIMESTAMP"` (uppercase, quoted).
   Snowflake treats quoted identifiers as case-sensitive, so these are
   two different identifiers — `dbt run` failed with `invalid identifier
   '"timestamp"'`. Fixed by matching the exact case used at table-creation
   time. Caught by actually running `dbt run` against the live warehouse
   — `dbt parse` (no live connection) has no way to catch this, since it
   never validates identifiers against real table metadata.

## 6. Current commands

```bash
# setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# fetch a range (idempotent per block; skips already-complete blocks)
python scripts/fetch_blocks.py                                   # 10 blocks ending tip-100
python scripts/fetch_blocks.py --start-height 959744 --end-height 959768
python scripts/fetch_blocks.py --force                           # re-fetch anyway

# generate Parquet (skips a height's partitions if already up to date)
python scripts/build_parquet.py --start-height 959744 --end-height 959768
python scripts/build_parquet.py --start-height 959744 --end-height 959768 --force

# formal data-quality gate (exits non-zero only on overall FAIL)
python scripts/validate_dataset.py --start-height 959744 --end-height 959768

# analytical summary (fees, weight, script types, window-resolution %)
python scripts/summarize_dataset.py --start-height 959744 --end-height 959768

# explore the fetched block(s) with DuckDB (teaching-oriented, ad hoc)
python scripts/explore_block.py

# --- Milestone 4: Snowflake + dbt (all require real credentials/account) ---

cp .env.example .env                          # then fill in real values
set -a && source .env && set +a               # dbt needs these exported into the shell
mkdir -p ~/.dbt && cp dbt/profiles.yml.example ~/.dbt/profiles.yml

# paste snowflake/setup.sql into a Snowflake worksheet and run it once

python scripts/load_snowflake.py --start-height 959744 --end-height 959768

cd dbt
dbt debug   # verifies connection -- requires a live account
dbt parse   # offline-only sanity check, no connection needed
dbt run
dbt test
dbt docs generate && dbt docs serve
cd ..

# suspend the warehouse when done (run in a Snowflake worksheet):
#   ALTER WAREHOUSE BTC_WAREHOUSE_WH SUSPEND;

# test
python -m pytest
```

## 7. Next approved activity — resume point

The **teaching walkthrough** of the loaded block window is still
started, still not finished — this has not changed since Milestone 2,
even though Milestone 3's and now Milestone 4's engineering work
(including its full live Snowflake/dbt verification) have since been
completed:

- **Lesson 1 (block height and chain linkage) has been delivered** —
  explained in plain English with an analogy, connected to the real
  `block.json` fields (`id`, `height`, `previousblockhash`) and the
  corresponding `data/parquet/blocks/` columns (`block_hash`,
  `block_height`, `previous_block_hash`) for blocks 959744–959746 from
  our own dataset, and closed with five comprehension questions.
- **The user's answers to those five questions have still not been given
  or reviewed.** Instead of answering them, the user approved and
  directed Milestone 3's implementation, then Milestone 4's, both of
  which are now done. Comprehension of Lesson 1 remains unconfirmed —
  see `docs/LEARNING_LOG.md`.

**Resume by:** reviewing the user's answers to Lesson 1's five questions
first, whenever they're given. Only after that should the walkthrough
continue to the next topics in order: transaction inputs and outputs;
fee calculation; size, weight, and vsize; and why many inputs reference
transactions outside the loaded window. The window is still 25 blocks
(959744–959768); Milestone 4 added a Snowflake/dbt layer on top of the
same data but didn't change or extend the dataset itself. Do not skip
ahead to those topics before Lesson 1 is actually resolved, and do not
treat Milestone 3 or 4's completion as evidence that any Bitcoin concept
has been learned — both are engineering/warehouse work, not a teaching
activity, and Milestone 4 in particular is about Snowflake/dbt
mechanics, not Bitcoin concepts at all.

## 8. Milestone status

**Milestone 5 has not been approved.** No Airflow installation or
configuration, no bounded UTXO model, no price ingestion, no streaming,
no dashboards, and no further milestone work should be started until
that approval is given explicitly.

Snowflake and dbt (Milestone 4) are now **fully live-verified**:
infrastructure created in a real trial account, the 25-block dataset
loaded into RAW with exact reconciliation, and `dbt run`/`dbt test` both
passed against CORE — see section 4 for the complete results. Airflow
remains completely uninstalled.

The one piece of Milestone 4's original scope not yet exercised live is
`dbt docs generate`/`dbt docs serve` (the documentation site) — not
required for the milestone to be considered complete, and doesn't need
further approval to run since it's read-only against already-verified
models.

**The next activity, whenever resumed, is the paused teaching
walkthrough (section 7) — not further engineering work.** Milestone 4's
live verification is engineering/warehouse work, same as its
implementation was; it does not constitute or substitute for any part of
the teaching walkthrough, and does not require or imply Milestone 5
approval.
