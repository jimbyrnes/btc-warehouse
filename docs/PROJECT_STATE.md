# Project state

Last updated: 2026-07-26, end of Milestone 2.

## 1. Agreed architecture

- **Python** — extraction only (Esplora API client, atomic idempotent
  writes to local files).
- **DuckDB** — local exploration/validation lens, queried directly against
  raw JSON/JSONL and (later) derived Parquet. Not a production warehouse
  and not a second copy of the data.
- **Snowflake** — eventual dbt target. Kept isolated; no credentials
  required until the milestone that needs it (Milestone 4, not yet
  approved).
- **dbt** — eventual staging/core models, tests, docs, lineage, and the
  bounded UTXO model.
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
3. ~25–50 blocks — DuckDB checks formalized as a data-quality gate before
   anything is considered load-ready; expect to see inputs referencing
   transactions outside the loaded window (already observed in Milestone 2
   at 10 blocks — see section 4 below).
4. Snowflake + dbt staging/core models — Snowflake config introduced,
   isolated, still optional up to this point.
5. Airflow 3 standalone DAG + the bounded UTXO derived model.
6. (Deferred, not part of the "first five") Rolling 100–300 block window,
   scheduled incremental ingestion, revisit Snowflake usage/cost at scale.

**No milestone beyond the current one is approved until explicitly
authorized. Milestone 3 has not been approved.**

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
- All output lives under `data/`, which is git-ignored.

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

## 6. Current commands

```bash
# setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# fetch a range (idempotent per block; skips already-complete blocks)
python scripts/fetch_blocks.py                                   # 10 blocks ending tip-100
python scripts/fetch_blocks.py --start-height 959744 --end-height 959753
python scripts/fetch_blocks.py --force                           # re-fetch anyway

# generate Parquet (derived, regenerable, safe to delete and rebuild)
python scripts/build_parquet.py

# explore the fetched block(s) with DuckDB
python scripts/explore_block.py

# test
python -m pytest
```

## 7. Next approved activity

A **learning walkthrough** of the downloaded 10-block window
(959744–959753), covering: block height and chain linkage; transaction
inputs and outputs; fee calculation; size, weight, and vsize; and why many
inputs reference transactions outside the loaded window. This walkthrough
has been proposed but not yet delivered — see `docs/LEARNING_LOG.md` for
what's actually been covered so far (still just Milestone 1's technical
mechanics, not concept explanations).

## 8. Milestone status

**Milestone 3 has not been approved.** No 25–50 block extraction, no
data-quality-gate formalization, no Snowflake/dbt/Airflow work should be
started until that approval is given explicitly.
