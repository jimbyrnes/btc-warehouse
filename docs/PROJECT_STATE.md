# Project state

Last updated: 2026-07-24, end of Milestone 1.

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
2. Ten consecutive blocks — extend extractor to a small range, add first
   Parquet flatten, validate multi-block chain linkage with DuckDB.
3. ~25–50 blocks — DuckDB checks formalized as a data-quality gate before
   anything is considered load-ready; expect to see inputs referencing
   transactions outside the loaded window.
4. Snowflake + dbt staging/core models — Snowflake config introduced,
   isolated, still optional up to this point.
5. Airflow 3 standalone DAG + the bounded UTXO derived model.
6. (Deferred, not part of the "first five") Rolling 100–300 block window,
   scheduled incremental ingestion, revisit Snowflake usage/cost at scale.

**No milestone beyond the current one is approved until explicitly
authorized.**

## 3. What Milestone 1 implemented

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

## 4. Verified live-data result

Block **959330** (mainnet, fetched when chain tip was 959430 — 100 blocks
behind tip) was fetched from `https://mempool.space/api` and written to
`data/raw/blocks/0959330/`:

- `tx_count_reported` (from block metadata) and `tx_count_fetched` (rows
  actually written) both equal **5175** — confirmed via
  `scripts/explore_block.py`'s completeness check.
- `pages_fetched`: 208 (207 pages of 25 transactions + 1 terminating page).
- Coinbase transaction identified (`vin[1].is_coinbase = true`), fee
  distribution computed across the 5174 ordinary transactions
  (min 10 sats, avg ~256.9 sats, max 100000 sats, total 1,328,954 sats),
  and one sample transaction's inputs/outputs unnested into rows.
- All output lives under `data/`, which is git-ignored.

## 5. Esplora pagination 404 edge case

Block 959330's transaction count (5175) is an exact multiple of the page
size (25). Esplora's `/block/:hash/txs/:start_index` endpoint returns
**HTTP 404**, not an empty JSON array, once `start_index` reaches the
total transaction count. The first implementation treated this as a fatal
error and crashed on the real block. Fixed by introducing a `NotFoundError`
in `btc_ingest/esplora.py` that is raised (without retry — a 404 here is
not transient) and caught specifically inside `get_block_txs`'s page
fetcher, treated as the end of pagination. Covered by
`test_get_block_txs_treats_404_as_end_of_pagination` in
`tests/test_extract.py`, so this behavior won't silently regress.

## 6. Current commands

```bash
# setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# fetch (idempotent; skips if the block dir is already complete)
python scripts/fetch_block.py                    # tip - 100 (default)
python scripts/fetch_block.py --height 959330     # exact height
python scripts/fetch_block.py --force             # re-fetch anyway

# explore the fetched block(s) with DuckDB
python scripts/explore_block.py

# test
python -m pytest
```

## 7. Next approved activity

A **learning walkthrough** of the already-downloaded block 959330: reading
through `block.json` and `txs.jsonl` together and connecting each field
(height, weight vs size vs vsize, fee, coinbase, vin/vout, scriptpubkey,
confirmations) to a plain-English analogy before touching any more code.

## 8. Milestone status

**Milestone 2 has not been approved.** No ten-block extraction, no
Parquet generation, no range-looping code should be started until that
approval is given explicitly.
