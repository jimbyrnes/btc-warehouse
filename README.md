# btc-warehouse
Building a data warehouse to boost my lower level BTC knowledge.

## Milestone 1 — fetch and explore one block

Fetches one mainnet block (default: ~100 blocks behind the current chain
tip, deep enough to not worry about chain reorgs yet) and every one of its
transactions from a public Esplora API (mempool.space by default,
blockstream.info as a same-shape fallback), and writes the untouched raw
data to disk so it can be inspected directly with DuckDB.

No Airflow, Snowflake, or dbt yet — just Python + local files + DuckDB.

### Setup

```bash
python3 -m venv .venv && source .venv/bin/activate   # optional but recommended
pip install -r requirements.txt
```

### Fetch a block

```bash
python scripts/fetch_block.py                                  # tip - 100 (default)
python scripts/fetch_block.py --behind-tip 50                   # tip - 50
python scripts/fetch_block.py --height 959330                   # exact height
python scripts/fetch_block.py --api-base-url https://blockstream.info/api
python scripts/fetch_block.py --force                           # re-fetch even if already ingested
```

Re-running with the same height is a no-op (it skips already-complete
blocks) unless `--force` is passed. Output lands at:

```
data/raw/blocks/<height, zero-padded>/
  block.json   raw block metadata, unmodified
  txs.jsonl    one raw transaction object per line, unmodified
  _meta.json   provenance: block height, block hash, API base URL used,
               pages fetched, fetch timestamp
```

The directory only appears under its final name once all three files are
fully written (atomic write via temp-dir-then-rename), so a killed or
failed fetch never leaves behind a directory that looks complete but isn't.

### Explore the fetched block with DuckDB

```bash
python scripts/explore_block.py
```

This queries `block.json` and `txs.jsonl` directly — no import step, no
copy of the data. It prints the block summary, a completeness check
(fetched tx count vs. the block's own reported count), the coinbase
transaction, fee statistics across ordinary transactions, and one sample
transaction's inputs and outputs unnested into rows.

### Run tests

```bash
python -m pytest
```

Covers the extraction behaviors that actually matter: pagination
termination (including a real quirk — Esplora returns HTTP 404, not an
empty page, once you page past the last transaction), that transaction
JSON is written back unmodified (no fields added/removed), that a failed
write leaves no partial block directory behind, and that already-ingested
blocks are skipped.

## Milestone 2 — 10 consecutive blocks, flattened to Parquet

Extends Milestone 1 from one isolated block to a **10-block consecutive
range**, reusing the same single-block ingestion logic. This is where
chain linkage, relational inputs/outputs, and "references to data outside
our window" become visible for the first time. Still no Airflow,
Snowflake, or dbt.

### Fetch a range

```bash
python scripts/fetch_blocks.py                                    # 10 blocks ending tip-100 (default)
python scripts/fetch_blocks.py --count 10 --behind-tip 100
python scripts/fetch_blocks.py --start-height 959744 --end-height 959753
python scripts/fetch_blocks.py --start-height 959744 --count 10
python scripts/fetch_blocks.py --force                            # re-fetch even if complete
```

Sequential, not concurrent — this is a two-CPU Codespace and each block
already involves dozens to hundreds of paginated API requests, so a small
politeness pause is added between blocks. Reuses `ingest_block` from
Milestone 1 per height, so raw output is the same per-block layout
(`block.json`, `txs.jsonl`, `_meta.json`), still atomic and idempotent per
block. One block failing doesn't touch any other block's completed data —
the command prints fetched/skipped/failed and exits non-zero if the
requested range isn't fully complete afterward.

Completeness is now checked using `_meta.json`, not just file existence:
a block only counts as complete if `tx_count_fetched == tx_count_reported`.

### Generate Parquet

```bash
python scripts/build_parquet.py                                   # every complete raw block on disk
python scripts/build_parquet.py --height 959744
python scripts/build_parquet.py --start-height 959744 --end-height 959753
```

Derived and regenerable — safe to delete `data/parquet/` and re-run at any
time; the only source of truth is `data/raw/`. One partition per block
height, one dataset per entity:

```
data/parquet/blocks/block_height=<height>/data.parquet
data/parquet/transactions/block_height=<height>/data.parquet
data/parquet/inputs/block_height=<height>/data.parquet
data/parquet/outputs/block_height=<height>/data.parquet
```

Each partition is written to a temp file and atomically renamed into
place — rebuilding one height never touches another height's partition,
and a failed write leaves the previous partition (if any) intact.

`vsize` is derived as `ceil(weight / 4)`; every other field is either a
direct pass-through of the raw JSON or a straightforward rename (e.g.
`size` → `size_bytes`). Fields genuinely absent in the source (a
coinbase input's `prevout`, an OP_RETURN output's address) are stored as
null — never invented.

### Explore the 10-block window with DuckDB

```bash
python scripts/explore_block.py
```

Same script as Milestone 1 (the glob patterns already worked across
however many blocks are on disk) — now also prints: chain linkage across
the window (does each block's `previousblockhash` match the prior block's
hash?), tx count/fees by block, average/max block weight, average
tx size/weight/vsize, input/output count distribution per transaction,
how many inputs reference a transaction *outside* the loaded window
("foreign" inputs), and how many outputs are both created and spent
*within* the window. That last one is explicitly **not** a UTXO set — see
`docs/PROJECT_STATE.md`.

### Run tests

```bash
python -m pytest
```

28 tests total. Milestone 2 adds range resolution, range-fetch
skip/fetch/partial-failure behavior, the strengthened metadata-based
completeness check, deterministic flattening of block/transaction/input/
output rows (including coinbase handling and the `vsize` formula), atomic
Parquet partition replacement, and a chain-linkage SQL check against a
small fixture.
