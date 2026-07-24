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
