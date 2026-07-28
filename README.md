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

## Milestone 3 — 25 blocks, formal data-quality gate

Extends Milestone 2's window from 10 to **25 consecutive blocks**
(959744–959768), reusing all 10 previously-fetched blocks unchanged, and
replaces "eyeball the DuckDB output" with a **formal, repeatable
pass/fail validator** — the gate the dataset has to clear before it would
ever be considered ready to load into Snowflake. Still no Airflow,
Snowflake, or dbt.

### Fetch the wider range (reuses existing blocks automatically)

```bash
python scripts/fetch_blocks.py --start-height 959744 --end-height 959768
```

### Build any missing Parquet partitions (skips ones already up to date)

```bash
python scripts/build_parquet.py --start-height 959744 --end-height 959768
python scripts/build_parquet.py --start-height 959744 --end-height 959768 --force   # rebuild anyway
```

`build_parquet.py` now skips a height's partitions if all four dataset
files already exist (raw data is written once and never mutated, so
existence is a sufficient staleness check) unless `--force` is passed. A
failure on one height doesn't stop the others, and the command exits
non-zero if an explicitly requested range ends up incomplete.

### Run the formal quality gate

```bash
python scripts/validate_dataset.py --start-height 959744 --end-height 959768
```

Runs 33 checks against the Parquet datasets via DuckDB, covering block
completeness, chain linkage, transaction/input/output integrity, monetary
consistency (`fee_sats = Σinput values − Σoutput values`), and
size/weight/vsize invariants. Each check gets one of four severities:

- **PASS** — invariant holds.
- **EXPECTED_BOUNDARY** — not a defect, a necessary consequence of only a
  bounded slice of the chain being loaded (foreign input references, the
  first block's unverifiable predecessor, outputs not observed spent
  within the window, outputs without a standard address). These never
  fail the gate.
- **WARN** — unusual but not proof of corruption.
- **FAIL** — a real internal inconsistency. Only this fails the gate.

Prints readable terminal output and writes a machine-readable JSON report
to `data/reports/validation_<start>-<end>.json` (under the already
git-ignored `data/`). Exit code is non-zero only on overall FAIL.

### Run the analytical summary

```bash
python scripts/summarize_dataset.py --start-height 959744 --end-height 959768
```

Fees and fee rate by block, average/max block weight, average tx
size/weight/vsize, input/output totals, the percentage of inputs that
resolve inside the window vs. reference something outside it, the
percentage of outputs spent later within the window, output counts by
script type, outputs without a decoded address, and the largest/highest
fee-rate transactions (`fee_rate_sats_per_vbyte = fee_sats / vsize`,
coinbase excluded from the ranking).

### Run tests

```bash
python -m pytest
```

57 tests total. Milestone 3 adds the `parquet_partitions_complete` skip
check and 14 validator tests (using small synthetic DuckDB fixtures, no
live API or real Parquet files needed) covering: a complete range
passing cleanly, missing-height/duplicate-height/chain-link/tx-count/
coinbase-count/fee-mismatch/unresolved-reference failures, correct fee
arithmetic and vsize validation, foreign references and addressless
outputs correctly classified as expected boundaries (not failures), JSON
report generation, and overall-status/exit-code semantics.
