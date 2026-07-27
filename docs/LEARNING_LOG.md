# Learning log

This log records what was actually built, run, and observed — not
Bitcoin concepts, unless they were specifically walked through and
explained. Entries should stay honest about what's been covered so far
versus what's still ahead.

## 2026-07-24 — Milestone 1: fetch and explore one block

**What was demonstrated technically:**

- A single mainnet block (959330, ~100 blocks behind tip at fetch time)
  and all 5,175 of its transactions were fetched from the mempool.space
  Esplora API and written to disk as raw, unmodified JSON/JSONL.
- The fetch is idempotent (re-running against an already-complete block
  directory is a no-op) and atomic (a killed or failed fetch cannot leave
  behind a directory that looks complete but isn't).
- A real API edge case was hit and fixed against live data: Esplora
  returns HTTP 404, not an empty array, once transaction pagination runs
  past the last page — this block's transaction count happened to be an
  exact multiple of the page size, which is what surfaced it. A
  regression test now locks in the correct behavior.
- The fetched block was queried directly with DuckDB (no import/copy
  step): block summary, a fetched-vs-reported completeness check, the
  coinbase transaction, fee statistics across ordinary transactions, and
  one transaction's inputs/outputs unnested into rows.
- 7 unit tests pass, covering pagination termination (both the normal
  short-page case and the 404 edge case), content fidelity, atomic-write
  failure handling, and idempotent skip behavior.

**What has NOT been covered yet (do not assume this is understood):**

No Bitcoin-concept walkthrough has happened yet — height, size/weight/
vsize, fees, coinbase transactions, scripts, confirmations, and UTXOs
have appeared in output and code, but have not been explained one at a
time with analogies. That walkthrough is the next approved activity
(see `docs/PROJECT_STATE.md`, section 7), not something to treat as
already learned.

## 2026-07-26 — Milestone 2: 10 consecutive blocks, flattened to Parquet

**What was demonstrated technically:**

- Extended from one block to a 10-block consecutive range (heights
  959744–959753), reusing the Milestone 1 single-block ingestion logic
  rather than duplicating it. Sequential fetch, one small politeness
  pause between blocks, one failure recorded without corrupting any
  other block's already-completed data.
- Strengthened the idempotency check: a block now only counts as
  "complete" if `_meta.json` confirms `tx_count_fetched ==
  tx_count_reported`, not merely that three files exist.
- Flattened raw JSON into four row shapes (block, transaction, input,
  output) via pure, network-free functions, then wrote them as Parquet
  partitioned by block height (one dataset per entity, one partition per
  height), atomically per partition.
- Ran DuckDB queries across the full 10-block window and got concrete
  numbers: 51,907 transactions, 77,968 inputs, 115,732 outputs, 10
  coinbase transactions (one per block), all 9 internal chain links
  verified (`previousblockhash` matches the prior block's hash), 30,490
  of 77,958 ordinary inputs reference a transaction outside the loaded
  window ("foreign" inputs), and 47,468 of 115,732 outputs are both
  created and spent within the window.
- Two real bugs were hit and fixed against live data: Esplora's 404-as-
  end-of-pagination (Milestone 1, re-confirmed here) and a DuckDB
  `UNNEST ... WITH ORDINALITY` column-reference mistake (`out.idx` instead
  of `idx`) caught while running the exploration script against the real
  10-block dataset.
- 28 automated tests pass, covering range resolution, range-fetch
  skip/fetch/partial-failure behavior, the strengthened completeness
  check, deterministic row flattening (including coinbase handling and
  the `vsize = ceil(weight/4)` formula), atomic Parquet partition
  replacement, and a chain-linkage SQL pattern against a small fixture.

**What has NOT been covered yet (do not assume this is understood):**

Same caveat as Milestone 1, now more pointed: the pipeline can now
*show* chain linkage, foreign inputs, and in-window spends as numbers and
tables, but none of these have been walked through conceptually yet —
why a block links to its predecessor, what an input/output actually
represents economically, how fee = Σinputs − Σoutputs plays out with
real numbers, what distinguishes size/weight/vsize, or why so many
inputs point outside a 10-block window. That walkthrough is the next
approved activity (see `docs/PROJECT_STATE.md`, section 7) and has not
happened yet.
