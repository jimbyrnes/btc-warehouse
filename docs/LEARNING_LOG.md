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

## 2026-07-27 — Lesson 1 delivered: block height and chain linkage

**What was taught (not yet confirmed learned):**

- Block height, block hash, and chain tip explained via a "numbered,
  photocopied ledger page" analogy, including where that analogy breaks
  down (a ledger page doesn't cryptographically staple itself to the
  previous page's fingerprint the way `previousblockhash` does).
- Walked through the real raw `block.json` for blocks 959744 and 959745
  from our own fetched data, showing `id`/`previousblockhash` matching
  hash-for-hash across consecutive blocks, and the same fields renamed
  (`block_hash`/`previous_block_hash`) in `data/parquet/blocks/`.
- Explained why height alone doesn't uniquely/permanently identify a
  block (possible temporary forks at the same height), why we fetch
  ~100 blocks behind tip to sidestep reorg handling, and why block
  959744's own predecessor (959743) can't be verified from our loaded
  window — a first concrete example of the observation-boundary idea.
- Closed with five comprehension questions, not yet answered.

**Explicitly NOT true yet — do not assume this in future sessions:**

This entry records that chain linkage *was explained*, using real data.
It does **not** record that the user has demonstrated understanding of
it. The five questions posed at the end of Lesson 1 have not been
answered or reviewed. Until that happens, treat block height / chain
linkage as *taught, comprehension unconfirmed* — not as a settled,
already-learned concept. All other topics (inputs, outputs, fees, size/
weight/vsize, foreign-window inputs, UTXOs) remain completely untaught.

## 2026-07-28 — Milestone 3: 25 blocks, formal data-quality gate

**What was demonstrated technically (engineering, not a teaching
session — no Bitcoin concepts were explained in this entry's work):**

- Extended the loaded window from 10 to 25 consecutive blocks
  (959744–959768), reusing all 10 of Milestone 2's blocks unchanged and
  fetching 15 new ones — real, working reuse, not just "if they happen
  to overlap."
- `build_parquet.py` now skips a height's Parquet partitions when
  already up to date (existence-based staleness check, since raw data is
  write-once) unless `--force`, isolates one height's build failure from
  the rest, and exits non-zero if an explicitly requested range ends up
  incomplete.
- Built a formal, repeatable data-quality gate (`validate_dataset.py`,
  33 SQL checks via DuckDB) with four severities — PASS,
  EXPECTED_BOUNDARY, WARN, FAIL — where only FAIL affects the overall
  result. Ran it against the real 25-block dataset: **29 PASS, 4
  EXPECTED_BOUNDARY, 0 WARN, 0 FAIL, overall PASS.** JSON report written
  to `data/reports/`.
- Built a separate analytical summary (`summarize_dataset.py`) and found
  that it and the validator, computed independently, agree exactly on
  the same underlying counts (outputs spent within the window: 125,933
  from both) — a real cross-check that both are correct, not just that
  each is internally consistent with itself.
- Got concrete numbers at 25 blocks: 123,125 transactions, 189,166
  inputs, 277,218 outputs, 25 coinbase transactions (one per block).
  66.58% of ordinary inputs resolve to a transaction inside the window;
  33.42% reference something outside it (expected, not a defect).
  45.43% of outputs are spent within the window; 31.57% of outputs have
  no decoded address (non-standard scripts).
- Found a genuine data anomaly, not a bug: block 959762 has only 35
  transactions versus 4,000+ for its neighbors — verified real (the
  block's own metadata reports the same count), not a fetch defect.
- 14 new validator tests, all against small synthetic fixtures — no live
  API or real Parquet files required for the test suite. 57 tests total,
  all passing.

**What has NOT been covered (unchanged from before, do not assume
otherwise):**

None of this session's work was a teaching activity. The teaching
walkthrough is still exactly where it was at the end of the last entry:
Lesson 1 (block height and chain linkage) delivered, its five
comprehension questions still unanswered. All other Bitcoin concepts —
inputs, outputs, fees, size/weight/vsize, foreign-window inputs, the
bounded-UTXO idea — remain completely untaught, regardless of how much
validation and analytical tooling now exists around that data.
