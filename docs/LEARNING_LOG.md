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
