# btc-warehouse — project instructions for Claude Code

## What this is

An educational Bitcoin data-engineering project — not primarily a dashboard
project. The dashboard, if one ever exists, is a side effect. The actual
goal is understanding Bitcoin's data model well enough to explain it.

**Primary learning objective:** understand blocks, transactions, inputs,
outputs, fees, UTXOs, block height, size, weight, vsize, confirmations,
coinbase transactions, scripts, and chain linkage well enough to explain
each with a plain-English analogy — not just to get a pipeline running.

## Teaching approach (apply every milestone)

- Before introducing a Bitcoin concept, explain it in plain English with a
  concrete analogy.
- Then connect it to where it actually appears: raw JSON, Python, DuckDB,
  SQL, and eventually the warehouse model.
- Do not hide important implementation details behind generated
  automation — the point is to see the mechanics, not abstract them away.
- Work one milestone at a time. Do not begin a later milestone without
  explicit approval, even if the next step seems obvious.
- After each implementation milestone, stop for a learning walkthrough
  before proceeding to the next one.

## Architecture (target end state)

- **Python** — extraction only.
- **DuckDB** — local exploration/validation lens over raw JSON and derived
  Parquet. Not the production warehouse, never a second copy of truth.
- **Snowflake** — dbt's warehouse target. Wired up and **live-verified**
  as of Milestone 4 (`snowflake/setup.sql`, `btc_ingest/snowflake_loader.py`)
  against a real trial account — see `docs/PROJECT_STATE.md` for results.
  Config stays env-var-driven and isolated; no credentials are ever
  committed.
- **dbt** — staging/core models, tests, and docs exist and are
  **live-verified** as of Milestone 4 (`dbt/`) — `dbt run`/`dbt test`
  both passed against the real warehouse. The bounded UTXO model is
  still deferred to Milestone 5.
- **Airflow 3, standalone mode** — eventual orchestration of bounded batch
  jobs. SQLite + LocalExecutor only.

## Hard constraints

- No Kubernetes, Kafka, Celery, Redis, or a full/pruned Bitcoin node.
- Esplora API base URL must stay configurable: mempool.space is primary,
  blockstream.info is the documented fallback (same response shape).
- Raw ingestion is organized and made idempotent by **both** block height
  and block hash — height alone must never be assumed to uniquely and
  permanently identify a block.
- The Codespace has ~2 CPUs, 8GB RAM, and limited disk. Keep
  implementations lightweight; this is why Airflow runs standalone rather
  than the full docker-compose stack, and why Airflow parallelism must
  later be explicitly capped (e.g. parallelism=2, max 2 active tasks/DAG)
  rather than left at defaults.
- Any derived UTXO model represents outputs unspent **within the loaded
  observation boundary**, not Bitcoin's complete global UTXO set. Name and
  document such models accordingly — never imply full-chain UTXO coverage.

## Where things stand

See `docs/PROJECT_STATE.md` for the current milestone, architecture
decisions made so far, and the next approved activity. See
`docs/LEARNING_LOG.md` for what's actually been demonstrated/learned to
date — don't assume concepts were covered unless they're logged there.
