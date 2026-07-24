#!/usr/bin/env python3
"""Milestone 1 DuckDB exploration: query raw block/tx files directly.

No copying, no separate database of record -- DuckDB reads block.json and
txs.jsonl in place under data/raw/blocks/. The glob patterns already work
across multiple block directories, so this keeps working unchanged once
later milestones ingest more than one block.

Usage:
    python scripts/explore_block.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

from btc_ingest.config import DATA_RAW_BLOCKS_DIR

BLOCK_GLOB = f"{DATA_RAW_BLOCKS_DIR}/*/block.json"
TXS_GLOB = f"{DATA_RAW_BLOCKS_DIR}/*/txs.jsonl"


def run() -> None:
    con = duckdb.connect()  # in-memory: DuckDB is a lens, not a copy of the data
    con.execute(f"CREATE VIEW blocks AS SELECT * FROM read_json_auto('{BLOCK_GLOB}')")
    con.execute(f"CREATE VIEW transactions AS SELECT * FROM read_ndjson_auto('{TXS_GLOB}')")

    print("=== Block summary ===")
    con.sql(
        """
        SELECT height, id AS block_hash, tx_count, size, weight,
               to_timestamp(timestamp) AS block_time
        FROM blocks
        ORDER BY height
        """
    ).show()

    print("=== Fetched tx count vs. block's reported tx_count (completeness check) ===")
    con.sql(
        """
        SELECT b.height, b.tx_count AS reported_tx_count, COUNT(t.txid) AS fetched_tx_count
        FROM blocks b
        LEFT JOIN transactions t ON t.status.block_hash = b.id
        GROUP BY b.height, b.tx_count
        ORDER BY b.height
        """
    ).show()

    print("=== Coinbase transaction (the block reward + fees payout) ===")
    con.sql(
        """
        SELECT txid, fee, len(vin) AS num_inputs, len(vout) AS num_outputs,
               vout[1].value AS first_output_value_sats
        FROM transactions
        WHERE vin[1].is_coinbase = true
        """
    ).show()

    print("=== Fee distribution across ordinary (non-coinbase) transactions ===")
    con.sql(
        """
        SELECT count(*) AS num_txs,
               min(fee) AS min_fee_sats,
               round(avg(fee), 1) AS avg_fee_sats,
               max(fee) AS max_fee_sats,
               sum(fee) AS total_fees_sats
        FROM transactions
        WHERE NOT vin[1].is_coinbase
        """
    ).show()

    print("=== One ordinary transaction's inputs and outputs, unnested ===")
    sample_txid = con.sql(
        "SELECT txid FROM transactions WHERE NOT vin[1].is_coinbase AND len(vin) > 1 LIMIT 1"
    ).fetchone()[0]
    print(f"Sample txid: {sample_txid}")

    print("-- inputs (what this tx spends, by prevout) --")
    con.sql(
        f"""
        SELECT idx, inp.prevout.value AS value_sats, inp.prevout.scriptpubkey_address AS address
        FROM transactions, UNNEST(vin) WITH ORDINALITY AS t(inp, idx)
        WHERE txid = '{sample_txid}'
        ORDER BY idx
        """
    ).show()

    print("-- outputs (what this tx creates -- future UTXO candidates) --")
    con.sql(
        f"""
        SELECT idx, out.value AS value_sats, out.scriptpubkey_address AS address
        FROM transactions, UNNEST(vout) WITH ORDINALITY AS t(out, idx)
        WHERE txid = '{sample_txid}'
        ORDER BY idx
        """
    ).show()


if __name__ == "__main__":
    run()
