#!/usr/bin/env python3
"""DuckDB exploration: query raw block/tx files directly.

No copying, no separate database of record -- DuckDB reads block.json and
txs.jsonl in place under data/raw/blocks/. The glob patterns work across
however many block directories are currently on disk, from one block
(Milestone 1) up through the current 10-block window (Milestone 2) and
beyond, unchanged.

The queries in the second half of this script (chain linkage, foreign
inputs, in-window spends) only become meaningful once more than one block
is loaded -- with a single block, "previous_block_hash" and "foreign
input" checks are trivially degenerate.

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

    print("=== Chain linkage: does each block's previous_block_hash match the prior block's hash? ===")
    con.sql(
        """
        SELECT height, id AS block_hash, previousblockhash,
               lag(id) OVER (ORDER BY height) AS prior_block_hash_in_window,
               previousblockhash = lag(id) OVER (ORDER BY height) AS linked_to_prior
        FROM blocks
        ORDER BY height
        """
    ).show()
    print("(NULL/false for the first block just means its predecessor isn't loaded -- not a break.)")

    print("=== Transaction count, total and average fee, by block ===")
    con.sql(
        """
        SELECT status.block_height AS height,
               count(*) AS tx_count,
               count(*) FILTER (WHERE NOT vin[1].is_coinbase) AS ordinary_tx_count,
               sum(fee) FILTER (WHERE NOT vin[1].is_coinbase) AS total_fees_sats,
               round(avg(fee) FILTER (WHERE NOT vin[1].is_coinbase), 1) AS avg_fee_sats
        FROM transactions
        GROUP BY status.block_height
        ORDER BY height
        """
    ).show()

    print("=== Average and maximum block weight across the loaded window ===")
    con.sql(
        """
        SELECT round(avg(weight), 1) AS avg_weight_units, max(weight) AS max_weight_units,
               round(avg(size), 1) AS avg_size_bytes, max(size) AS max_size_bytes
        FROM blocks
        """
    ).show()

    print("=== Average transaction size, weight, and vsize across the loaded window ===")
    con.sql(
        """
        SELECT round(avg(size), 1) AS avg_size_bytes,
               round(avg(weight), 1) AS avg_weight_units,
               round(avg(ceil(weight / 4.0)), 1) AS avg_vsize
        FROM transactions
        """
    ).show()

    print("=== Input/output count distribution per transaction (ordinary transactions only) ===")
    con.sql(
        """
        SELECT min(len(vin)) AS min_inputs, round(avg(len(vin)), 2) AS avg_inputs, max(len(vin)) AS max_inputs,
               min(len(vout)) AS min_outputs, round(avg(len(vout)), 2) AS avg_outputs, max(len(vout)) AS max_outputs
        FROM transactions
        WHERE NOT vin[1].is_coinbase
        """
    ).show()

    print("=== Inputs whose referenced previous transaction is NOT in our loaded window ('foreign' inputs) ===")
    con.sql(
        """
        WITH all_inputs AS (
            SELECT t.txid, inp.txid AS previous_txid, inp.is_coinbase AS is_coinbase
            FROM transactions t, UNNEST(t.vin) AS x(inp)
        )
        SELECT
            count(*) FILTER (WHERE NOT is_coinbase) AS ordinary_input_count,
            count(*) FILTER (
                WHERE NOT is_coinbase
                AND NOT EXISTS (SELECT 1 FROM transactions t2 WHERE t2.txid = all_inputs.previous_txid)
            ) AS foreign_input_count
        FROM all_inputs
        """
    ).show()
    print("(These reference transactions outside our 10-block window -- expected, and covered in the walkthrough.)")

    print("=== Outputs created AND spent within the loaded window (not a full UTXO set -- see note) ===")
    con.sql(
        """
        WITH all_outputs AS (
            SELECT t.txid, idx - 1 AS output_index
            FROM transactions t, UNNEST(t.vout) WITH ORDINALITY AS o(out, idx)
        ),
        all_inputs AS (
            SELECT inp.txid AS previous_txid, inp.vout AS previous_vout_index
            FROM transactions t, UNNEST(t.vin) AS x(inp)
            WHERE NOT inp.is_coinbase
        )
        SELECT
            count(*) AS outputs_created_in_window,
            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM all_inputs ai
                    WHERE ai.previous_txid = all_outputs.txid
                      AND ai.previous_vout_index = all_outputs.output_index
                )
            ) AS outputs_also_spent_in_window
        FROM all_outputs
        """
    ).show()
    print(
        "NOTE: 'outputs created but not spent in window' is NOT the UTXO set -- it only means "
        "no spending input for that output happens to be loaded. It may well be spent by a "
        "transaction outside this window. See docs/PROJECT_STATE.md for the observation-boundary note."
    )


if __name__ == "__main__":
    run()
