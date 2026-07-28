#!/usr/bin/env python3
"""Analytical summary over a loaded block window (Parquet-backed).

Complements scripts/validate_dataset.py (pass/fail data-quality gate) and
scripts/explore_block.py (teaching-oriented ad hoc exploration) with a
concise operational summary: fees, weight, script types, and the
window-boundary resolution percentages, scoped to [--start-height,
--end-height].

fee_rate_sats_per_vbyte = fee_sats / vsize. Coinbase transactions are
excluded from fee-rate ranking (their fee_sats is 0 by convention -- it's
not a market-priced transaction).

Usage:
    python scripts/summarize_dataset.py --start-height 959744 --end-height 959768
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

from btc_ingest.config import DATA_PARQUET_DIR


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-height", type=int, required=True)
    parser.add_argument("--end-height", type=int, required=True)
    return parser.parse_args()


def build_connection(parquet_root: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for dataset in ("blocks", "transactions", "inputs", "outputs"):
        glob = f"{parquet_root}/{dataset}/*/data.parquet"
        con.execute(f"CREATE VIEW {dataset} AS SELECT * FROM read_parquet('{glob}')")
    return con


def run(start: int, end: int) -> None:
    con = build_connection(Path(DATA_PARQUET_DIR))

    print(f"=== Summary for loaded window {start}-{end} ===")
    print()

    print("=== Blocks and transactions by height ===")
    con.sql(f"""
        SELECT b.block_height, b.block_hash, count(t.txid) AS tx_count
        FROM blocks b
        LEFT JOIN transactions t ON t.block_height = b.block_height AND t.block_height BETWEEN {start} AND {end}
        WHERE b.block_height BETWEEN {start} AND {end}
        GROUP BY b.block_height, b.block_hash ORDER BY b.block_height
    """).show()

    print("=== Total fees and average fee rate by block (ordinary transactions only) ===")
    con.sql(f"""
        SELECT block_height,
               sum(fee_sats) AS total_fees_sats,
               round(avg(fee_sats::DOUBLE / vsize), 3) AS avg_fee_rate_sats_per_vbyte
        FROM transactions
        WHERE block_height BETWEEN {start} AND {end} AND NOT is_coinbase
        GROUP BY block_height ORDER BY block_height
    """).show()

    print("=== Average and maximum block weight ===")
    con.sql(f"""
        SELECT round(avg(weight_units), 1) AS avg_weight_units, max(weight_units) AS max_weight_units
        FROM blocks WHERE block_height BETWEEN {start} AND {end}
    """).show()

    print("=== Average transaction size, weight, and vsize ===")
    con.sql(f"""
        SELECT round(avg(size_bytes), 1) AS avg_size_bytes,
               round(avg(weight_units), 1) AS avg_weight_units,
               round(avg(vsize), 1) AS avg_vsize
        FROM transactions WHERE block_height BETWEEN {start} AND {end}
    """).show()

    print("=== Input / output totals ===")
    con.sql(f"""
        SELECT
            (SELECT count(*) FROM inputs WHERE block_height BETWEEN {start} AND {end}) AS input_count,
            (SELECT count(*) FROM outputs WHERE block_height BETWEEN {start} AND {end}) AS output_count
    """).show()

    print("=== Percentage of ordinary inputs that resolve inside the loaded window ===")
    con.sql(f"""
        WITH ordinary_inputs AS (
            SELECT previous_txid FROM inputs
            WHERE block_height BETWEEN {start} AND {end} AND NOT is_coinbase
        )
        SELECT count(*) AS total_ordinary_inputs,
               count(*) FILTER (
                   WHERE EXISTS (
                       SELECT 1 FROM transactions t
                       WHERE t.txid = oi.previous_txid AND t.block_height BETWEEN {start} AND {end}
                   )
               ) AS internal_count,
               round(100.0 * count(*) FILTER (
                   WHERE EXISTS (
                       SELECT 1 FROM transactions t
                       WHERE t.txid = oi.previous_txid AND t.block_height BETWEEN {start} AND {end}
                   )
               ) / count(*), 2) AS internal_pct
        FROM ordinary_inputs oi
    """).show()

    print("=== Percentage of outputs spent later within the loaded window ===")
    con.sql(f"""
        WITH outputs_in_window AS (
            SELECT txid, output_index FROM outputs WHERE block_height BETWEEN {start} AND {end}
        )
        SELECT count(*) AS total_outputs,
               count(*) FILTER (
                   WHERE EXISTS (
                       SELECT 1 FROM inputs i
                       WHERE i.previous_txid = ow.txid AND i.previous_vout_index = ow.output_index
                         AND i.block_height BETWEEN {start} AND {end} AND NOT i.is_coinbase
                   )
               ) AS spent_in_window,
               round(100.0 * count(*) FILTER (
                   WHERE EXISTS (
                       SELECT 1 FROM inputs i
                       WHERE i.previous_txid = ow.txid AND i.previous_vout_index = ow.output_index
                         AND i.block_height BETWEEN {start} AND {end} AND NOT i.is_coinbase
                   )
               ) / count(*), 2) AS spent_in_window_pct
        FROM outputs_in_window ow
    """).show()

    print("=== Output counts by script type ===")
    con.sql(f"""
        SELECT script_type, count(*) AS output_count
        FROM outputs WHERE block_height BETWEEN {start} AND {end}
        GROUP BY script_type ORDER BY output_count DESC
    """).show()

    print("=== Outputs without a decoded address ===")
    con.sql(f"""
        SELECT count(*) AS total_outputs,
               count(*) FILTER (WHERE address IS NULL) AS no_address_count,
               round(100.0 * count(*) FILTER (WHERE address IS NULL) / count(*), 2) AS no_address_pct
        FROM outputs WHERE block_height BETWEEN {start} AND {end}
    """).show()

    print("=== 10 largest transactions by weight ===")
    con.sql(f"""
        SELECT txid, block_height, weight_units, size_bytes, vsize
        FROM transactions WHERE block_height BETWEEN {start} AND {end}
        ORDER BY weight_units DESC LIMIT 10
    """).show()

    print("=== 10 highest fee-rate transactions (coinbase excluded) ===")
    con.sql(f"""
        SELECT txid, block_height, fee_sats, vsize,
               round(fee_sats::DOUBLE / vsize, 3) AS fee_rate_sats_per_vbyte
        FROM transactions
        WHERE block_height BETWEEN {start} AND {end} AND NOT is_coinbase
        ORDER BY fee_rate_sats_per_vbyte DESC LIMIT 10
    """).show()


def main() -> None:
    args = parse_args()
    run(args.start_height, args.end_height)


if __name__ == "__main__":
    main()
