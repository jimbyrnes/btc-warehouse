#!/usr/bin/env python3
"""Milestone 3 formal data-quality gate.

Queries the partitioned Parquet datasets with DuckDB and runs a fixed set
of internal-consistency checks scoped to [--start-height, --end-height].
Prints readable terminal output and writes a machine-readable JSON report
under data/reports/ (git-ignored). Exits non-zero only on a real internal
inconsistency (FAIL) -- bounded-window artifacts (foreign input
references, the first block's unverifiable predecessor, in-window-unspent
outputs, addressless outputs) are reported but never fail the gate.

Usage:
    python scripts/validate_dataset.py --start-height 959744 --end-height 959768
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

from btc_ingest.config import DATA_PARQUET_DIR, DATA_REPORTS_DIR
from btc_ingest.validate import print_report, run_validation, write_report_json


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


def main() -> None:
    args = parse_args()
    if args.end_height < args.start_height:
        print(f"ERROR: end-height ({args.end_height}) must be >= start-height ({args.start_height}).")
        sys.exit(2)

    con = build_connection(Path(DATA_PARQUET_DIR))
    report = run_validation(con, args.start_height, args.end_height)

    print_report(report)

    report_path = Path(DATA_REPORTS_DIR) / f"validation_{args.start_height}-{args.end_height}.json"
    write_report_json(report, report_path)
    print()
    print(f"JSON report written to {report_path}")

    sys.exit(0 if report["overall_status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
