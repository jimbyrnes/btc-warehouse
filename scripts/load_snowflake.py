#!/usr/bin/env python3
"""Milestone 4 CLI: load validated local Parquet into Snowflake RAW tables.

Refuses to run unless a passing Milestone 3 validation report already
exists for exactly the requested range (see scripts/validate_dataset.py).
Loads via PUT -> COPY INTO a temp table -> MERGE into the permanent RAW
table on a natural key, so re-running against an already-loaded range
never duplicates rows.

Requires real Snowflake credentials in the environment (see .env.example)
-- this script makes a live connection and cannot be exercised without one.

Usage:
    python scripts/load_snowflake.py --start-height 959744 --end-height 959768
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from btc_ingest.config import DATA_PARQUET_DIR, DATA_REPORTS_DIR
from btc_ingest.snowflake_loader import (
    SnowflakeConfig,
    SnowflakeConfigError,
    ValidationNotPassedError,
    build_connection,
    load_height_range,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-height", type=int, required=True)
    parser.add_argument("--end-height", type=int, required=True)
    return parser.parse_args()


def main() -> None:
    load_dotenv()  # populates os.environ from a repo-root .env if present
    args = parse_args()

    if args.end_height < args.start_height:
        print(f"ERROR: end-height ({args.end_height}) must be >= start-height ({args.start_height}).")
        sys.exit(2)

    try:
        config = SnowflakeConfig.from_env()
    except SnowflakeConfigError as exc:
        print(f"ERROR: {exc}")
        sys.exit(2)

    print(f"Loading heights {args.start_height}-{args.end_height} into "
          f"{config.database}.RAW as warehouse {config.warehouse} ...")

    try:
        con = build_connection(config)
    except Exception as exc:
        print(f"ERROR: could not connect to Snowflake: {exc}")
        sys.exit(1)

    try:
        result = load_height_range(
            con, Path(DATA_PARQUET_DIR), Path(DATA_REPORTS_DIR), args.start_height, args.end_height
        )
    except ValidationNotPassedError as exc:
        print(f"ERROR: {exc}")
        sys.exit(2)
    finally:
        con.close()

    print()
    print(f"Loaded ({len(result.loaded)} height/dataset pairs): {result.loaded}")
    if result.failed:
        print(f"Failed ({len(result.failed)}):")
        for height, dataset, error in result.failed:
            print(f"  height={height} dataset={dataset}: {error}")

    if not result.is_complete:
        print()
        print("ERROR: one or more height/dataset partitions failed to load.")
        sys.exit(1)

    print()
    print(f"Load complete for heights {args.start_height}-{args.end_height}.")
    print("Remember to suspend the warehouse when done: "
          "ALTER WAREHOUSE BTC_WAREHOUSE_WH SUSPEND;")


if __name__ == "__main__":
    main()
