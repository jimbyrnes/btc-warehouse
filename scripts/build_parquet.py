#!/usr/bin/env python3
"""Flatten ingested raw blocks into partitioned Parquet.

Derived and regenerable -- safe to delete data/parquet/ and re-run this
at any time. Only reads from data/raw/blocks/; never a second source of
truth.

Usage:
    python scripts/build_parquet.py                              # every complete raw block on disk
    python scripts/build_parquet.py --height 959744
    python scripts/build_parquet.py --start-height 959744 --end-height 959768
    python scripts/build_parquet.py --start-height 959744 --end-height 959768 --force
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from btc_ingest.config import DATA_PARQUET_DIR, DATA_RAW_BLOCKS_DIR
from btc_ingest.extract import block_dir_name, is_complete, read_raw_block
from btc_ingest.parquet_build import build_block_parquet_partitions, parquet_partitions_complete


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--height", type=int, default=None)
    parser.add_argument("--start-height", type=int, default=None)
    parser.add_argument("--end-height", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Rebuild even if partitions already exist.")
    return parser.parse_args()


def discover_heights(raw_root: Path) -> list[int]:
    return sorted(int(p.name) for p in raw_root.iterdir() if p.is_dir() and p.name.isdigit())


def main() -> None:
    args = parse_args()
    raw_root = Path(DATA_RAW_BLOCKS_DIR)
    parquet_root = Path(DATA_PARQUET_DIR)

    explicit_range = args.height is not None or (args.start_height is not None and args.end_height is not None)
    if args.height is not None:
        heights = [args.height]
    elif args.start_height is not None and args.end_height is not None:
        heights = list(range(args.start_height, args.end_height + 1))
    else:
        heights = discover_heights(raw_root)

    created, skipped, missing_raw, failed = [], [], [], []
    for height in heights:
        block_dir = raw_root / block_dir_name(height)
        if not is_complete(block_dir):
            missing_raw.append(height)
            continue
        if not args.force and parquet_partitions_complete(parquet_root, height):
            skipped.append(height)
            continue
        try:
            block_json, tx_objects = read_raw_block(block_dir)
            row_counts = build_block_parquet_partitions(parquet_root, height, block_json, tx_objects)
            created.append(height)
            print(
                f"Height {height}: built (blocks=1 "
                f"transactions={row_counts['transactions']} "
                f"inputs={row_counts['inputs']} "
                f"outputs={row_counts['outputs']})"
            )
        except Exception as exc:
            failed.append((height, str(exc)))

    print()
    print(f"Created ({len(created)}): {created}")
    print(f"Skipped, already up to date ({len(skipped)}): {skipped}")
    if missing_raw:
        print(f"Skipped, raw block missing/incomplete ({len(missing_raw)}): {missing_raw}")
    if failed:
        print(f"Failed ({len(failed)}):")
        for height, error in failed:
            print(f"  {height}: {error}")

    if explicit_range:
        incomplete = [h for h in heights if not parquet_partitions_complete(parquet_root, h)]
        if incomplete:
            print()
            print(f"ERROR: requested Parquet range is incomplete. Missing/failed heights: {incomplete}")
            sys.exit(1)

    print()
    print(f"Parquet partitions up to date for {len(created) + len(skipped)} block(s) under {parquet_root}/")


if __name__ == "__main__":
    main()
