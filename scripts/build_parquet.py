#!/usr/bin/env python3
"""Milestone 2 CLI: flatten ingested raw blocks into partitioned Parquet.

Derived and regenerable -- safe to delete data/parquet/ and re-run this
at any time. Only reads from data/raw/blocks/; never a second source of
truth.

Usage:
    python scripts/build_parquet.py                    # every complete raw block on disk
    python scripts/build_parquet.py --height 959330
    python scripts/build_parquet.py --start-height 959739 --end-height 959748
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from btc_ingest.config import DATA_PARQUET_DIR, DATA_RAW_BLOCKS_DIR
from btc_ingest.extract import block_dir_name, is_complete, read_raw_block
from btc_ingest.parquet_build import build_block_parquet_partitions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--height", type=int, default=None)
    parser.add_argument("--start-height", type=int, default=None)
    parser.add_argument("--end-height", type=int, default=None)
    return parser.parse_args()


def discover_heights(raw_root: Path) -> list[int]:
    return sorted(int(p.name) for p in raw_root.iterdir() if p.is_dir() and p.name.isdigit())


def main() -> None:
    args = parse_args()
    raw_root = Path(DATA_RAW_BLOCKS_DIR)
    parquet_root = Path(DATA_PARQUET_DIR)

    if args.height is not None:
        heights = [args.height]
    elif args.start_height is not None and args.end_height is not None:
        heights = list(range(args.start_height, args.end_height + 1))
    else:
        heights = discover_heights(raw_root)

    built, incomplete = [], []
    for height in heights:
        block_dir = raw_root / block_dir_name(height)
        if not is_complete(block_dir):
            incomplete.append(height)
            continue
        block_json, tx_objects = read_raw_block(block_dir)
        row_counts = build_block_parquet_partitions(parquet_root, height, block_json, tx_objects)
        built.append((height, row_counts))
        print(
            f"Height {height}: blocks=1 "
            f"transactions={row_counts['transactions']} "
            f"inputs={row_counts['inputs']} "
            f"outputs={row_counts['outputs']}"
        )

    if incomplete:
        print(f"Skipped (raw block missing/incomplete, not in Parquet output): {incomplete}")

    print(f"Built Parquet partitions for {len(built)} block(s) under {parquet_root}/")


if __name__ == "__main__":
    main()
