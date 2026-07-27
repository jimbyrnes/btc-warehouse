#!/usr/bin/env python3
"""Milestone 2 CLI: fetch a range of consecutive blocks and all their
transactions to disk, sequentially, reusing the single-block ingestion
logic from Milestone 1.

Usage:
    python scripts/fetch_blocks.py                              # 10 blocks ending tip-100
    python scripts/fetch_blocks.py --count 10 --behind-tip 100
    python scripts/fetch_blocks.py --start-height 959739 --end-height 959748
    python scripts/fetch_blocks.py --start-height 959739 --count 10
    python scripts/fetch_blocks.py --force                      # re-fetch even if complete
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from btc_ingest.config import API_BASE_URL, DATA_RAW_BLOCKS_DIR
from btc_ingest.esplora import EsploraClient
from btc_ingest.extract import block_dir_name, ingest_block_range, is_complete, resolve_block_range


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-height", type=int, default=None)
    parser.add_argument("--end-height", type=int, default=None)
    parser.add_argument("--count", type=int, default=None, help="Number of consecutive blocks.")
    parser.add_argument("--behind-tip", type=int, default=None, help="End the range this many blocks behind tip.")
    parser.add_argument("--api-base-url", default=API_BASE_URL)
    parser.add_argument("--force", action="store_true", help="Re-fetch even if already ingested.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = EsploraClient(args.api_base_url)

    tip_height = None
    if args.start_height is None:
        tip_height = client.get_tip_height()
        print(f"Chain tip is {tip_height}.")

    start_height, end_height = resolve_block_range(
        start_height=args.start_height,
        end_height=args.end_height,
        count=args.count,
        behind_tip=args.behind_tip,
        tip_height=tip_height,
    )
    total = end_height - start_height + 1
    print(f"Target range: {start_height}-{end_height} ({total} blocks) from {args.api_base_url}")

    output_root = Path(DATA_RAW_BLOCKS_DIR)
    result = ingest_block_range(client, start_height, end_height, output_root, force=args.force)

    print()
    print(f"Fetched ({len(result.fetched)}): {result.fetched}")
    print(f"Skipped, already complete ({len(result.skipped)}): {result.skipped}")
    if result.failed:
        print(f"Failed ({len(result.failed)}):")
        for height, error in result.failed:
            print(f"  {height}: {error}")

    incomplete = [
        height
        for height in range(start_height, end_height + 1)
        if not is_complete(output_root / block_dir_name(height))
    ]
    if incomplete:
        print()
        print(f"ERROR: range {start_height}-{end_height} is incomplete. "
              f"Missing/incomplete heights: {incomplete}")
        sys.exit(1)

    print()
    print(f"Range {start_height}-{end_height} complete: {total}/{total} blocks.")


if __name__ == "__main__":
    main()
