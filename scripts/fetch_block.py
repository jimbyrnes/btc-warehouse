#!/usr/bin/env python3
"""Milestone 1 CLI: fetch one block and all its transactions to disk.

Usage:
    python scripts/fetch_block.py                  # tip - 100 (default)
    python scripts/fetch_block.py --behind-tip 50
    python scripts/fetch_block.py --height 959330
    python scripts/fetch_block.py --api-base-url https://blockstream.info/api
    python scripts/fetch_block.py --force           # re-fetch even if complete
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from btc_ingest.config import (
    API_BASE_URL,
    DATA_RAW_BLOCKS_DIR,
    DEFAULT_BLOCKS_BEHIND_TIP,
)
from btc_ingest.esplora import EsploraClient
from btc_ingest.extract import ingest_block, is_complete, block_dir_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--height", type=int, default=None, help="Exact block height to fetch."
    )
    group.add_argument(
        "--behind-tip",
        type=int,
        default=DEFAULT_BLOCKS_BEHIND_TIP,
        help=f"Fetch (tip - N). Default: {DEFAULT_BLOCKS_BEHIND_TIP}.",
    )
    parser.add_argument(
        "--api-base-url",
        default=API_BASE_URL,
        help=f"Esplora API base URL. Default: {API_BASE_URL}",
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-fetch even if already ingested."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = EsploraClient(args.api_base_url)

    if args.height is not None:
        height = args.height
    else:
        tip = client.get_tip_height()
        height = tip - args.behind_tip
        print(f"Chain tip is {tip}; targeting height {height} ({args.behind_tip} behind tip).")

    output_root = Path(DATA_RAW_BLOCKS_DIR)
    final_dir = output_root / block_dir_name(height)

    if not args.force and is_complete(final_dir):
        print(f"Block {height} already ingested at {final_dir} (use --force to re-fetch).")
        return

    print(f"Fetching block {height} from {args.api_base_url} ...")
    result_dir = ingest_block(client, height, output_root, force=args.force)
    print(f"Done. Wrote {result_dir}/{{block.json,txs.jsonl,_meta.json}}")


if __name__ == "__main__":
    main()
