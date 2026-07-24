"""Configuration for Esplora-based block ingestion.

Both mempool.space and blockstream.info run the same Esplora API shape,
so switching sources is just a base URL change (env var or --api-base-url).
"""

import os

DEFAULT_API_BASE_URL = "https://mempool.space/api"
FALLBACK_API_BASE_URL = "https://blockstream.info/api"

API_BASE_URL = os.environ.get("BTC_API_BASE_URL", DEFAULT_API_BASE_URL)

# Esplora paginates block transactions in fixed-size pages.
TX_PAGE_SIZE = 25

REQUEST_TIMEOUT_SECONDS = 15
REQUEST_MAX_RETRIES = 3
REQUEST_RETRY_BACKOFF_SECONDS = 0.5

DATA_RAW_BLOCKS_DIR = "data/raw/blocks"

# Zero-padding width for block-height directory names, so lexical sort
# matches numeric sort. 7 digits comfortably covers height for centuries.
HEIGHT_ZERO_PAD = 7

# Default depth behind the chain tip, chosen to avoid reorg handling
# for now: a block ~100 deep is already effectively immutable.
DEFAULT_BLOCKS_BEHIND_TIP = 100
