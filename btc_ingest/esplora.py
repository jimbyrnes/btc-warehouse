"""Minimal client for the Esplora API (mempool.space / blockstream.info).

Only the endpoints Milestone 1 needs: tip height, height->hash, block
metadata, and paginated block transactions.
"""

import time
from typing import Callable

import requests

from btc_ingest.config import (
    REQUEST_MAX_RETRIES,
    REQUEST_RETRY_BACKOFF_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    TX_PAGE_SIZE,
)


def paginate_block_txs(
    fetch_page: Callable[[int], list], page_size: int = TX_PAGE_SIZE
) -> tuple[list, int]:
    """Merge Esplora's paginated block-tx responses into one flat list.

    `fetch_page(start_index)` returns one page of raw transaction objects.
    Pagination stops at the first page shorter than `page_size` (Esplora's
    convention for "last page"), including an empty page. This is a pure
    function so pagination logic is testable without any HTTP calls.
    """
    txs: list = []
    start_index = 0
    pages_fetched = 0
    while True:
        page = fetch_page(start_index)
        pages_fetched += 1
        txs.extend(page)
        if len(page) < page_size:
            break
        start_index += page_size
    return txs, pages_fetched


class NotFoundError(Exception):
    """Raised for HTTP 404s, which Esplora uses for out-of-range pagination."""


class EsploraClient:
    def __init__(
        self,
        base_url: str,
        session: requests.Session | None = None,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout

    def _get(self, path: str):
        url = f"{self.base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(REQUEST_MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code == 404:
                    # Not found is not transient -- don't waste retries on it.
                    raise NotFoundError(url)
                response.raise_for_status()
                return response
            except NotFoundError:
                raise
            except requests.RequestException as exc:
                last_error = exc
                if attempt < REQUEST_MAX_RETRIES - 1:
                    time.sleep(REQUEST_RETRY_BACKOFF_SECONDS * (2**attempt))
        raise RuntimeError(f"GET {url} failed after {REQUEST_MAX_RETRIES} attempts") from last_error

    def get_tip_height(self) -> int:
        return int(self._get("/blocks/tip/height").text)

    def get_block_hash(self, height: int) -> str:
        return self._get(f"/block-height/{height}").text.strip()

    def get_block(self, block_hash: str) -> dict:
        return self._get(f"/block/{block_hash}").json()

    def get_block_txs(self, block_hash: str) -> tuple[list, int]:
        def fetch_page(start_index: int) -> list:
            path = f"/block/{block_hash}/txs"
            if start_index:
                path += f"/{start_index}"
            try:
                return self._get(path).json()
            except NotFoundError:
                # Esplora returns 404 (not an empty array) once start_index
                # reaches the transaction count -- that's the end of pagination.
                return []

        return paginate_block_txs(fetch_page)
