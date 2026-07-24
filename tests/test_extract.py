import json

import pytest

from btc_ingest.esplora import EsploraClient, NotFoundError, paginate_block_txs
from btc_ingest.extract import is_complete, write_block_atomic


def test_paginate_stops_on_short_page():
    pages = {0: [{"txid": "a"}, {"txid": "b"}], 2: [{"txid": "c"}]}
    calls = []

    def fetch_page(start_index):
        calls.append(start_index)
        return pages[start_index]

    txs, pages_fetched = paginate_block_txs(fetch_page, page_size=2)

    assert [t["txid"] for t in txs] == ["a", "b", "c"]
    assert pages_fetched == 2
    assert calls == [0, 2]


def test_paginate_stops_on_empty_page_when_evenly_divisible():
    pages = {0: [{"txid": "a"}, {"txid": "b"}], 2: []}

    def fetch_page(start_index):
        return pages[start_index]

    txs, pages_fetched = paginate_block_txs(fetch_page, page_size=2)

    assert [t["txid"] for t in txs] == ["a", "b"]
    assert pages_fetched == 2


def test_get_block_txs_treats_404_as_end_of_pagination(monkeypatch):
    # Esplora returns HTTP 404 (not an empty JSON array) once start_index
    # reaches the block's transaction count -- a real quirk hit while
    # fetching a block whose tx_count was an exact multiple of the page size.
    client = EsploraClient("https://example.invalid/api")
    pages = {0: [{"txid": "a"}] * 25, 25: [{"txid": "b"}] * 25, 50: NotFoundError}

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_get(path: str):
        last_segment = path.rsplit("/", 1)[-1]
        start_index = int(last_segment) if last_segment.isdigit() else 0
        result = pages[start_index]
        if result is NotFoundError:
            raise NotFoundError("simulated 404")
        return FakeResponse(result)

    monkeypatch.setattr(client, "_get", fake_get)

    txs, pages_fetched = client.get_block_txs("deadbeef")
    assert len(txs) == 50
    assert pages_fetched == 3


def test_write_block_atomic_preserves_tx_fields_unmodified(tmp_path):
    block_json = {"id": "hash123", "height": 100}
    tx = {
        "txid": "abc",
        "vin": [{"is_coinbase": True}],
        "vout": [{"value": 5000}],
        "fee": 0,
        "status": {"confirmed": True, "block_height": 100},
    }
    meta = {"block_height": 100, "block_hash": "hash123"}

    final_dir = write_block_atomic(tmp_path, 100, block_json, [tx], meta)

    lines = (final_dir / "txs.jsonl").read_text().splitlines()
    assert len(lines) == 1
    round_tripped = json.loads(lines[0])
    assert round_tripped == tx
    assert set(round_tripped.keys()) == set(tx.keys())


def test_write_block_atomic_is_all_or_nothing_on_failure(tmp_path, monkeypatch):
    import btc_ingest.extract as extract_module

    original_replace = extract_module.os.replace

    def failing_replace(*args, **kwargs):
        raise OSError("simulated failure during publish")

    monkeypatch.setattr(extract_module.os, "replace", failing_replace)

    with pytest.raises(OSError):
        write_block_atomic(tmp_path, 100, {"id": "h"}, [{"txid": "a"}], {"block_height": 100})

    final_dir = tmp_path / "0000100"
    assert not final_dir.exists()
    # no orphaned temp directories left behind either
    assert list(tmp_path.iterdir()) == []

    monkeypatch.setattr(extract_module.os, "replace", original_replace)


def test_is_complete_requires_all_three_files(tmp_path):
    block_dir = tmp_path / "0000100"
    block_dir.mkdir()
    assert not is_complete(block_dir)

    (block_dir / "block.json").write_text("{}")
    (block_dir / "txs.jsonl").write_text("")
    assert not is_complete(block_dir)

    (block_dir / "_meta.json").write_text("{}")
    assert is_complete(block_dir)


def test_ingest_block_skips_when_already_complete(tmp_path, monkeypatch):
    from btc_ingest.extract import ingest_block

    output_root = tmp_path
    write_block_atomic(output_root, 100, {"id": "h"}, [{"txid": "a"}], {"block_height": 100})

    class ExplodingClient:
        base_url = "https://example.invalid/api"

        def get_block_hash(self, height):
            raise AssertionError("should not be called when already complete")

    result_dir = ingest_block(ExplodingClient(), 100, output_root)
    assert result_dir == output_root / "0000100"
