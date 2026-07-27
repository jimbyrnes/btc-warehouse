import pytest

from btc_ingest.extract import (
    block_dir_name,
    ingest_block_range,
    is_complete,
    resolve_block_range,
    write_block_atomic,
)


def test_resolve_block_range_explicit_start_end():
    assert resolve_block_range(start_height=100, end_height=109) == (100, 109)


def test_resolve_block_range_rejects_backwards_explicit_range():
    with pytest.raises(ValueError):
        resolve_block_range(start_height=110, end_height=100)


def test_resolve_block_range_start_plus_count():
    assert resolve_block_range(start_height=100, count=10) == (100, 109)


def test_resolve_block_range_start_plus_default_count():
    assert resolve_block_range(start_height=100) == (100, 109)  # DEFAULT_RANGE_COUNT == 10


def test_resolve_block_range_behind_tip_plus_count():
    assert resolve_block_range(behind_tip=100, count=10, tip_height=1000) == (891, 900)


def test_resolve_block_range_behind_tip_uses_defaults():
    # default behind_tip=100, default count=10 -> same as the explicit case
    assert resolve_block_range(tip_height=1000) == (891, 900)


def test_resolve_block_range_requires_tip_height_for_behind_tip_mode():
    with pytest.raises(ValueError):
        resolve_block_range(behind_tip=100, count=10)


def _make_fake_client(fail_heights: set[int] | None = None):
    fail_heights = fail_heights or set()

    class FakeClient:
        base_url = "https://example.invalid/api"

        def get_block_hash(self, height):
            if height in fail_heights:
                raise RuntimeError(f"simulated network failure at {height}")
            return f"hash_{height}"

        def get_block(self, block_hash):
            return {"id": block_hash, "tx_count": 1}

        def get_block_txs(self, block_hash):
            return [{"txid": f"tx_{block_hash}"}], 1

    return FakeClient()


def test_ingest_block_range_reports_fetched_and_skipped(tmp_path):
    # Pre-populate height 101 as already complete.
    write_block_atomic(
        tmp_path, 101, {"id": "hash_101"}, [{"txid": "tx_hash_101"}],
        {"block_height": 101, "tx_count_fetched": 1, "tx_count_reported": 1},
    )

    result = ingest_block_range(_make_fake_client(), 100, 102, tmp_path, inter_block_delay=0)

    assert result.skipped == [101]
    assert sorted(result.fetched) == [100, 102]
    assert result.failed == []
    assert result.is_complete
    for height in (100, 101, 102):
        assert is_complete(tmp_path / block_dir_name(height))


def test_ingest_block_range_one_failure_does_not_corrupt_others(tmp_path):
    result = ingest_block_range(
        _make_fake_client(fail_heights={101}), 100, 102, tmp_path, inter_block_delay=0
    )

    assert result.fetched == [100, 102]
    assert result.failed == [(101, "simulated network failure at 101")]
    assert not result.is_complete
    assert is_complete(tmp_path / block_dir_name(100))
    assert is_complete(tmp_path / block_dir_name(102))
    assert not is_complete(tmp_path / block_dir_name(101))
