import duckdb
import pytest

from btc_ingest.parquet_build import build_block_parquet_partitions, write_parquet_partition_atomic


def test_write_parquet_partition_atomic_roundtrip(tmp_path):
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    path = write_parquet_partition_atomic(tmp_path, 100, rows)

    assert path == tmp_path / "block_height=100" / "data.parquet"
    result = duckdb.sql(f"SELECT a, b FROM read_parquet('{path}') ORDER BY a").fetchall()
    assert result == [(1, "x"), (2, "y")]


def test_write_parquet_partition_atomic_only_touches_its_own_height(tmp_path):
    write_parquet_partition_atomic(tmp_path, 100, [{"a": 1}])
    path_101 = write_parquet_partition_atomic(tmp_path, 101, [{"a": 2}])

    assert path_101 == tmp_path / "block_height=101" / "data.parquet"
    height_100_value = duckdb.sql(
        f"SELECT a FROM read_parquet('{tmp_path}/block_height=100/data.parquet')"
    ).fetchone()
    assert height_100_value == (1,)  # untouched by writing height 101


def test_write_parquet_partition_atomic_failure_preserves_previous_file(tmp_path, monkeypatch):
    import btc_ingest.parquet_build as pb

    write_parquet_partition_atomic(tmp_path, 100, [{"a": 1}])
    original_replace = pb.os.replace

    def failing_replace(*args, **kwargs):
        raise OSError("simulated failure during publish")

    monkeypatch.setattr(pb.os, "replace", failing_replace)
    with pytest.raises(OSError):
        write_parquet_partition_atomic(tmp_path, 100, [{"a": 999}])
    monkeypatch.setattr(pb.os, "replace", original_replace)

    # Old partition is untouched, and no temp files were left behind.
    result = duckdb.sql(
        f"SELECT a FROM read_parquet('{tmp_path}/block_height=100/data.parquet')"
    ).fetchone()
    assert result == (1,)
    partition_dir = tmp_path / "block_height=100"
    leftovers = list(partition_dir.glob("*.tmp.parquet")) + list(partition_dir.glob("*.jsonl"))
    assert leftovers == []


def test_build_block_parquet_partitions_row_counts_and_files(tmp_path):
    block_json = {
        "id": "hash_b",
        "height": 100,
        "previousblockhash": "hash_a",
        "timestamp": 1700000000,
        "tx_count": 1,
        "size": 500,
        "weight": 2000,
        "difficulty": 1.0,
        "merkle_root": "m",
        "nonce": 1,
        "bits": 1,
    }
    tx = {
        "txid": "coinbase",
        "version": 1,
        "locktime": 0,
        "size": 200,
        "weight": 800,
        "fee": 0,
        "vin": [
            {
                "txid": "0" * 64,
                "vout": 4294967295,
                "prevout": None,
                "scriptsig": "",
                "sequence": 0,
                "is_coinbase": True,
            }
        ],
        "vout": [
            {
                "value": 100,
                "scriptpubkey_address": "bc1q",
                "scriptpubkey_type": "v0_p2wpkh",
                "scriptpubkey": "00",
            }
        ],
    }

    counts = build_block_parquet_partitions(tmp_path, 100, block_json, [tx])

    assert counts == {"blocks": 1, "transactions": 1, "inputs": 1, "outputs": 1}
    for dataset in ("blocks", "transactions", "inputs", "outputs"):
        assert (tmp_path / dataset / "block_height=100" / "data.parquet").exists()
