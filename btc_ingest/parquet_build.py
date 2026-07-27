"""Build derived, regenerable Parquet datasets from raw block JSON.

One partition per block height, one dataset per entity (blocks,
transactions, inputs, outputs):

    data/parquet/<dataset>/block_height=<height>/data.parquet

Regenerating one height's partitions never reads or rewrites any other
height's files -- these are derived artifacts, safe to delete and rebuild
from data/raw/ at any time, never a second copy of the raw source of truth.
"""

import json
import os
import tempfile
from pathlib import Path

import duckdb

from btc_ingest.flatten import (
    flatten_block,
    flatten_inputs,
    flatten_outputs,
    flatten_transactions,
)


def write_parquet_partition_atomic(dataset_root: Path, block_height: int, rows: list[dict]) -> Path:
    """Write one block-height partition's `data.parquet`, atomically.

    Rows are staged as newline-delimited JSON and loaded through DuckDB's
    JSON reader so column types are inferred from the actual values, then
    copied to a temp Parquet file in the same partition directory and
    `os.replace`'d into place. A failure at any point leaves the previous
    `data.parquet` (if any) untouched -- never a partially written file --
    and no temp files behind.
    """
    partition_dir = dataset_root / f"block_height={block_height}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    final_path = partition_dir / "data.parquet"

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", dir=partition_dir, delete=False
    ) as f:
        for row in rows:
            f.write(json.dumps(row))
            f.write("\n")
        ndjson_path = Path(f.name)

    tmp_parquet_path = partition_dir / f"{ndjson_path.stem}.tmp.parquet"
    try:
        con = duckdb.connect()
        con.execute(
            f"COPY (SELECT * FROM read_ndjson_auto('{ndjson_path}')) "
            f"TO '{tmp_parquet_path}' (FORMAT parquet)"
        )
        os.replace(tmp_parquet_path, final_path)
    finally:
        ndjson_path.unlink(missing_ok=True)
        if tmp_parquet_path.exists():
            tmp_parquet_path.unlink(missing_ok=True)

    return final_path


def build_block_parquet_partitions(
    parquet_root: Path, block_height: int, block_json: dict, tx_objects: list[dict]
) -> dict[str, int]:
    """Flatten one block's raw JSON and write its four dataset partitions.

    Returns the row count written per dataset.
    """
    block_hash = block_json["id"]
    rows_by_dataset = {
        "blocks": [flatten_block(block_json)],
        "transactions": flatten_transactions(block_height, block_hash, tx_objects),
        "inputs": flatten_inputs(block_height, tx_objects),
        "outputs": flatten_outputs(block_height, tx_objects),
    }
    for dataset, rows in rows_by_dataset.items():
        write_parquet_partition_atomic(parquet_root / dataset, block_height, rows)
    return {dataset: len(rows) for dataset, rows in rows_by_dataset.items()}
