"""Ingest a single block's raw data to disk, atomically and idempotently.

Layout per block:
    data/raw/blocks/<height, zero-padded>/
        block.json   raw /block/:hash response, unmodified
        txs.jsonl    one raw transaction object per line, unmodified
        _meta.json   ingestion provenance (height, hash, source, timestamps)

"Unmodified" means field-level fidelity: every transaction object is
serialized as-is, with no fields added, removed, or transformed. Splitting
the API's paginated JSON arrays into one JSON object per line is a change
in file-level serialization, not in the content of each object.

The whole block directory is written to a temporary sibling directory and
then renamed into place in one atomic step, so a directory only ever
exists under its final name once all three files are fully written. A
process crash mid-fetch leaves an orphaned temp directory, never a
half-written final one.
"""

import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from btc_ingest.config import HEIGHT_ZERO_PAD
from btc_ingest.esplora import EsploraClient

REQUIRED_FILES = ("block.json", "txs.jsonl", "_meta.json")


def block_dir_name(height: int) -> str:
    return f"{height:0{HEIGHT_ZERO_PAD}d}"


def is_complete(block_dir: Path) -> bool:
    return block_dir.is_dir() and all((block_dir / name).exists() for name in REQUIRED_FILES)


def write_block_atomic(
    output_root: Path,
    height: int,
    block_json: dict,
    tx_objects: list,
    meta: dict,
) -> Path:
    """Write block.json/txs.jsonl/_meta.json and atomically publish the dir.

    Returns the final block directory path. Raises and leaves no final
    directory behind if writing fails partway through.
    """
    output_root.mkdir(parents=True, exist_ok=True)
    final_dir = output_root / block_dir_name(height)

    tmp_dir = Path(tempfile.mkdtemp(prefix=f".tmp-{block_dir_name(height)}-", dir=output_root))
    try:
        (tmp_dir / "block.json").write_text(json.dumps(block_json, indent=2))

        with (tmp_dir / "txs.jsonl").open("w") as f:
            for tx in tx_objects:
                f.write(json.dumps(tx, separators=(",", ":")))
                f.write("\n")

        (tmp_dir / "_meta.json").write_text(json.dumps(meta, indent=2))

        # Atomic on POSIX filesystems as long as src/dst share a mount,
        # which tmp_dir (created under output_root) guarantees.
        os.replace(tmp_dir, final_dir)
    except BaseException:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    return final_dir


def ingest_block(
    client: EsploraClient,
    height: int,
    output_root: Path,
    force: bool = False,
) -> Path:
    """Fetch one block + all its transactions and write them to disk.

    Idempotent: if the block's directory is already complete, the fetch is
    skipped unless `force=True`.
    """
    final_dir = output_root / block_dir_name(height)
    if not force and is_complete(final_dir):
        return final_dir

    block_hash = client.get_block_hash(height)
    block_json = client.get_block(block_hash)
    tx_objects, pages_fetched = client.get_block_txs(block_hash)

    meta = {
        "block_height": height,
        "block_hash": block_hash,
        "api_base_url": client.base_url,
        "tx_count_fetched": len(tx_objects),
        "tx_count_reported": block_json.get("tx_count"),
        "pages_fetched": pages_fetched,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    return write_block_atomic(output_root, height, block_json, tx_objects, meta)
