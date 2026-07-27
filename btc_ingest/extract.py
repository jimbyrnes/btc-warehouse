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
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from btc_ingest.config import (
    DEFAULT_BLOCKS_BEHIND_TIP,
    DEFAULT_RANGE_COUNT,
    HEIGHT_ZERO_PAD,
    INTER_BLOCK_DELAY_SECONDS,
)
from btc_ingest.esplora import EsploraClient

REQUIRED_FILES = ("block.json", "txs.jsonl", "_meta.json")


def block_dir_name(height: int) -> str:
    return f"{height:0{HEIGHT_ZERO_PAD}d}"


def is_complete(block_dir: Path) -> bool:
    """A block directory is complete only if all three files exist AND
    `_meta.json` confirms every reported transaction was actually fetched.

    The mere presence of the three files is not sufficient -- a run that
    was interrupted after the atomic rename but recorded a short fetch
    would still look "done" by file existence alone.
    """
    if not block_dir.is_dir():
        return False
    if not all((block_dir / name).exists() for name in REQUIRED_FILES):
        return False
    try:
        meta = json.loads((block_dir / "_meta.json").read_text())
    except (json.JSONDecodeError, OSError):
        return False
    tx_count_reported = meta.get("tx_count_reported")
    tx_count_fetched = meta.get("tx_count_fetched")
    if tx_count_reported is not None and tx_count_fetched != tx_count_reported:
        return False
    return True


def read_raw_block(block_dir: Path) -> tuple[dict, list[dict]]:
    """Load a previously-ingested block's raw JSON back into Python objects."""
    block_json = json.loads((block_dir / "block.json").read_text())
    tx_objects = [
        json.loads(line)
        for line in (block_dir / "txs.jsonl").read_text().splitlines()
        if line
    ]
    return block_json, tx_objects


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


def resolve_block_range(
    *,
    start_height: int | None = None,
    end_height: int | None = None,
    count: int | None = None,
    behind_tip: int | None = None,
    tip_height: int | None = None,
) -> tuple[int, int]:
    """Resolve a fetch request into an explicit (start_height, end_height).

    Three ways to specify a range:
      - start_height + end_height: used as-is.
      - start_height + count: `count` consecutive blocks from start_height.
      - behind_tip + count (default mode): `count` consecutive blocks
        ending `behind_tip` blocks behind `tip_height`.

    Pure function -- `tip_height` is passed in rather than fetched here, so
    range resolution is testable without any network access.
    """
    if start_height is not None and end_height is not None:
        if count is not None:
            raise ValueError("Specify either end_height or count, not both.")
        if end_height < start_height:
            raise ValueError(f"end_height ({end_height}) must be >= start_height ({start_height}).")
        return start_height, end_height

    n = count if count is not None else DEFAULT_RANGE_COUNT

    if start_height is not None:
        return start_height, start_height + n - 1

    if tip_height is None:
        raise ValueError("tip_height is required to resolve a --behind-tip range.")
    depth = behind_tip if behind_tip is not None else DEFAULT_BLOCKS_BEHIND_TIP
    end = tip_height - depth
    return end - n + 1, end


@dataclass
class BlockRangeResult:
    fetched: list[int] = field(default_factory=list)
    skipped: list[int] = field(default_factory=list)
    failed: list[tuple[int, str]] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return not self.failed


def ingest_block_range(
    client: EsploraClient,
    start_height: int,
    end_height: int,
    output_root: Path,
    force: bool = False,
    inter_block_delay: float = INTER_BLOCK_DELAY_SECONDS,
) -> BlockRangeResult:
    """Ingest each height in [start_height, end_height] sequentially.

    Each block is written atomically to its own directory (see
    `write_block_atomic`), so one block failing can never corrupt another
    block's already-completed data -- failures are simply recorded and
    ingestion continues with the next height.
    """
    result = BlockRangeResult()
    heights = list(range(start_height, end_height + 1))
    for i, height in enumerate(heights):
        final_dir = output_root / block_dir_name(height)
        was_already_complete = not force and is_complete(final_dir)
        try:
            ingest_block(client, height, output_root, force=force)
            if was_already_complete:
                result.skipped.append(height)
            else:
                result.fetched.append(height)
        except Exception as exc:
            result.failed.append((height, str(exc)))
        if i < len(heights) - 1:
            time.sleep(inter_block_delay)
    return result
