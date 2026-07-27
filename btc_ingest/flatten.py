"""Pure functions that flatten raw Esplora JSON into tabular rows.

No I/O here -- these are deterministic transformations from the raw
block/transaction shape into one row per block, transaction, input, and
output, so they're testable with small fixture dicts and no network or
DuckDB involved.

Fields that are genuinely absent in the source (e.g. `prevout` is null for
a coinbase input; many non-standard scripts have no address) are passed
through as None -- never invented or defaulted to a placeholder value.
"""

import math


def vsize_from_weight(weight: int) -> int:
    """vsize = ceil(weight / 4), the SegWit-discounted virtual size."""
    return math.ceil(weight / 4)


def flatten_block(block_json: dict) -> dict:
    return {
        "block_height": block_json["height"],
        "block_hash": block_json["id"],
        "previous_block_hash": block_json.get("previousblockhash"),
        "timestamp": block_json["timestamp"],
        "transaction_count": block_json["tx_count"],
        "size_bytes": block_json["size"],
        "weight_units": block_json["weight"],
        "difficulty": block_json["difficulty"],
        "merkle_root": block_json["merkle_root"],
        "nonce": block_json["nonce"],
        "bits": block_json["bits"],
    }


def flatten_transactions(block_height: int, block_hash: str, tx_objects: list[dict]) -> list[dict]:
    rows = []
    for index, tx in enumerate(tx_objects):
        weight = tx["weight"]
        vin = tx["vin"]
        rows.append(
            {
                "block_height": block_height,
                "block_hash": block_hash,
                "txid": tx["txid"],
                "transaction_index": index,
                "version": tx["version"],
                "locktime": tx["locktime"],
                "size_bytes": tx["size"],
                "weight_units": weight,
                "vsize": vsize_from_weight(weight),
                "fee_sats": tx.get("fee"),
                "is_coinbase": bool(vin) and bool(vin[0].get("is_coinbase", False)),
            }
        )
    return rows


def flatten_inputs(block_height: int, tx_objects: list[dict]) -> list[dict]:
    rows = []
    for tx in tx_objects:
        txid = tx["txid"]
        for index, vin in enumerate(tx["vin"]):
            prevout = vin.get("prevout")
            rows.append(
                {
                    "block_height": block_height,
                    "txid": txid,
                    "input_index": index,
                    "previous_txid": vin.get("txid"),
                    "previous_vout_index": vin.get("vout"),
                    "previous_output_value_sats": prevout["value"] if prevout else None,
                    "previous_output_address": prevout.get("scriptpubkey_address") if prevout else None,
                    "previous_output_script_type": prevout.get("scriptpubkey_type") if prevout else None,
                    "scriptsig": vin.get("scriptsig"),
                    "sequence": vin.get("sequence"),
                    "is_coinbase": bool(vin.get("is_coinbase", False)),
                }
            )
    return rows


def flatten_outputs(block_height: int, tx_objects: list[dict]) -> list[dict]:
    rows = []
    for tx in tx_objects:
        txid = tx["txid"]
        for index, vout in enumerate(tx["vout"]):
            rows.append(
                {
                    "block_height": block_height,
                    "txid": txid,
                    "output_index": index,
                    "value_sats": vout.get("value"),
                    "address": vout.get("scriptpubkey_address"),
                    "script_type": vout.get("scriptpubkey_type"),
                    "scriptpubkey": vout.get("scriptpubkey"),
                }
            )
    return rows
