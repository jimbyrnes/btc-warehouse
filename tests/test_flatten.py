import duckdb

from btc_ingest.flatten import (
    flatten_block,
    flatten_inputs,
    flatten_outputs,
    flatten_transactions,
    vsize_from_weight,
)

COINBASE_TX = {
    "txid": "coinbase_tx",
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
            "scriptsig": "abcd",
            "sequence": 4294967295,
            "is_coinbase": True,
        }
    ],
    "vout": [
        {
            "value": 625000000,
            "scriptpubkey_address": "bc1qminer",
            "scriptpubkey_type": "v0_p2wpkh",
            "scriptpubkey": "0014aaaa",
        }
    ],
}

ORDINARY_TX = {
    "txid": "ordinary_tx",
    "version": 2,
    "locktime": 0,
    "size": 250,
    "weight": 900,
    "fee": 500,
    "vin": [
        {
            "txid": "prev_tx",
            "vout": 0,
            "prevout": {
                "value": 100000,
                "scriptpubkey_address": "bc1qsender",
                "scriptpubkey_type": "v0_p2wpkh",
            },
            "scriptsig": "",
            "sequence": 4294967293,
            "is_coinbase": False,
        }
    ],
    "vout": [
        {
            "value": 99500,
            "scriptpubkey_address": "bc1qrecipient",
            "scriptpubkey_type": "v0_p2wpkh",
            "scriptpubkey": "0014bbbb",
        },
        {
            # OP_RETURN: no address -- a genuinely addressless output.
            "value": 0,
            "scriptpubkey_type": "op_return",
            "scriptpubkey": "6a04deadbeef",
        },
    ],
}


def test_vsize_from_weight_rounds_up():
    assert vsize_from_weight(400) == 100
    assert vsize_from_weight(401) == 101
    assert vsize_from_weight(1) == 1


def test_flatten_block_maps_expected_fields():
    block_json = {
        "id": "hash_b",
        "height": 100,
        "previousblockhash": "hash_a",
        "timestamp": 1700000000,
        "tx_count": 2,
        "size": 1000,
        "weight": 4000,
        "difficulty": 123.4,
        "merkle_root": "merkle",
        "nonce": 42,
        "bits": 386,
    }
    assert flatten_block(block_json) == {
        "block_height": 100,
        "block_hash": "hash_b",
        "previous_block_hash": "hash_a",
        "timestamp": 1700000000,
        "transaction_count": 2,
        "size_bytes": 1000,
        "weight_units": 4000,
        "difficulty": 123.4,
        "merkle_root": "merkle",
        "nonce": 42,
        "bits": 386,
    }


def test_flatten_transactions_marks_coinbase_indexes_and_computes_vsize():
    rows = flatten_transactions(100, "hash_b", [COINBASE_TX, ORDINARY_TX])

    assert rows[0]["is_coinbase"] is True
    assert rows[0]["transaction_index"] == 0
    assert rows[0]["vsize"] == vsize_from_weight(800)

    assert rows[1]["is_coinbase"] is False
    assert rows[1]["transaction_index"] == 1
    assert rows[1]["fee_sats"] == 500
    assert rows[1]["vsize"] == vsize_from_weight(900)


def test_flatten_inputs_handles_coinbase_null_prevout_without_inventing_values():
    row = flatten_inputs(100, [COINBASE_TX])[0]

    assert row["previous_txid"] == "0" * 64  # passed through unmodified, not nulled
    assert row["previous_vout_index"] == 4294967295
    assert row["previous_output_value_sats"] is None  # genuinely absent, not invented
    assert row["previous_output_address"] is None
    assert row["previous_output_script_type"] is None
    assert row["is_coinbase"] is True


def test_flatten_inputs_ordinary_input_carries_prevout_fields():
    row = flatten_inputs(100, [ORDINARY_TX])[0]

    assert row["previous_txid"] == "prev_tx"
    assert row["previous_output_value_sats"] == 100000
    assert row["previous_output_address"] == "bc1qsender"
    assert row["is_coinbase"] is False


def test_flatten_outputs_handles_missing_address_without_inventing_values():
    rows = flatten_outputs(100, [ORDINARY_TX])

    assert rows[0]["address"] == "bc1qrecipient"
    assert rows[1]["address"] is None  # OP_RETURN has no address -- genuinely absent
    assert rows[1]["script_type"] == "op_return"


def test_chain_linkage_query_detects_break_in_small_fixture():
    # Same self-join-via-lag pattern used in scripts/explore_block.py's
    # chain-linkage check, exercised here against a tiny synthetic fixture.
    con = duckdb.connect()

    linked = con.sql(
        """
        WITH blocks(height, block_hash, previous_block_hash) AS (
            VALUES (100, 'hash_a', NULL),
                   (101, 'hash_b', 'hash_a'),
                   (102, 'hash_c', 'hash_b')
        )
        SELECT height, previous_block_hash = lag(block_hash) OVER (ORDER BY height) AS linked_to_prior
        FROM blocks ORDER BY height
        """
    ).fetchall()
    assert linked[0][1] is None  # no prior block in this fixture's window -- expected
    assert linked[1][1] is True
    assert linked[2][1] is True

    broken = con.sql(
        """
        WITH blocks(height, block_hash, previous_block_hash) AS (
            VALUES (100, 'hash_a', NULL),
                   (101, 'hash_b', 'hash_a'),
                   (102, 'hash_c', 'WRONG_HASH')
        )
        SELECT height, previous_block_hash = lag(block_hash) OVER (ORDER BY height) AS linked_to_prior
        FROM blocks ORDER BY height
        """
    ).fetchall()
    assert broken[2][1] is False
