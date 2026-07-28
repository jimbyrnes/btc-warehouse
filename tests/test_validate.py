import copy
import json
import tempfile
from pathlib import Path

import duckdb
import pytest

from btc_ingest.validate import run_validation, write_report_json

# A minimal, internally-consistent 3-block fixture (heights 100-102):
#   - each block: one coinbase tx (index 0) + one ordinary tx (index 1)
#   - block 100's ordinary tx spends a FOREIGN output (outside the window)
#   - blocks 101/102's ordinary tx spend the PRIOR block's coinbase output
#     (an INTERNAL, resolvable reference)
#   - block 100's ordinary tx has a second, addressless (OP_RETURN-style) output

_BLOCKS = [
    {"block_height": 100, "block_hash": "hash_100", "previous_block_hash": "hash_099", "transaction_count": 2},
    {"block_height": 101, "block_hash": "hash_101", "previous_block_hash": "hash_100", "transaction_count": 2},
    {"block_height": 102, "block_hash": "hash_102", "previous_block_hash": "hash_101", "transaction_count": 2},
]

_TRANSACTIONS = [
    {"block_height": 100, "block_hash": "hash_100", "txid": "cb100", "transaction_index": 0,
     "size_bytes": 300, "weight_units": 800, "vsize": 200, "fee_sats": 0, "is_coinbase": True},
    {"block_height": 100, "block_hash": "hash_100", "txid": "ord100", "transaction_index": 1,
     "size_bytes": 300, "weight_units": 900, "vsize": 225, "fee_sats": 100, "is_coinbase": False},
    {"block_height": 101, "block_hash": "hash_101", "txid": "cb101", "transaction_index": 0,
     "size_bytes": 300, "weight_units": 800, "vsize": 200, "fee_sats": 0, "is_coinbase": True},
    {"block_height": 101, "block_hash": "hash_101", "txid": "ord101", "transaction_index": 1,
     "size_bytes": 300, "weight_units": 900, "vsize": 225, "fee_sats": 150, "is_coinbase": False},
    {"block_height": 102, "block_hash": "hash_102", "txid": "cb102", "transaction_index": 0,
     "size_bytes": 300, "weight_units": 800, "vsize": 200, "fee_sats": 0, "is_coinbase": True},
    {"block_height": 102, "block_hash": "hash_102", "txid": "ord102", "transaction_index": 1,
     "size_bytes": 300, "weight_units": 900, "vsize": 225, "fee_sats": 200, "is_coinbase": False},
]

_INPUTS = [
    {"block_height": 100, "txid": "cb100", "input_index": 0, "previous_txid": "0" * 64,
     "previous_vout_index": 4294967295, "previous_output_value_sats": None, "is_coinbase": True},
    {"block_height": 100, "txid": "ord100", "input_index": 0, "previous_txid": "ext_tx_outside_window",
     "previous_vout_index": 0, "previous_output_value_sats": 100000, "is_coinbase": False},
    {"block_height": 101, "txid": "cb101", "input_index": 0, "previous_txid": "0" * 64,
     "previous_vout_index": 4294967295, "previous_output_value_sats": None, "is_coinbase": True},
    {"block_height": 101, "txid": "ord101", "input_index": 0, "previous_txid": "cb100",
     "previous_vout_index": 0, "previous_output_value_sats": 625000000, "is_coinbase": False},
    {"block_height": 102, "txid": "cb102", "input_index": 0, "previous_txid": "0" * 64,
     "previous_vout_index": 4294967295, "previous_output_value_sats": None, "is_coinbase": True},
    {"block_height": 102, "txid": "ord102", "input_index": 0, "previous_txid": "cb101",
     "previous_vout_index": 0, "previous_output_value_sats": 625000000, "is_coinbase": False},
]

_OUTPUTS = [
    {"block_height": 100, "txid": "cb100", "output_index": 0, "value_sats": 625000000, "address": "bc1qminer100", "script_type": "v0_p2wpkh"},
    {"block_height": 100, "txid": "ord100", "output_index": 0, "value_sats": 99900, "address": "bc1qrecipient100", "script_type": "v0_p2wpkh"},
    {"block_height": 100, "txid": "ord100", "output_index": 1, "value_sats": 0, "address": None, "script_type": "op_return"},
    {"block_height": 101, "txid": "cb101", "output_index": 0, "value_sats": 625000000, "address": "bc1qminer101", "script_type": "v0_p2wpkh"},
    {"block_height": 101, "txid": "ord101", "output_index": 0, "value_sats": 624999850, "address": "bc1qrecipient101", "script_type": "v0_p2wpkh"},
    {"block_height": 102, "txid": "cb102", "output_index": 0, "value_sats": 625000000, "address": "bc1qminer102", "script_type": "v0_p2wpkh"},
    {"block_height": 102, "txid": "ord102", "output_index": 0, "value_sats": 624999800, "address": "bc1qrecipient102", "script_type": "v0_p2wpkh"},
]


def _baseline():
    return copy.deepcopy(_BLOCKS), copy.deepcopy(_TRANSACTIONS), copy.deepcopy(_INPUTS), copy.deepcopy(_OUTPUTS)


def _load_table(con, table_name, rows):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        for row in rows:
            f.write(json.dumps(row))
            f.write("\n")
        path = f.name
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_ndjson_auto('{path}')")
    Path(path).unlink()


def _con(blocks, transactions, inputs, outputs):
    con = duckdb.connect()
    _load_table(con, "blocks", blocks)
    _load_table(con, "transactions", transactions)
    _load_table(con, "inputs", inputs)
    _load_table(con, "outputs", outputs)
    return con


def _checks_by_id(report):
    return {c["id"]: c for c in report["checks"]}


def test_baseline_range_passes_with_no_fail():
    con = _con(*_baseline())
    report = run_validation(con, 100, 102)

    assert report["overall_status"] == "PASS"
    assert report["severity_totals"]["FAIL"] == 0
    assert report["observed_block_count"] == 3
    assert report["expected_block_count"] == 3
    checks = _checks_by_id(report)
    assert checks["fee_arithmetic_matches"]["severity"] == "PASS"
    assert checks["vsize_matches_formula"]["severity"] == "PASS"
    assert checks["chain_linkage_internal"]["severity"] == "PASS"


def test_missing_height_fails():
    blocks, transactions, inputs, outputs = _baseline()
    blocks = [b for b in blocks if b["block_height"] != 101]

    con = _con(blocks, transactions, inputs, outputs)
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    assert checks["heights_present_exactly_once"]["severity"] == "FAIL"
    assert checks["heights_present_exactly_once"]["details"]["violation_count"] == 1
    assert report["overall_status"] == "FAIL"


def test_duplicate_height_fails():
    blocks, transactions, inputs, outputs = _baseline()
    blocks.append(copy.deepcopy(blocks[0]))  # duplicate height 100

    con = _con(blocks, transactions, inputs, outputs)
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    assert checks["no_duplicate_block_heights"]["severity"] == "FAIL"
    assert report["overall_status"] == "FAIL"


def test_chain_link_mismatch_fails():
    blocks, transactions, inputs, outputs = _baseline()
    for b in blocks:
        if b["block_height"] == 102:
            b["previous_block_hash"] = "wrong_hash"

    con = _con(blocks, transactions, inputs, outputs)
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    assert checks["chain_linkage_internal"]["severity"] == "FAIL"
    assert checks["chain_linkage_internal"]["details"]["samples"][0]["block_height"] == 102
    assert report["overall_status"] == "FAIL"


def test_transaction_count_mismatch_fails():
    blocks, transactions, inputs, outputs = _baseline()
    for b in blocks:
        if b["block_height"] == 100:
            b["transaction_count"] = 99

    con = _con(blocks, transactions, inputs, outputs)
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    assert checks["block_tx_count_matches_transactions"]["severity"] == "FAIL"
    assert report["overall_status"] == "FAIL"


def test_incorrect_coinbase_count_fails():
    blocks, transactions, inputs, outputs = _baseline()
    extra_coinbase = {
        "block_height": 100, "block_hash": "hash_100", "txid": "cb100_extra", "transaction_index": 2,
        "size_bytes": 300, "weight_units": 800, "vsize": 200, "fee_sats": 0, "is_coinbase": True,
    }
    transactions.append(extra_coinbase)
    inputs.append({"block_height": 100, "txid": "cb100_extra", "input_index": 0, "previous_txid": "0" * 64,
                    "previous_vout_index": 4294967295, "previous_output_value_sats": None, "is_coinbase": True})
    outputs.append({"block_height": 100, "txid": "cb100_extra", "output_index": 0, "value_sats": 100,
                     "address": "bc1qextra", "script_type": "v0_p2wpkh"})

    con = _con(blocks, transactions, inputs, outputs)
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    assert checks["exactly_one_coinbase_per_block"]["severity"] == "FAIL"
    assert report["overall_status"] == "FAIL"


def test_unresolved_internal_reference_fails():
    blocks, transactions, inputs, outputs = _baseline()
    for i in inputs:
        if i["txid"] == "ord101":
            i["previous_vout_index"] = 5  # cb100 has no output index 5

    con = _con(blocks, transactions, inputs, outputs)
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    assert checks["internal_input_references_resolve_uniquely"]["severity"] == "FAIL"
    assert report["overall_status"] == "FAIL"


def test_foreign_reference_is_expected_boundary_not_failure():
    con = _con(*_baseline())
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    foreign = checks["foreign_input_references"]
    assert foreign["severity"] == "EXPECTED_BOUNDARY"
    assert foreign["details"]["foreign_count"] == 1  # ord100's input
    assert foreign["details"]["internal_count"] == 2  # ord101, ord102
    assert report["overall_status"] == "PASS"  # boundary exceptions never fail the gate
    assert report["boundary_metrics"]["ordinary_inputs_referencing_before_window"]["count"] == 1


def test_fee_mismatch_fails():
    blocks, transactions, inputs, outputs = _baseline()
    for t in transactions:
        if t["txid"] == "ord101":
            t["fee_sats"] = 99999  # actual derived fee is 150

    con = _con(blocks, transactions, inputs, outputs)
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    fee_check = checks["fee_arithmetic_matches"]
    assert fee_check["severity"] == "FAIL"
    assert fee_check["details"]["samples"][0]["txid"] == "ord101"
    assert report["overall_status"] == "FAIL"


def test_fee_arithmetic_correct_in_baseline():
    con = _con(*_baseline())
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    fee_check = checks["fee_arithmetic_matches"]
    assert fee_check["severity"] == "PASS"
    # all 3 ordinary transactions have known previous_output_value_sats (foreign refs
    # still carry prevout data from the API, matching real Esplora behavior)
    assert fee_check["details"]["checkable_transactions"] == 3
    assert fee_check["details"]["violation_count"] == 0


def test_vsize_formula_violation_fails():
    blocks, transactions, inputs, outputs = _baseline()
    for t in transactions:
        if t["txid"] == "cb100":
            t["vsize"] = 999  # should be ceil(800/4) = 200

    con = _con(blocks, transactions, inputs, outputs)
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    assert checks["vsize_matches_formula"]["severity"] == "FAIL"
    assert report["overall_status"] == "FAIL"


def test_nonstandard_output_without_address_is_allowed():
    con = _con(*_baseline())
    report = run_validation(con, 100, 102)

    checks = _checks_by_id(report)
    no_address = checks["outputs_without_standard_address"]
    assert no_address["severity"] == "EXPECTED_BOUNDARY"
    assert no_address["details"]["no_address_count"] == 1  # ord100's OP_RETURN output
    assert report["overall_status"] == "PASS"


def test_json_report_generation(tmp_path):
    con = _con(*_baseline())
    report = run_validation(con, 100, 102)

    report_path = tmp_path / "reports" / "validation_100-102.json"
    write_report_json(report, report_path)

    assert report_path.exists()
    loaded = json.loads(report_path.read_text())
    assert loaded["overall_status"] == "PASS"
    assert loaded["requested_start_height"] == 100
    assert loaded["requested_end_height"] == 102
    assert len(loaded["checks"]) == len(report["checks"])


def test_overall_status_exit_code_semantics():
    # PASS fixture
    con = _con(*_baseline())
    assert run_validation(con, 100, 102)["overall_status"] == "PASS"

    # FAIL fixture (duplicate height)
    blocks, transactions, inputs, outputs = _baseline()
    blocks.append(copy.deepcopy(blocks[0]))
    con2 = _con(blocks, transactions, inputs, outputs)
    report2 = run_validation(con2, 100, 102)
    assert report2["overall_status"] == "FAIL"
    totals = report2["severity_totals"]
    assert totals["PASS"] + totals["EXPECTED_BOUNDARY"] + totals["WARN"] + totals["FAIL"] == len(report2["checks"])
