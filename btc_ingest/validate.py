"""Formal data-quality gate for a loaded block window.

Runs a fixed set of SQL checks against a DuckDB connection that already
has `blocks`, `transactions`, `inputs`, and `outputs` registered (as
views over real Parquet in production, or as tiny synthetic tables in
tests -- this module never touches the filesystem or the network itself).

Every check is scoped to `[start_height, end_height]` -- "loaded window"
always means exactly that range, not whatever else happens to be sitting
in data/parquet/ on disk.

Severity levels:
    PASS              -- invariant holds.
    EXPECTED_BOUNDARY -- not a defect; a necessary consequence of only a
                         bounded slice of the chain being loaded (foreign
                         input references, the first block's unverifiable
                         predecessor, in-window-unspent outputs,
                         addressless outputs).
    WARN              -- unusual, worth surfacing, but not proof of a
                         broken internal invariant.
    FAIL              -- a real internal inconsistency in data we do
                         control, or missing required data.

Only FAIL affects overall_status.
"""

import json
import math
from datetime import datetime, timezone

SAMPLE_LIMIT = 10


def _result(check_id: str, category: str, description: str, severity: str, details: dict) -> dict:
    return {
        "id": check_id,
        "category": category,
        "severity": severity,
        "passed": severity != "FAIL",
        "description": description,
        "details": details,
    }


def _rows(con, sql: str) -> list[dict]:
    result = con.sql(sql)
    fetched = result.fetchall()
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, row)) for row in fetched]


def _violation_check(con, check_id, category, description, sql, fail_severity="FAIL") -> dict:
    rows = _rows(con, sql)
    severity = fail_severity if rows else "PASS"
    return _result(
        check_id, category, description, severity,
        {"violation_count": len(rows), "samples": rows[:SAMPLE_LIMIT]},
    )


def _pct(numerator: int, denominator: int) -> float:
    return round(100.0 * numerator / denominator, 2) if denominator else 0.0


# ---------------------------------------------------------------------------
# Block completeness
# ---------------------------------------------------------------------------

def _check_heights_present(con, start, end):
    values = ", ".join(f"({h})" for h in range(start, end + 1))
    sql = f"""
        WITH expected(height) AS (VALUES {values})
        SELECT e.height AS missing_height
        FROM expected e
        LEFT JOIN blocks b ON b.block_height = e.height
        WHERE b.block_height IS NULL
        ORDER BY e.height
    """
    return _violation_check(
        con, "heights_present_exactly_once", "block_completeness",
        "Every requested height has a loaded block.", sql,
    )


def _check_no_duplicate_heights(con, start, end):
    sql = f"""
        SELECT block_height, count(*) AS occurrences
        FROM blocks WHERE block_height BETWEEN {start} AND {end}
        GROUP BY block_height HAVING count(*) > 1 ORDER BY block_height
    """
    return _violation_check(
        con, "no_duplicate_block_heights", "block_completeness",
        "No block height appears more than once.", sql,
    )


def _check_no_duplicate_hashes(con, start, end):
    sql = f"""
        SELECT block_hash, count(*) AS occurrences
        FROM blocks WHERE block_height BETWEEN {start} AND {end}
        GROUP BY block_hash HAVING count(*) > 1 ORDER BY block_hash
    """
    return _violation_check(
        con, "no_duplicate_block_hashes", "block_completeness",
        "No block hash appears more than once.", sql,
    )


def _check_tx_count_matches(con, start, end):
    sql = f"""
        SELECT b.block_height, b.transaction_count AS reported, count(t.txid) AS actual
        FROM blocks b
        LEFT JOIN transactions t ON t.block_height = b.block_height
            AND t.block_height BETWEEN {start} AND {end}
        WHERE b.block_height BETWEEN {start} AND {end}
        GROUP BY b.block_height, b.transaction_count
        HAVING count(t.txid) != b.transaction_count
        ORDER BY b.block_height
    """
    return _violation_check(
        con, "block_tx_count_matches_transactions", "block_completeness",
        "blocks.transaction_count equals actual loaded transaction rows.", sql,
    )


def _check_exactly_one_coinbase(con, start, end):
    sql = f"""
        SELECT block_height, count(*) FILTER (WHERE is_coinbase) AS coinbase_count
        FROM transactions WHERE block_height BETWEEN {start} AND {end}
        GROUP BY block_height
        HAVING count(*) FILTER (WHERE is_coinbase) != 1
        ORDER BY block_height
    """
    return _violation_check(
        con, "exactly_one_coinbase_per_block", "block_completeness",
        "Each block has exactly one coinbase transaction.", sql,
    )


def _check_block_height_hash_agree(con, start, end):
    sql = f"""
        SELECT t.txid, t.block_height, t.block_hash AS tx_block_hash, b.block_hash AS blocks_table_hash
        FROM transactions t
        JOIN blocks b ON b.block_height = t.block_height
        WHERE t.block_height BETWEEN {start} AND {end} AND t.block_hash != b.block_hash
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "block_height_hash_agree_with_transactions", "block_completeness",
        "Each transaction's block_hash matches the blocks table for its block_height.", sql,
    )


# ---------------------------------------------------------------------------
# Chain linkage
# ---------------------------------------------------------------------------

def _check_chain_linkage_internal(con, start, end):
    sql = f"""
        SELECT b.block_height, b.previous_block_hash, prior.block_hash AS expected_prior_hash
        FROM blocks b
        LEFT JOIN blocks prior ON prior.block_height = b.block_height - 1
        WHERE b.block_height BETWEEN {start + 1} AND {end}
          AND (prior.block_hash IS NULL OR b.previous_block_hash != prior.block_hash)
        ORDER BY b.block_height
    """
    return _violation_check(
        con, "chain_linkage_internal", "chain_linkage",
        "Each block after the first links to the prior loaded block's hash.", sql,
    )


def _check_first_block_unverifiable(con, start, end):
    rows = _rows(con, f"SELECT block_height, previous_block_hash FROM blocks WHERE block_height = {start}")
    return _result(
        "first_block_predecessor_unverifiable", "chain_linkage",
        "The first loaded block's predecessor is outside the window and cannot be verified.",
        "EXPECTED_BOUNDARY",
        {"first_block": rows[0] if rows else None},
    )


# ---------------------------------------------------------------------------
# Transaction integrity
# ---------------------------------------------------------------------------

def _check_txid_unique(con, start, end):
    sql = f"""
        SELECT txid, count(*) AS occurrences
        FROM transactions WHERE block_height BETWEEN {start} AND {end}
        GROUP BY txid HAVING count(*) > 1 ORDER BY txid LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(con, "txid_unique", "transaction_integrity", "No txid repeats in the loaded window.", sql)


def _check_tx_references_loaded_block(con, start, end):
    sql = f"""
        SELECT t.txid, t.block_height
        FROM transactions t
        LEFT JOIN blocks b ON b.block_height = t.block_height
        WHERE t.block_height BETWEEN {start} AND {end} AND b.block_height IS NULL
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "transactions_reference_loaded_block", "transaction_integrity",
        "Every transaction's block_height corresponds to a loaded block.", sql,
    )


def _check_tx_index_nonnegative(con, start, end):
    sql = f"""
        SELECT txid, block_height, transaction_index
        FROM transactions WHERE block_height BETWEEN {start} AND {end} AND transaction_index < 0
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "transaction_index_nonnegative", "transaction_integrity",
        "transaction_index is never negative.", sql,
    )


def _check_tx_index_unique_per_block(con, start, end):
    sql = f"""
        SELECT block_height, transaction_index, count(*) AS occurrences
        FROM transactions WHERE block_height BETWEEN {start} AND {end}
        GROUP BY block_height, transaction_index HAVING count(*) > 1
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "transaction_index_unique_per_block", "transaction_integrity",
        "transaction_index is unique within each block.", sql,
    )


def _check_tx_has_input_and_output(con, start, end):
    missing_inputs = _rows(con, f"""
        SELECT t.txid FROM transactions t
        LEFT JOIN inputs i ON i.txid = t.txid AND i.block_height BETWEEN {start} AND {end}
        WHERE t.block_height BETWEEN {start} AND {end}
        GROUP BY t.txid HAVING count(i.input_index) = 0 LIMIT {SAMPLE_LIMIT}
    """)
    missing_outputs = _rows(con, f"""
        SELECT t.txid FROM transactions t
        LEFT JOIN outputs o ON o.txid = t.txid AND o.block_height BETWEEN {start} AND {end}
        WHERE t.block_height BETWEEN {start} AND {end}
        GROUP BY t.txid HAVING count(o.output_index) = 0 LIMIT {SAMPLE_LIMIT}
    """)
    violation_count = len(missing_inputs) + len(missing_outputs)
    return _result(
        "each_transaction_has_input_and_output", "transaction_integrity",
        "Every transaction has at least one input and one output.",
        "FAIL" if violation_count else "PASS",
        {"txids_missing_inputs": missing_inputs, "txids_missing_outputs": missing_outputs},
    )


def _check_coinbase_is_first_in_block(con, start, end):
    sql = f"""
        SELECT block_height, txid, transaction_index
        FROM transactions
        WHERE block_height BETWEEN {start} AND {end} AND is_coinbase AND transaction_index != 0
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "coinbase_is_first_transaction_in_block", "transaction_integrity",
        "The coinbase transaction is conventionally transaction_index 0.", sql, fail_severity="WARN",
    )


def _check_ordinary_tx_no_coinbase_input(con, start, end):
    sql = f"""
        SELECT DISTINCT t.txid
        FROM transactions t
        JOIN inputs i ON i.txid = t.txid AND i.block_height BETWEEN {start} AND {end}
        WHERE t.block_height BETWEEN {start} AND {end} AND NOT t.is_coinbase AND i.is_coinbase
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "ordinary_transactions_no_coinbase_input", "transaction_integrity",
        "A transaction not flagged as coinbase has no coinbase-style input.", sql,
    )


# ---------------------------------------------------------------------------
# Input integrity
# ---------------------------------------------------------------------------

def _check_input_index_unique(con, start, end):
    sql = f"""
        SELECT txid, input_index, count(*) AS occurrences
        FROM inputs WHERE block_height BETWEEN {start} AND {end}
        GROUP BY txid, input_index HAVING count(*) > 1 LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(con, "input_index_unique_per_transaction", "input_integrity", "input_index is unique within each transaction.", sql)


def _check_ordinary_inputs_have_prev_ref(con, start, end):
    sql = f"""
        SELECT txid, input_index
        FROM inputs
        WHERE block_height BETWEEN {start} AND {end} AND NOT is_coinbase
          AND (previous_txid IS NULL OR previous_vout_index IS NULL)
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "ordinary_inputs_have_previous_reference", "input_integrity",
        "Non-coinbase inputs carry a previous_txid and previous_vout_index.", sql,
    )


def _check_coinbase_prevout_null(con, start, end):
    sql = f"""
        SELECT txid, input_index, previous_output_value_sats
        FROM inputs
        WHERE block_height BETWEEN {start} AND {end} AND is_coinbase
          AND previous_output_value_sats IS NOT NULL
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "coinbase_inputs_have_no_previous_output", "input_integrity",
        "Coinbase inputs are expected to have no real previous output (prevout is null).", sql,
        fail_severity="WARN",
    )


def _check_prev_output_value_nonnegative(con, start, end):
    sql = f"""
        SELECT txid, input_index, previous_output_value_sats
        FROM inputs
        WHERE block_height BETWEEN {start} AND {end}
          AND previous_output_value_sats IS NOT NULL AND previous_output_value_sats < 0
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "previous_output_value_nonnegative", "input_integrity",
        "previous_output_value_sats is never negative when present.", sql,
    )


def _classify_ordinary_inputs_sql(start, end):
    return f"""
        WITH ordinary_inputs AS (
            SELECT txid, input_index, previous_txid, previous_vout_index
            FROM inputs WHERE block_height BETWEEN {start} AND {end} AND NOT is_coinbase
        )
        SELECT oi.*,
            EXISTS (
                SELECT 1 FROM transactions t
                WHERE t.txid = oi.previous_txid AND t.block_height BETWEEN {start} AND {end}
            ) AS is_internal
        FROM ordinary_inputs oi
    """


def _check_foreign_inputs_expected_boundary(con, start, end):
    classify_sql = _classify_ordinary_inputs_sql(start, end)
    totals = _rows(con, f"""
        SELECT count(*) AS total, count(*) FILTER (WHERE is_internal) AS internal_count,
               count(*) FILTER (WHERE NOT is_internal) AS foreign_count
        FROM ({classify_sql})
    """)[0]
    samples = _rows(con, f"""
        SELECT txid, input_index, previous_txid FROM ({classify_sql})
        WHERE NOT is_internal LIMIT {SAMPLE_LIMIT}
    """)
    return _result(
        "foreign_input_references", "input_integrity",
        "Ordinary inputs referencing a transaction outside the loaded window are expected, not failures.",
        "EXPECTED_BOUNDARY",
        {
            "total_ordinary_inputs": totals["total"],
            "internal_count": totals["internal_count"],
            "foreign_count": totals["foreign_count"],
            "foreign_pct": _pct(totals["foreign_count"], totals["total"]),
            "samples": samples,
        },
    )


def _check_internal_inputs_resolve_uniquely(con, start, end):
    classify_sql = _classify_ordinary_inputs_sql(start, end)
    sql = f"""
        WITH internal_inputs AS (
            SELECT txid, input_index, previous_txid, previous_vout_index
            FROM ({classify_sql}) WHERE is_internal
        )
        SELECT ii.txid, ii.input_index, ii.previous_txid, ii.previous_vout_index,
               count(o.output_index) AS matching_outputs
        FROM internal_inputs ii
        LEFT JOIN outputs o ON o.txid = ii.previous_txid AND o.output_index = ii.previous_vout_index
            AND o.block_height BETWEEN {start} AND {end}
        GROUP BY ii.txid, ii.input_index, ii.previous_txid, ii.previous_vout_index
        HAVING count(o.output_index) != 1
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "internal_input_references_resolve_uniquely", "input_integrity",
        "Inputs referencing a transaction inside the window match exactly one loaded output.", sql,
    )


# ---------------------------------------------------------------------------
# Output integrity
# ---------------------------------------------------------------------------

def _check_output_index_unique(con, start, end):
    sql = f"""
        SELECT txid, output_index, count(*) AS occurrences
        FROM outputs WHERE block_height BETWEEN {start} AND {end}
        GROUP BY txid, output_index HAVING count(*) > 1 LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(con, "output_index_unique_per_transaction", "output_integrity", "output_index is unique within each transaction.", sql)


def _check_output_value_nonnegative(con, start, end):
    sql = f"""
        SELECT txid, output_index, value_sats
        FROM outputs WHERE block_height BETWEEN {start} AND {end} AND value_sats < 0
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(con, "output_value_nonnegative", "output_integrity", "value_sats is never negative.", sql)


def _check_outputs_without_address(con, start, end):
    totals = _rows(con, f"""
        SELECT count(*) AS total, count(*) FILTER (WHERE address IS NULL) AS no_address
        FROM outputs WHERE block_height BETWEEN {start} AND {end}
    """)[0]
    samples = _rows(con, f"""
        SELECT txid, output_index, script_type FROM outputs
        WHERE block_height BETWEEN {start} AND {end} AND address IS NULL LIMIT {SAMPLE_LIMIT}
    """)
    return _result(
        "outputs_without_standard_address", "output_integrity",
        "Outputs with non-standard scripts (e.g. OP_RETURN) legitimately have no address.",
        "EXPECTED_BOUNDARY",
        {
            "total_outputs": totals["total"],
            "no_address_count": totals["no_address"],
            "no_address_pct": _pct(totals["no_address"], totals["total"]),
            "samples": samples,
        },
    )


def _check_outputs_reference_valid_tx(con, start, end):
    sql = f"""
        SELECT o.txid, o.output_index
        FROM outputs o
        LEFT JOIN transactions t ON t.txid = o.txid AND t.block_height BETWEEN {start} AND {end}
        WHERE o.block_height BETWEEN {start} AND {end} AND t.txid IS NULL
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "outputs_reference_valid_loaded_transaction", "output_integrity",
        "Every output's txid corresponds to a loaded transaction.", sql,
    )


# ---------------------------------------------------------------------------
# Monetary consistency
# ---------------------------------------------------------------------------

def _fee_arithmetic_ctes(start, end):
    return f"""
        WITH input_sums AS (
            SELECT txid, sum(previous_output_value_sats) AS total_input_value,
                   count(*) FILTER (WHERE previous_output_value_sats IS NULL) AS null_input_values
            FROM inputs WHERE block_height BETWEEN {start} AND {end} AND NOT is_coinbase
            GROUP BY txid
        ),
        output_sums AS (
            SELECT txid, sum(value_sats) AS total_output_value
            FROM outputs WHERE block_height BETWEEN {start} AND {end}
            GROUP BY txid
        )
    """


def _check_fee_arithmetic(con, start, end):
    ctes = _fee_arithmetic_ctes(start, end)
    coverage = _rows(con, f"""
        {ctes}
        SELECT count(*) AS total_ordinary, count(*) FILTER (WHERE i.null_input_values = 0) AS checkable
        FROM transactions t
        JOIN input_sums i ON i.txid = t.txid
        WHERE t.block_height BETWEEN {start} AND {end} AND NOT t.is_coinbase
    """)[0]
    mismatches = _rows(con, f"""
        {ctes}
        SELECT t.txid, t.fee_sats AS reported_fee,
               (i.total_input_value - o.total_output_value) AS derived_fee
        FROM transactions t
        JOIN input_sums i ON i.txid = t.txid
        JOIN output_sums o ON o.txid = t.txid
        WHERE t.block_height BETWEEN {start} AND {end} AND NOT t.is_coinbase
          AND i.null_input_values = 0
          AND (i.total_input_value - o.total_output_value) != t.fee_sats
        LIMIT {SAMPLE_LIMIT}
    """)
    return _result(
        "fee_arithmetic_matches", "monetary_consistency",
        "fee_sats equals sum(previous output values) minus sum(output values) for checkable ordinary transactions.",
        "FAIL" if mismatches else "PASS",
        {
            "total_ordinary_transactions": coverage["total_ordinary"],
            "checkable_transactions": coverage["checkable"],
            "violation_count": len(mismatches),
            "samples": mismatches,
        },
    )


def _check_fees_nonnegative(con, start, end):
    sql = f"""
        SELECT txid, fee_sats FROM transactions
        WHERE block_height BETWEEN {start} AND {end} AND NOT is_coinbase AND fee_sats < 0
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(con, "fees_nonnegative", "monetary_consistency", "Ordinary transaction fees are never negative.", sql)


def _check_coinbase_fee_zero(con, start, end):
    sql = f"""
        SELECT txid, fee_sats FROM transactions
        WHERE block_height BETWEEN {start} AND {end} AND is_coinbase AND fee_sats != 0
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(
        con, "coinbase_fee_is_zero", "monetary_consistency",
        "Coinbase transactions are conventionally reported with fee_sats = 0.", sql, fail_severity="WARN",
    )


# ---------------------------------------------------------------------------
# Size, weight, vsize
# ---------------------------------------------------------------------------

def _check_size_positive(con, start, end):
    sql = f"SELECT txid, size_bytes FROM transactions WHERE block_height BETWEEN {start} AND {end} AND size_bytes <= 0 LIMIT {SAMPLE_LIMIT}"
    return _violation_check(con, "size_positive", "size_weight_vsize", "size_bytes is always positive.", sql)


def _check_weight_positive(con, start, end):
    sql = f"SELECT txid, weight_units FROM transactions WHERE block_height BETWEEN {start} AND {end} AND weight_units <= 0 LIMIT {SAMPLE_LIMIT}"
    return _violation_check(con, "weight_positive", "size_weight_vsize", "weight_units is always positive.", sql)


def _check_vsize_formula(con, start, end):
    sql = f"""
        SELECT txid, weight_units, vsize FROM transactions
        WHERE block_height BETWEEN {start} AND {end}
          AND vsize != CAST(ceil(weight_units / 4.0) AS BIGINT)
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(con, "vsize_matches_formula", "size_weight_vsize", "vsize equals ceil(weight_units / 4).", sql)


def _check_weight_bounded_by_size(con, start, end):
    sql = f"""
        SELECT txid, size_bytes, weight_units FROM transactions
        WHERE block_height BETWEEN {start} AND {end} AND weight_units > 4 * size_bytes
        LIMIT {SAMPLE_LIMIT}
    """
    return _violation_check(con, "weight_not_greater_than_four_times_size", "size_weight_vsize", "weight_units never exceeds 4 * size_bytes.", sql)


def _size_weight_vsize_summary(con, start, end):
    return _rows(con, f"""
        SELECT round(min(size_bytes),1) AS min_size, round(avg(size_bytes),1) AS avg_size, round(max(size_bytes),1) AS max_size,
               round(min(weight_units),1) AS min_weight, round(avg(weight_units),1) AS avg_weight, round(max(weight_units),1) AS max_weight,
               round(min(vsize),1) AS min_vsize, round(avg(vsize),1) AS avg_vsize, round(max(vsize),1) AS max_vsize
        FROM transactions WHERE block_height BETWEEN {start} AND {end}
    """)[0]


# ---------------------------------------------------------------------------
# Window-boundary metrics (informational, not per-check violations)
# ---------------------------------------------------------------------------

def _check_outputs_unspent_in_window(con, start, end):
    totals = _rows(con, f"""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE spent.txid IS NULL) AS unspent_in_window
        FROM outputs o
        LEFT JOIN inputs spent
            ON spent.previous_txid = o.txid AND spent.previous_vout_index = o.output_index
            AND spent.block_height BETWEEN {start} AND {end} AND NOT spent.is_coinbase
        WHERE o.block_height BETWEEN {start} AND {end}
    """)[0]
    return _result(
        "outputs_unspent_within_window", "window_boundary",
        "Outputs with no spending input observed inside the window are not proven unspent globally.",
        "EXPECTED_BOUNDARY",
        {
            "total_outputs": totals["total"],
            "unspent_in_window_count": totals["unspent_in_window"],
            "unspent_in_window_pct": _pct(totals["unspent_in_window"], totals["total"]),
        },
    )


def _boundary_metrics(checks: list[dict]) -> dict:
    by_id = {c["id"]: c for c in checks}
    foreign = by_id["foreign_input_references"]["details"]
    unspent = by_id["outputs_unspent_within_window"]["details"]
    no_address = by_id["outputs_without_standard_address"]["details"]
    first_block = by_id["first_block_predecessor_unverifiable"]["details"]["first_block"]
    return {
        "ordinary_inputs_referencing_before_window": {
            "count": foreign["foreign_count"], "pct": foreign["foreign_pct"],
        },
        "outputs_unspent_within_window": {
            "count": unspent["unspent_in_window_count"], "pct": unspent["unspent_in_window_pct"],
        },
        "outputs_without_standard_address": {
            "count": no_address["no_address_count"], "pct": no_address["no_address_pct"],
        },
        "first_block_lacks_loaded_predecessor": first_block,
    }


CHECKS = [
    _check_heights_present,
    _check_no_duplicate_heights,
    _check_no_duplicate_hashes,
    _check_tx_count_matches,
    _check_exactly_one_coinbase,
    _check_block_height_hash_agree,
    _check_chain_linkage_internal,
    _check_first_block_unverifiable,
    _check_txid_unique,
    _check_tx_references_loaded_block,
    _check_tx_index_nonnegative,
    _check_tx_index_unique_per_block,
    _check_tx_has_input_and_output,
    _check_coinbase_is_first_in_block,
    _check_ordinary_tx_no_coinbase_input,
    _check_input_index_unique,
    _check_ordinary_inputs_have_prev_ref,
    _check_coinbase_prevout_null,
    _check_prev_output_value_nonnegative,
    _check_foreign_inputs_expected_boundary,
    _check_internal_inputs_resolve_uniquely,
    _check_output_index_unique,
    _check_output_value_nonnegative,
    _check_outputs_without_address,
    _check_outputs_reference_valid_tx,
    _check_fee_arithmetic,
    _check_fees_nonnegative,
    _check_coinbase_fee_zero,
    _check_size_positive,
    _check_weight_positive,
    _check_vsize_formula,
    _check_weight_bounded_by_size,
    _check_outputs_unspent_in_window,
]


def run_validation(con, start_height: int, end_height: int) -> dict:
    """Run every check and assemble the full validation report."""
    checks = [check_fn(con, start_height, end_height) for check_fn in CHECKS]

    observed = _rows(con, f"""
        SELECT min(block_height) AS min_height, max(block_height) AS max_height, count(*) AS block_count
        FROM blocks WHERE block_height BETWEEN {start_height} AND {end_height}
    """)[0]

    severity_totals = {"PASS": 0, "EXPECTED_BOUNDARY": 0, "WARN": 0, "FAIL": 0}
    for check in checks:
        severity_totals[check["severity"]] += 1

    overall_status = "FAIL" if severity_totals["FAIL"] > 0 else "PASS"

    return {
        "requested_start_height": start_height,
        "requested_end_height": end_height,
        "observed_min_height": observed["min_height"],
        "observed_max_height": observed["max_height"],
        "expected_block_count": end_height - start_height + 1,
        "observed_block_count": observed["block_count"],
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "boundary_metrics": _boundary_metrics(checks),
        "summary_statistics": {"size_weight_vsize": _size_weight_vsize_summary(con, start_height, end_height)},
        "severity_totals": severity_totals,
        "overall_status": overall_status,
    }


def write_report_json(report: dict, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str))


def print_report(report: dict) -> None:
    print(f"Requested range: {report['requested_start_height']}-{report['requested_end_height']} "
          f"(expected {report['expected_block_count']} blocks)")
    print(f"Observed range: {report['observed_min_height']}-{report['observed_max_height']} "
          f"({report['observed_block_count']} blocks)")
    print(f"Validated at: {report['validated_at']}")
    print()
    for check in report["checks"]:
        marker = {"PASS": "PASS", "EXPECTED_BOUNDARY": "BOUNDARY", "WARN": "WARN", "FAIL": "FAIL"}[check["severity"]]
        print(f"[{marker:>8}] {check['id']}: {check['description']}")
        if check["severity"] == "FAIL":
            print(f"           -> {json.dumps(check['details'], default=str)[:300]}")
    print()
    print("Boundary metrics:")
    for key, value in report["boundary_metrics"].items():
        print(f"  {key}: {value}")
    print()
    print("Size/weight/vsize summary:", report["summary_statistics"]["size_weight_vsize"])
    print()
    totals = report["severity_totals"]
    print(f"Severity totals: PASS={totals['PASS']} EXPECTED_BOUNDARY={totals['EXPECTED_BOUNDARY']} "
          f"WARN={totals['WARN']} FAIL={totals['FAIL']}")
    print(f"OVERALL: {report['overall_status']}")
