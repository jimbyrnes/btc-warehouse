import json

import pytest

from btc_ingest.snowflake_loader import (
    DATASET_SCHEMAS,
    SnowflakeConfig,
    SnowflakeConfigError,
    ValidationNotPassedError,
    generate_copy_into_sql,
    generate_create_temp_table_sql,
    generate_merge_sql,
    generate_put_sql,
    load_height_range,
    load_validation_report,
)

ALL_ENV_VARS = {
    "SNOWFLAKE_ACCOUNT": "abc12345",
    "SNOWFLAKE_USER": "test_user",
    "SNOWFLAKE_PASSWORD": "hunter2",
    "SNOWFLAKE_ROLE": "SYSADMIN",
    "SNOWFLAKE_WAREHOUSE": "BTC_WAREHOUSE_WH",
    "SNOWFLAKE_DATABASE": "BTC_WAREHOUSE",
    "SNOWFLAKE_SCHEMA": "RAW",
}


def _clear_snowflake_env(monkeypatch):
    for name in ALL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


class FakeCursor:
    def __init__(self, log: list, fail_substrings: set):
        self.log = log
        self.fail_substrings = fail_substrings

    def execute(self, sql):
        self.log.append(sql)
        if any(s in sql for s in self.fail_substrings):
            raise RuntimeError("simulated Snowflake failure")
        return self


class FakeConnection:
    def __init__(self, fail_substrings=None):
        self.log: list[str] = []
        self.fail_substrings = fail_substrings or set()

    def cursor(self):
        return FakeCursor(self.log, self.fail_substrings)

    def close(self):
        pass


def _write_report(reports_root, start, end, status="PASS"):
    reports_root.mkdir(parents=True, exist_ok=True)
    path = reports_root / f"validation_{start}-{end}.json"
    path.write_text(json.dumps({"overall_status": status}))
    return path


def _touch_partition(parquet_root, dataset, height):
    p = parquet_root / dataset / f"block_height={height}" / "data.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"")
    return p


# --- SnowflakeConfig ---------------------------------------------------------

def test_config_from_env_missing_vars_raises_clear_error(monkeypatch):
    _clear_snowflake_env(monkeypatch)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "abc12345")  # only one of seven set

    with pytest.raises(SnowflakeConfigError) as exc_info:
        SnowflakeConfig.from_env()

    message = str(exc_info.value)
    assert "SNOWFLAKE_USER" in message
    assert "SNOWFLAKE_PASSWORD" in message
    assert "SNOWFLAKE_ACCOUNT" not in message  # that one was set, shouldn't be listed missing


def test_config_from_env_success(monkeypatch):
    _clear_snowflake_env(monkeypatch)
    for name, value in ALL_ENV_VARS.items():
        monkeypatch.setenv(name, value)

    config = SnowflakeConfig.from_env()

    assert config.account == "abc12345"
    assert config.database == "BTC_WAREHOUSE"
    assert config.warehouse == "BTC_WAREHOUSE_WH"


# --- SQL generation -----------------------------------------------------------

def test_generate_put_sql_mirrors_local_layout(tmp_path):
    local_path = tmp_path / "data.parquet"
    sql = generate_put_sql(local_path, "blocks/block_height=959744")

    assert "PUT" in sql
    assert str(local_path) in sql
    assert "@RAW.PARQUET_STAGE/blocks/block_height=959744/" in sql
    assert "OVERWRITE=TRUE" in sql


def test_generate_create_temp_table_sql_likes_the_real_table():
    sql = generate_create_temp_table_sql("blocks", "_STG_BLOCKS_959744")
    assert "TEMPORARY TABLE _STG_BLOCKS_959744" in sql
    assert "LIKE RAW.BLOCKS" in sql


def test_generate_copy_into_sql_uses_match_by_column_name():
    sql = generate_copy_into_sql("transactions", "_STG_TRANSACTIONS_959744", "transactions/block_height=959744")
    assert "COPY INTO _STG_TRANSACTIONS_959744" in sql
    assert "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE" in sql
    assert "@RAW.PARQUET_STAGE/transactions/block_height=959744/" in sql


def test_generate_merge_sql_blocks_uses_composite_natural_key():
    sql = generate_merge_sql("blocks", "_STG_BLOCKS_959744", "data.parquet")
    assert "MERGE INTO RAW.BLOCKS AS tgt" in sql
    assert "tgt.BLOCK_HEIGHT = stg.BLOCK_HEIGHT" in sql
    assert "tgt.BLOCK_HASH = stg.BLOCK_HASH" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_generate_merge_sql_inputs_quotes_reserved_sequence_column():
    sql = generate_merge_sql("inputs", "_STG_TRANSACTION_INPUTS_959744", "data.parquet")
    assert '"SEQUENCE"' in sql
    assert "tgt.TXID = stg.TXID" in sql
    assert "tgt.INPUT_INDEX = stg.INPUT_INDEX" in sql


def test_all_datasets_have_matching_merge_and_copy_sql():
    for dataset in DATASET_SCHEMAS:
        merge_sql = generate_merge_sql(dataset, "tmp_table", "f.parquet")
        assert "MERGE INTO" in merge_sql
        copy_sql = generate_copy_into_sql(dataset, "tmp_table", f"{dataset}/block_height=1")
        assert "COPY INTO tmp_table" in copy_sql


# --- Validation-report gate ----------------------------------------------------

def test_load_validation_report_missing_file_raises(tmp_path):
    with pytest.raises(ValidationNotPassedError, match="No validation report found"):
        load_validation_report(tmp_path, 959744, 959768)


def test_load_validation_report_fail_status_raises(tmp_path):
    _write_report(tmp_path, 959744, 959768, status="FAIL")

    with pytest.raises(ValidationNotPassedError, match="not PASS"):
        load_validation_report(tmp_path, 959744, 959768)


def test_load_validation_report_pass_returns_report(tmp_path):
    _write_report(tmp_path, 959744, 959768, status="PASS")

    report = load_validation_report(tmp_path, 959744, 959768)

    assert report["overall_status"] == "PASS"


# --- load_height_range orchestration (mocked connection) -----------------------

def test_load_height_range_refuses_without_passing_report(tmp_path):
    parquet_root = tmp_path / "parquet"
    reports_root = tmp_path / "reports"  # deliberately empty
    con = FakeConnection()

    with pytest.raises(ValidationNotPassedError):
        load_height_range(con, parquet_root, reports_root, 100, 100)

    assert con.log == []  # never touched the connection


def test_load_height_range_issues_put_copy_merge_per_partition(tmp_path):
    parquet_root = tmp_path / "parquet"
    reports_root = tmp_path / "reports"
    _write_report(reports_root, 100, 100, status="PASS")
    for dataset in DATASET_SCHEMAS:
        _touch_partition(parquet_root, dataset, 100)

    con = FakeConnection()
    result = load_height_range(con, parquet_root, reports_root, 100, 100)

    assert result.is_complete
    assert sorted(result.loaded) == sorted((100, d) for d in DATASET_SCHEMAS)
    assert result.failed == []

    joined_log = "\n".join(con.log)
    assert joined_log.count("PUT ") == 4
    assert joined_log.count("COPY INTO") == 4
    assert joined_log.count("MERGE INTO") == 4
    assert joined_log.count("DROP TABLE IF EXISTS") == 4


def test_load_height_range_missing_partition_recorded_not_crashed(tmp_path):
    parquet_root = tmp_path / "parquet"
    reports_root = tmp_path / "reports"
    _write_report(reports_root, 100, 100, status="PASS")
    for dataset in DATASET_SCHEMAS:
        if dataset != "outputs":
            _touch_partition(parquet_root, dataset, 100)
    # "outputs" partition deliberately left missing

    con = FakeConnection()
    result = load_height_range(con, parquet_root, reports_root, 100, 100)

    assert len(result.failed) == 1
    height, dataset, error = result.failed[0]
    assert (height, dataset) == (100, "outputs")
    assert "missing local partition" in error
    assert len(result.loaded) == 3  # the other three still succeeded


def test_load_height_range_one_dataset_failure_does_not_stop_others(tmp_path):
    parquet_root = tmp_path / "parquet"
    reports_root = tmp_path / "reports"
    _write_report(reports_root, 100, 100, status="PASS")
    for dataset in DATASET_SCHEMAS:
        _touch_partition(parquet_root, dataset, 100)

    con = FakeConnection(fail_substrings={"RAW.TRANSACTION_INPUTS"})
    result = load_height_range(con, parquet_root, reports_root, 100, 100)

    assert result.failed == [(100, "inputs", "simulated Snowflake failure")]
    assert sorted(result.loaded) == sorted((100, d) for d in DATASET_SCHEMAS if d != "inputs")
