"""Load validated local Parquet partitions into Snowflake RAW tables.

Architecture: validated local Parquet -> PUT to an internal stage -> COPY
INTO a session-scoped TEMPORARY table -> MERGE into the permanent RAW
table on a natural key. The MERGE step is what makes reloading the same
block-height partition safe -- COPY INTO alone only prevents loading the
exact same file twice, which wouldn't help after a `--force` re-fetch
produces new file bytes for the same logical partition.

This module never touches the network or the filesystem's real data
except through the `con` object and `parquet_root` passed in by the
caller -- both are dependency-injected specifically so tests can supply a
fake connection and a tmp_path directory, with no live Snowflake account
or real Parquet files required.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Column names that need double-quoting in generated SQL because they
# collide with Snowflake reserved words if left bare.
_QUOTED_COLUMNS = {"timestamp", "sequence"}


def _col(name: str) -> str:
    return f'"{name.upper()}"' if name in _QUOTED_COLUMNS else name.upper()


# Single source of truth for each dataset's Snowflake table, natural key,
# and Bitcoin-data column list (metadata columns LOADED_AT/SOURCE_FILE_NAME
# are handled separately -- they aren't part of the source Parquet).
# Column names/order here must match btc_ingest/flatten.py and
# snowflake/setup.sql's CREATE TABLE statements.
DATASET_SCHEMAS: dict[str, dict[str, Any]] = {
    "blocks": {
        "table": "RAW.BLOCKS",
        "key_columns": ("block_height", "block_hash"),
        "columns": (
            "block_height", "block_hash", "previous_block_hash", "timestamp",
            "transaction_count", "size_bytes", "weight_units", "difficulty",
            "merkle_root", "nonce", "bits",
        ),
    },
    "transactions": {
        "table": "RAW.TRANSACTIONS",
        "key_columns": ("txid",),
        "columns": (
            "block_height", "block_hash", "txid", "transaction_index", "version",
            "locktime", "size_bytes", "weight_units", "vsize", "fee_sats", "is_coinbase",
        ),
    },
    "inputs": {
        "table": "RAW.TRANSACTION_INPUTS",
        "key_columns": ("txid", "input_index"),
        "columns": (
            "block_height", "txid", "input_index", "previous_txid", "previous_vout_index",
            "previous_output_value_sats", "previous_output_address",
            "previous_output_script_type", "scriptsig", "sequence", "is_coinbase",
        ),
    },
    "outputs": {
        "table": "RAW.TRANSACTION_OUTPUTS",
        "key_columns": ("txid", "output_index"),
        "columns": (
            "block_height", "txid", "output_index", "value_sats", "address",
            "script_type", "scriptpubkey",
        ),
    },
}


class ValidationNotPassedError(Exception):
    """Raised when a load is attempted without a passing quality-gate report."""


def load_validation_report(reports_root: Path, start_height: int, end_height: int) -> dict:
    """Load and check the Milestone 3 validation report for this exact range.

    Refuses to hand back a report (raising instead) unless one exists and
    its overall_status is PASS -- this is the enforcement point for
    "never load a dataset that hasn't cleared the quality gate."
    """
    report_path = Path(reports_root) / f"validation_{start_height}-{end_height}.json"
    if not report_path.exists():
        raise ValidationNotPassedError(
            f"No validation report found at {report_path}. Run "
            f"'python scripts/validate_dataset.py --start-height {start_height} "
            f"--end-height {end_height}' first."
        )
    report = json.loads(report_path.read_text())
    if report.get("overall_status") != "PASS":
        raise ValidationNotPassedError(
            f"Validation report at {report_path} has overall_status="
            f"{report.get('overall_status')!r}, not PASS. Refusing to load a "
            f"dataset that has not cleared the Milestone 3 quality gate."
        )
    return report


class SnowflakeConfigError(Exception):
    """Raised when required Snowflake environment variables are missing."""


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str

    _ENV_VARS = {
        "account": "SNOWFLAKE_ACCOUNT",
        "user": "SNOWFLAKE_USER",
        "password": "SNOWFLAKE_PASSWORD",
        "role": "SNOWFLAKE_ROLE",
        "warehouse": "SNOWFLAKE_WAREHOUSE",
        "database": "SNOWFLAKE_DATABASE",
        "schema": "SNOWFLAKE_SCHEMA",
    }

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        missing = [env_name for env_name in cls._ENV_VARS.values() if not os.environ.get(env_name)]
        if missing:
            raise SnowflakeConfigError(
                "Missing required Snowflake environment variable(s): "
                + ", ".join(missing)
                + ". Copy .env.example to .env and fill in real values, "
                + "then `set -a && source .env && set +a` before running this command."
            )
        return cls(**{field_name: os.environ[env_name] for field_name, env_name in cls._ENV_VARS.items()})


def build_connection(config: SnowflakeConfig):
    """Open a real Snowflake connection. Requires the snowflake-connector-python
    package and a live, reachable Snowflake account -- never called in tests."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=config.account,
        user=config.user,
        password=config.password,
        role=config.role,
        warehouse=config.warehouse,
        database=config.database,
        schema=config.schema,
    )


def generate_put_sql(local_path: Path, stage_subpath: str) -> str:
    """PUT one local Parquet file to a stage path mirroring the local
    partition layout: @RAW.PARQUET_STAGE/<dataset>/block_height=<height>/."""
    return (
        f"PUT 'file://{local_path}' @RAW.PARQUET_STAGE/{stage_subpath}/ "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )


def generate_create_temp_table_sql(dataset: str, temp_table: str) -> str:
    table = DATASET_SCHEMAS[dataset]["table"]
    return f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} LIKE {table}"


def generate_copy_into_sql(dataset: str, temp_table: str, stage_subpath: str) -> str:
    return (
        f"COPY INTO {temp_table} "
        f"FROM @RAW.PARQUET_STAGE/{stage_subpath}/ "
        f"FILE_FORMAT=(FORMAT_NAME=RAW.PARQUET_FORMAT) "
        f"MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
    )


def generate_merge_sql(dataset: str, temp_table: str, source_file_name: str) -> str:
    """Idempotent upsert: update on natural-key match, insert otherwise.

    This -- not COPY INTO's own file-level load history -- is what makes
    reloading the same block-height partition safe after a `--force`
    re-fetch changes the underlying file bytes for the same logical rows.
    """
    schema = DATASET_SCHEMAS[dataset]
    key_columns = schema["key_columns"]
    all_columns = schema["columns"]
    non_key_columns = [c for c in all_columns if c not in key_columns]

    on_clause = " AND ".join(f"tgt.{_col(k)} = stg.{_col(k)}" for k in key_columns)
    update_assignments = [f"tgt.{_col(c)} = stg.{_col(c)}" for c in non_key_columns]
    update_assignments += [
        "tgt.LOADED_AT = CURRENT_TIMESTAMP()",
        f"tgt.SOURCE_FILE_NAME = '{source_file_name}'",
    ]
    insert_columns = [_col(c) for c in all_columns] + ["LOADED_AT", "SOURCE_FILE_NAME"]
    insert_values = [f"stg.{_col(c)}" for c in all_columns] + [
        "CURRENT_TIMESTAMP()",
        f"'{source_file_name}'",
    ]

    return (
        f"MERGE INTO {schema['table']} AS tgt\n"
        f"USING {temp_table} AS stg\n"
        f"ON {on_clause}\n"
        f"WHEN MATCHED THEN UPDATE SET {', '.join(update_assignments)}\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(insert_columns)}) "
        f"VALUES ({', '.join(insert_values)})"
    )


@dataclass
class LoadResult:
    loaded: list[tuple[int, str]] = field(default_factory=list)
    skipped: list[tuple[int, str]] = field(default_factory=list)
    failed: list[tuple[int, str, str]] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return not self.failed


def load_height_range(
    con,
    parquet_root: Path,
    reports_root: Path,
    start_height: int,
    end_height: int,
) -> LoadResult:
    """Load every dataset's partition for each height in [start, end].

    Refuses to proceed at all unless a passing Milestone 3 validation
    report already exists for exactly this range -- see
    `btc_ingest.validate_report.load_validation_report`.

    One (height, dataset) failing is recorded and does not stop the rest
    -- each PUT/COPY/MERGE targets only that height's own temp table and
    stage subpath, so failures can't corrupt unrelated already-loaded data.
    """
    load_validation_report(reports_root, start_height, end_height)  # raises if missing/not PASS

    result = LoadResult()
    for height in range(start_height, end_height + 1):
        for dataset in DATASET_SCHEMAS:
            local_path = parquet_root / dataset / f"block_height={height}" / "data.parquet"
            if not local_path.exists():
                result.failed.append((height, dataset, f"missing local partition: {local_path}"))
                continue

            stage_subpath = f"{dataset}/block_height={height}"
            temp_table = f"_STG_{dataset.upper()}_{height}"
            try:
                cursor = con.cursor()
                cursor.execute(generate_put_sql(local_path, stage_subpath))
                cursor.execute(generate_create_temp_table_sql(dataset, temp_table))
                cursor.execute(generate_copy_into_sql(dataset, temp_table, stage_subpath))
                cursor.execute(generate_merge_sql(dataset, temp_table, local_path.name))
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                result.loaded.append((height, dataset))
            except Exception as exc:
                result.failed.append((height, dataset, str(exc)))

    return result
