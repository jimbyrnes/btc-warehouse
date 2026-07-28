-- Milestone 4: Snowflake infrastructure for the btc-warehouse educational
-- project. Run once, manually, by a human with an active Snowflake trial
-- account -- this file is never executed automatically by any script here.
--
-- Substitute <YOUR_...> placeholders (or just run as-is; the names below
-- are sensible defaults matching what btc_ingest/snowflake_loader.py
-- expects via the SNOWFLAKE_DATABASE / SNOWFLAKE_WAREHOUSE env vars).
--
-- Cost note: this is a 30-day/$400 trial account, not a permanent free
-- tier. The warehouse below is created suspended, XSMALL, with a 60-second
-- auto-suspend so it never idles expensively -- but you are still
-- responsible for suspending it manually after any session (see the
-- bottom of this file) and for eventually deleting the account/warehouse
-- once this project is done with it.

-- === Warehouse ===============================================================
-- XSMALL is the smallest compute size Snowflake offers -- more than enough
-- for a 25-block (four tables, low hundreds of thousands of rows) dataset.
CREATE WAREHOUSE IF NOT EXISTS BTC_WAREHOUSE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60          -- seconds of inactivity before suspending
    AUTO_RESUME = TRUE         -- next query wakes it back up automatically
    INITIALLY_SUSPENDED = TRUE -- don't spend a single credit until first used
    COMMENT = 'btc-warehouse educational project -- Milestone 4';

-- === Database and schemas ====================================================
CREATE DATABASE IF NOT EXISTS BTC_WAREHOUSE
    COMMENT = 'btc-warehouse educational project -- bounded 25-block window';

USE DATABASE BTC_WAREHOUSE;

-- RAW: typed landing tables, one row per already-flattened/validated local
--      Parquet row. Not VARIANT -- the local pipeline already did the
--      deterministic flattening and passed the Milestone 3 quality gate.
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Typed landing zone loaded from validated local Parquet.';

-- STAGING: dbt staging models live here (thin casts/renames of RAW).
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'dbt staging models -- casts and renames only, no business logic.';

-- CORE: dbt core models live here (the four analytical grains).
CREATE SCHEMA IF NOT EXISTS CORE
    COMMENT = 'dbt core models -- blocks, transactions, transaction_inputs, transaction_outputs.';

-- === File format and internal stage =========================================
USE SCHEMA RAW;

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT
    TYPE = PARQUET
    COMMENT = 'Shared Parquet file format for all four raw landing tables.';

-- Internal (Snowflake-managed) stage -- no external cloud storage account
-- needed. btc_ingest/snowflake_loader.py PUTs partition files here under a
-- path that mirrors the local layout exactly:
--   @RAW.PARQUET_STAGE/<dataset>/block_height=<height>/data.parquet
CREATE STAGE IF NOT EXISTS PARQUET_STAGE
    FILE_FORMAT = PARQUET_FORMAT
    COMMENT = 'Internal stage for validated local Parquet partitions.';

-- === Raw landing tables ======================================================
-- Column names/types match btc_ingest/flatten.py's row shape exactly (see
-- that module's docstring for field-by-field provenance). LOADED_AT and
-- SOURCE_FILE_NAME are the only added ingestion-metadata columns --
-- deliberately no SOURCE_BLOCK_HEIGHT, since BLOCK_HEIGHT is already a
-- column on every one of these tables and a second copy would just be
-- clutter, not information.

CREATE TABLE IF NOT EXISTS RAW.BLOCKS (
    BLOCK_HEIGHT          NUMBER      NOT NULL,
    BLOCK_HASH            VARCHAR     NOT NULL,
    PREVIOUS_BLOCK_HASH   VARCHAR,
    "TIMESTAMP"           NUMBER,      -- raw Unix epoch seconds; staging casts this
    TRANSACTION_COUNT     NUMBER,
    SIZE_BYTES            NUMBER,
    WEIGHT_UNITS          NUMBER,
    DIFFICULTY            FLOAT,
    MERKLE_ROOT           VARCHAR,
    NONCE                 NUMBER,
    BITS                  NUMBER,
    LOADED_AT             TIMESTAMP_NTZ,
    SOURCE_FILE_NAME      VARCHAR
);

CREATE TABLE IF NOT EXISTS RAW.TRANSACTIONS (
    BLOCK_HEIGHT          NUMBER      NOT NULL,
    BLOCK_HASH            VARCHAR     NOT NULL,
    TXID                  VARCHAR     NOT NULL,
    TRANSACTION_INDEX     NUMBER,
    VERSION               NUMBER,
    LOCKTIME              NUMBER,
    SIZE_BYTES            NUMBER,
    WEIGHT_UNITS          NUMBER,
    VSIZE                 NUMBER,
    FEE_SATS              NUMBER,
    IS_COINBASE           BOOLEAN,
    LOADED_AT             TIMESTAMP_NTZ,
    SOURCE_FILE_NAME      VARCHAR
);

CREATE TABLE IF NOT EXISTS RAW.TRANSACTION_INPUTS (
    BLOCK_HEIGHT                   NUMBER  NOT NULL,
    TXID                           VARCHAR NOT NULL,
    INPUT_INDEX                    NUMBER  NOT NULL,
    PREVIOUS_TXID                  VARCHAR,
    PREVIOUS_VOUT_INDEX            NUMBER,
    PREVIOUS_OUTPUT_VALUE_SATS     NUMBER,
    PREVIOUS_OUTPUT_ADDRESS        VARCHAR,
    PREVIOUS_OUTPUT_SCRIPT_TYPE    VARCHAR,
    SCRIPTSIG                      VARCHAR,
    "SEQUENCE"                     NUMBER,     -- quoted: SEQUENCE collides with a Snowflake reserved word
    IS_COINBASE                    BOOLEAN,
    LOADED_AT                      TIMESTAMP_NTZ,
    SOURCE_FILE_NAME               VARCHAR
);

CREATE TABLE IF NOT EXISTS RAW.TRANSACTION_OUTPUTS (
    BLOCK_HEIGHT          NUMBER      NOT NULL,
    TXID                  VARCHAR     NOT NULL,
    OUTPUT_INDEX          NUMBER      NOT NULL,
    VALUE_SATS            NUMBER,
    ADDRESS               VARCHAR,
    SCRIPT_TYPE           VARCHAR,
    SCRIPTPUBKEY          VARCHAR,
    LOADED_AT             TIMESTAMP_NTZ,
    SOURCE_FILE_NAME      VARCHAR
);

-- === After you're done for the session ======================================
-- The warehouse auto-suspends after 60s idle on its own, but you can also
-- suspend it immediately and confirm:
--
--   ALTER WAREHOUSE BTC_WAREHOUSE_WH SUSPEND;
--   SHOW WAREHOUSES LIKE 'BTC_WAREHOUSE_WH';  -- check "state" = 'SUSPENDED'
