-- Thin cast/rename of RAW.BLOCKS. No business logic, no aggregation --
-- see docs/PROJECT_STATE.md for the raw/staging/core distinction.
select
    block_height::number         as block_height,
    block_hash::varchar          as block_hash,
    previous_block_hash::varchar as previous_block_hash,
    to_timestamp_ntz("TIMESTAMP") as block_timestamp, -- raw epoch seconds -> real timestamp; quoted+uppercase to match the column as created in snowflake/setup.sql (quoted identifiers are case-sensitive in Snowflake)
    transaction_count::number    as transaction_count,
    size_bytes::number           as size_bytes,
    weight_units::number         as weight_units,
    difficulty::float            as difficulty,
    merkle_root::varchar         as merkle_root,
    nonce::number                as nonce,
    bits::number                 as bits,
    loaded_at,
    source_file_name
from {{ source('raw_btc', 'blocks') }}
