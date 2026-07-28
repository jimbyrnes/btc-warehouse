-- Grain: one row per block identity (block_height + block_hash).
select
    block_height,
    block_hash,
    previous_block_hash,
    block_timestamp,
    transaction_count,
    size_bytes,
    weight_units,
    difficulty,
    merkle_root,
    nonce,
    bits,
    block_height || '-' || block_hash as block_id -- surrogate key enforcing the composite natural key
from {{ ref('stg_blocks') }}
