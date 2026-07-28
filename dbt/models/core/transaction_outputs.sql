-- Grain: one row per output position within a transaction
-- (natural key: txid + output_index).
select
    txid,
    output_index,
    block_height,
    value_sats,
    address,
    script_type,
    scriptpubkey,
    txid || '-' || output_index as output_id -- surrogate key enforcing the composite natural key
from {{ ref('stg_transaction_outputs') }}
