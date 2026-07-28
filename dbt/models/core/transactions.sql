-- Grain: one row per transaction. txid is itself the natural key -- no
-- surrogate key needed.
select
    txid,
    block_height,
    block_hash,
    transaction_index,
    version,
    locktime,
    size_bytes,
    weight_units,
    vsize,
    fee_sats,
    is_coinbase
from {{ ref('stg_transactions') }}
