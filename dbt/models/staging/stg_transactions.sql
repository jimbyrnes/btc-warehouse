-- Thin cast/rename of RAW.TRANSACTIONS. No business logic.
select
    block_height::number      as block_height,
    block_hash::varchar       as block_hash,
    txid::varchar             as txid,
    transaction_index::number as transaction_index,
    version::number           as version,
    locktime::number          as locktime,
    size_bytes::number        as size_bytes,
    weight_units::number      as weight_units,
    vsize::number             as vsize,
    fee_sats::number          as fee_sats,
    is_coinbase::boolean      as is_coinbase,
    loaded_at,
    source_file_name
from {{ source('raw_btc', 'transactions') }}
