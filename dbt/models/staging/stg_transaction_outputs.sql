-- Thin cast/rename of RAW.TRANSACTION_OUTPUTS. No business logic.
select
    block_height::number as block_height,
    txid::varchar        as txid,
    output_index::number as output_index,
    value_sats::number   as value_sats,
    address::varchar     as address,
    script_type::varchar as script_type,
    scriptpubkey::varchar as scriptpubkey,
    loaded_at,
    source_file_name
from {{ source('raw_btc', 'transaction_outputs') }}
