-- Thin cast/rename of RAW.TRANSACTION_INPUTS. No business logic.
-- SEQUENCE is renamed to sequence_number here so every downstream query
-- (and every analyst) is spared from needing to quote a reserved word.
select
    block_height::number                 as block_height,
    txid::varchar                        as txid,
    input_index::number                  as input_index,
    previous_txid::varchar               as previous_txid,
    previous_vout_index::number          as previous_vout_index,
    previous_output_value_sats::number   as previous_output_value_sats,
    previous_output_address::varchar     as previous_output_address,
    previous_output_script_type::varchar as previous_output_script_type,
    scriptsig::varchar                   as scriptsig,
    "SEQUENCE"::number                   as sequence_number,
    is_coinbase::boolean                 as is_coinbase,
    loaded_at,
    source_file_name
from {{ source('raw_btc', 'transaction_inputs') }}
