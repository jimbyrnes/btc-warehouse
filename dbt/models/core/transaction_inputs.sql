-- Grain: one row per input position within a transaction
-- (natural key: txid + input_index).
select
    txid,
    input_index,
    block_height,
    previous_txid,
    previous_vout_index,
    previous_output_value_sats,
    previous_output_address,
    previous_output_script_type,
    scriptsig,
    sequence_number,
    is_coinbase,
    txid || '-' || input_index as input_id -- surrogate key enforcing the composite natural key
from {{ ref('stg_transaction_inputs') }}
