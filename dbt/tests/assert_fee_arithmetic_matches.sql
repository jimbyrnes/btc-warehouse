-- fee_sats = sum(input previous_output_value_sats) - sum(output value_sats),
-- for ordinary transactions where every input's previous output value is
-- known. Mirrors btc_ingest/validate.py's fee_arithmetic_matches check.
with input_sums as (
    select
        txid,
        sum(previous_output_value_sats) as total_input_value,
        sum(case when previous_output_value_sats is null then 1 else 0 end) as null_input_values
    from {{ ref('transaction_inputs') }}
    where not is_coinbase
    group by txid
),
output_sums as (
    select txid, sum(value_sats) as total_output_value
    from {{ ref('transaction_outputs') }}
    group by txid
)
select
    t.txid, t.fee_sats as reported_fee,
    (i.total_input_value - o.total_output_value) as derived_fee
from {{ ref('transactions') }} t
join input_sums i on i.txid = t.txid
join output_sums o on o.txid = t.txid
where not t.is_coinbase
  and i.null_input_values = 0
  and (i.total_input_value - o.total_output_value) != t.fee_sats
