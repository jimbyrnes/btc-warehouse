-- For inputs referencing a transaction that IS present in the loaded
-- window ("internal" references), there must be exactly one matching
-- output. Foreign references (previous_txid outside the window) are
-- deliberately excluded here -- that's an expected boundary condition,
-- not a defect. Mirrors btc_ingest/validate.py's
-- internal_input_references_resolve_uniquely check.
with internal_inputs as (
    select i.txid, i.input_index, i.previous_txid, i.previous_vout_index
    from {{ ref('transaction_inputs') }} i
    where not i.is_coinbase
      and exists (
          select 1 from {{ ref('transactions') }} t where t.txid = i.previous_txid
      )
)
select
    ii.txid, ii.input_index, ii.previous_txid, ii.previous_vout_index,
    count(o.output_index) as matching_outputs
from internal_inputs ii
left join {{ ref('transaction_outputs') }} o
    on o.txid = ii.previous_txid and o.output_index = ii.previous_vout_index
group by ii.txid, ii.input_index, ii.previous_txid, ii.previous_vout_index
having count(o.output_index) != 1
