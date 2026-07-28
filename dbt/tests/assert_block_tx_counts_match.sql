-- Fails if a block's reported transaction_count disagrees with the actual
-- number of loaded transaction rows for that block. Mirrors
-- btc_ingest/validate.py's block_tx_count_matches_transactions check.
select b.block_height, b.transaction_count as reported, count(t.txid) as actual
from {{ ref('blocks') }} b
left join {{ ref('transactions') }} t on t.block_height = b.block_height
group by b.block_height, b.transaction_count
having count(t.txid) != b.transaction_count
