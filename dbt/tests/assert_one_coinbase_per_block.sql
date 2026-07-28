-- Fails if any loaded block does not have exactly one coinbase transaction.
select block_height, count(*) as coinbase_count
from {{ ref('transactions') }}
where is_coinbase
group by block_height
having count(*) != 1
