-- vsize must equal ceil(weight_units / 4) for every transaction. Mirrors
-- btc_ingest/validate.py's vsize_matches_formula check.
select txid, weight_units, vsize
from {{ ref('transactions') }}
where vsize != ceil(weight_units / 4.0)
