{#
  A tiny hand-rolled generic test (no dbt_utils dependency, for a 25-block
  educational project that doesn't need an external package for this).
  Supports an optional `where` argument, e.g.:
    - nonnegative:
        where: "not is_coinbase"
#}
{% test nonnegative(model, column_name, where=none) %}
select *
from {{ model }}
where {{ column_name }} < 0
{% if where %} and ({{ where }}) {% endif %}
{% endtest %}
