{% test positive(model, column_name, where=none) %}
select *
from {{ model }}
where {{ column_name }} <= 0
{% if where %} and ({{ where }}) {% endif %}
{% endtest %}
