{# Composite-key uniqueness without pulling in dbt_utils (keeps the project dependency-free). #}
{% test dbt_utils_free_unique_combination(model, combination_of_columns) %}
select {{ combination_of_columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}
