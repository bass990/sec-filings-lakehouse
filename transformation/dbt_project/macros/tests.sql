{% test dbt_utils_free_between(model, column_name, lo, hi) %}
select {{ column_name }} from {{ model }}
where {{ column_name }} is not null and ({{ column_name }} < {{ lo }} or {{ column_name }} > {{ hi }})
{% endtest %}

{% test exactly_one_latest_per_key(model) %}
select fact_key, sum(case when is_latest then 1 else 0 end) as n_latest
from {{ model }}
group by fact_key
having sum(case when is_latest then 1 else 0 end) <> 1
{% endtest %}

{# Consecutive versions of a fact must tile time: version n's valid_to == version n+1's valid_from. #}
{% test windows_do_not_overlap(model) %}
with w as (
    select fact_key, version_no, valid_from, valid_to,
           lead(valid_from) over (partition by fact_key order by version_no) as next_from
    from {{ model }}
)
select * from w where next_from is not null and valid_to <> next_from
{% endtest %}
