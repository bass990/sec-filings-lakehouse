-- Facts whose reported value changed between filings: the original value, the
-- latest value, and how far apart they are. This is the table an analyst
-- opens after a surprise, and the table an ML feature store must consult to
-- avoid training on values that were not knowable at the time.
with first_last as (
    select
        fact_key, cik, tag, ddate, qtrs, uom, segments, is_dimensional,
        min(case when version_no = 1 then value end) as original_value,
        max(case when is_latest then value end) as latest_value,
        min(valid_from) as first_reported,
        max(valid_from) as last_reported,
        max(n_versions) as n_versions,
        max(case when is_latest then adsh end) as latest_adsh,
        max(case when is_latest then footnote end) as latest_footnote
    from {{ ref('fact_versions') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8
)
select
    *,
    latest_value - original_value as abs_change,
    case when original_value is null or original_value = 0 then null
         else (latest_value - original_value) / abs(original_value) end as rel_change,
    datediff('day', first_reported, last_reported) as days_between
from first_last
where n_versions > 1
  and original_value is distinct from latest_value
