-- Per-quarter data-quality scorecard, computed from the data itself (not from
-- dbt test pass/fail). Exposed through the API's /health so an operator sees
-- freshness and completeness without opening dbt.
with facts as (
    select quarter, count(*) as n_facts, count(distinct adsh) as n_filings_with_facts,
           count(distinct cik) as n_companies, min(knowledge_date) as first_filed, max(knowledge_date) as last_filed,
           sum(case when value is null then 1 else 0 end) as n_null_values
    from {{ source('silver', 'facts') }}
    group by 1
),
subs as (
    select quarter, count(*) as n_submissions,
           sum(case when form in ('10-K', '10-K/A') then 1 else 0 end) as n_10k,
           sum(case when form in ('10-Q', '10-Q/A') then 1 else 0 end) as n_10q
    from {{ source('silver', 'submissions') }}
    group by 1
),
restated as (
    select count(*) as n_restated_facts from {{ ref('restatements') }}
),
ambiguous as (
    select quarter, count(*) as n_ambiguous_facts
    from (select quarter, adsh, fact_key from {{ source('silver', 'facts') }} group by 1, 2, 3 having count(distinct value) > 1)
    group by 1
)
select
    s.quarter, s.n_submissions, s.n_10k, s.n_10q,
    f.n_facts, f.n_filings_with_facts, f.n_companies, f.first_filed, f.last_filed,
    f.n_null_values, round(100.0 * f.n_null_values / nullif(f.n_facts, 0), 3) as pct_null_values,
    round(100.0 * f.n_filings_with_facts / nullif(s.n_submissions, 0), 2) as pct_filings_with_facts,
    r.n_restated_facts,
    coalesce(a.n_ambiguous_facts, 0) as n_ambiguous_facts,
    current_timestamp as built_at
from subs s
join facts f using (quarter)
left join ambiguous a using (quarter)
cross join restated r
