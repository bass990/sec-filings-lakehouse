-- One row per (company, reporting period) with the headline metrics as CURRENTLY
-- known (latest version of each fact), plus the date each metric was first
-- reported so a consumer can reconstruct what was knowable when.
--
-- The period is the FACT's period (ddate + duration), never the filing's: a
-- restated FY2023 revenue carried in the FY2024 10-K still lands in the FY2023
-- row. fy/fp labels come from the filing that originally covered that period
-- end; comparatives from before the loaded quarters fall back to the calendar
-- year of the period end and a duration-based label.
--
-- Tags are the us-gaap concepts most filers use; companies that report under a
-- custom extension for a metric show NULL rather than a guessed value (the
-- data contract in schema.yml says which columns may be null).
{% set flow_metrics = {
    'revenue':             ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet'],
    'net_income':          ['NetIncomeLoss', 'ProfitLoss'],
    'operating_income':    ['OperatingIncomeLoss'],
    'operating_cash_flow': ['NetCashProvidedByUsedInOperatingActivities'],
    'eps_diluted':         ['EarningsPerShareDiluted'],
} %}
{% set instant_metrics = {
    'total_assets':        ['Assets'],
    'total_liabilities':   ['Liabilities'],
    'stockholders_equity': ['StockholdersEquity'],
} %}

with all_versions as (
    select v.cik, v.tag, v.ddate, v.qtrs, v.value, v.is_latest,
           min(v.valid_from) over (partition by v.fact_key) as first_reported   -- from the ORIGINAL version, not the restatement
    from {{ ref('fact_versions') }} v
    where v.coreg is null            -- consolidated entity only, no subsidiaries
      and not v.is_dimensional       -- whole-company figures, not segment/geography slices
      and v.uom in ('USD', 'USD/shares')
      and v.qtrs in (0, 1, 4)        -- instants, quarters, fiscal years
),
latest as (
    select cik, tag, ddate, qtrs, value, first_reported as valid_from
    from all_versions
    where is_latest
),
-- the filing that originally covered each period end labels it (fy, fp)
period_labels as (
    select cik, period as period_end, max(fy) as fy, max(fp) as fp
    from {{ source('silver', 'submissions') }}
    where period is not null and fp is not null
    group by 1, 2
),
company_names as (
    select cik, max(name) as company_name, max(sic) as sic from {{ source('silver', 'submissions') }} group by 1
),
period_durations as (
    select cik, ddate as period_end, max(qtrs) as max_qtrs from latest group by 1, 2
),
periods as (
    select d.cik, d.period_end,
        coalesce(p.fy, year(d.period_end)) as fy,
        coalesce(p.fp, case when d.max_qtrs = 4 then 'FY' else 'Q' end) as fp
    from period_durations d
    left join period_labels p on p.cik = d.cik and p.period_end = d.period_end
),
pivoted as (
    select
        pr.cik, pr.period_end, pr.fy, pr.fp,
        {% for col, tags in flow_metrics.items() %}
        max(case when l.tag in ({{ "'" ~ tags | join("','") ~ "'" }})
                 and l.qtrs = case when pr.fp = 'FY' then 4 else 1 end then l.value end) as {{ col }},
        min(case when l.tag in ({{ "'" ~ tags | join("','") ~ "'" }}) then l.valid_from end) as {{ col }}_first_reported,
        {% endfor %}
        {% for col, tags in instant_metrics.items() %}
        max(case when l.tag in ({{ "'" ~ tags | join("','") ~ "'" }}) and l.qtrs = 0 then l.value end) as {{ col }},
        min(case when l.tag in ({{ "'" ~ tags | join("','") ~ "'" }}) then l.valid_from end) as {{ col }}_first_reported{{ "," if not loop.last }}
        {% endfor %}
    from periods pr
    join latest l on l.cik = pr.cik and l.ddate = pr.period_end
    group by 1, 2, 3, 4
)
select
    pv.cik, cn.company_name, cn.sic, pv.fy, pv.fp, pv.period_end,
    {% for col in flow_metrics.keys() %}pv.{{ col }}, pv.{{ col }}_first_reported,
    {% endfor %}
    {% for col in instant_metrics.keys() %}pv.{{ col }}, pv.{{ col }}_first_reported,
    {% endfor %}
    case when pv.total_assets > 0 then pv.total_liabilities / pv.total_assets end as leverage_ratio,
    case when pv.revenue > 0 then pv.net_income / pv.revenue end as net_margin,
    current_timestamp as built_at
from pivoted pv
join company_names cn using (cik)
