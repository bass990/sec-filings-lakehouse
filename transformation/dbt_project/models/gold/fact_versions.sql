-- Every version of every fact with its validity window: the interval during which
-- that value was the most recently reported one. This is the point-in-time
-- backbone. "Value as known on date D" = the row where valid_from <= D < valid_to.
--
-- Amendments (10-K/A) filed the same day as the original are ordered after it
-- via `accepted` so the amended value wins.
-- A few dozen (filing, fact) pairs per quarter carry several different values for
-- the same key: filer-side XBRL errors where a dimension was lost in the SEC's
-- flattening. They cannot be resolved honestly, so they are excluded here and
-- counted in gold.data_quality (n_ambiguous_facts) rather than silently picked.
with ambiguous as (
    select adsh, fact_key
    from {{ source('silver', 'facts') }}
    group by 1, 2
    having count(distinct value) > 1
),
clean as (
    -- one row per (filing, fact): drop exact duplicate rows and the ambiguous pairs above
    select f.*
    from {{ source('silver', 'facts') }} f
    anti join ambiguous a on a.adsh = f.adsh and a.fact_key = f.fact_key
    qualify row_number() over (partition by f.adsh, f.fact_key order by f.footnote nulls last) = 1
),
ranked as (
    select
        f.fact_key, f.cik, f.tag, f.ddate, f.qtrs, f.uom, f.coreg, f.segments, f.is_dimensional,
        f.value, f.footnote, f.adsh, f.form, f.fy, f.fp, f.version as taxonomy_version,
        f.knowledge_date,
        s.accepted,
        row_number() over (partition by f.fact_key order by f.knowledge_date, s.accepted, f.adsh) as version_no,
        lead(f.knowledge_date) over (partition by f.fact_key order by f.knowledge_date, s.accepted, f.adsh) as next_knowledge_date,
        count(*) over (partition by f.fact_key) as n_versions
    from clean f
    join {{ source('silver', 'submissions') }} s using (adsh)
)
select
    fact_key, cik, tag, ddate, qtrs, uom, coreg, segments, is_dimensional, value, footnote, adsh, form, fy, fp, taxonomy_version,
    knowledge_date as valid_from,
    coalesce(next_knowledge_date, date '9999-12-31') as valid_to,
    version_no,
    n_versions,
    next_knowledge_date is null as is_latest
from ranked
