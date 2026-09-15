{{ config(enabled=var('learning_demo', false), schema='learning_demo',
    indexes=[{'columns': ['usdot_number', 'scoring_date'], 'unique': true}]
    if target.type == 'postgres' else []) }}

-- Frozen calendar landmarks are selected before model fitting. This relation
-- uses retained snapshot versions, not the canonical event-history contract.
with scoring_grid(scoring_date) as (
    select date '2024-02-01' union all
    select date '2024-09-01' union all
    select date '2026-09-01'
), inspections as (
    select distinct source_key, usdot_number, event_date, reported_date,
        violation_count, oos_violation_count
    from {{ source('learning_demo', 'source_events') }}
    where event_type = 'inspection'
), inspection_features as (
    select inspection.usdot_number, grid.scoring_date,
        count(*)::bigint as inspections_4m,
        sum(inspection.violation_count)::bigint as violations_4m,
        sum(inspection.oos_violation_count)::bigint as oos_violations_4m,
        max(inspection.event_date) as last_inspection_date
    from scoring_grid grid
    join inspections inspection
      on inspection.event_date >=
          ({{ dbt.dateadd('month', -4, 'grid.scoring_date') }})::date
     and inspection.event_date < grid.scoring_date
     and inspection.reported_date < grid.scoring_date
    group by inspection.usdot_number, grid.scoring_date
), crashes as (
    select source_key, usdot_number, event_date,
        min(reported_date) as first_reported_date,
        max(case when federal_recordable then 1 else 0 end) as federal_recordable
    from {{ source('learning_demo', 'source_events') }}
    where event_type = 'crash'
    group by source_key, usdot_number, event_date
), crash_features as (
    select features.usdot_number, features.scoring_date,
        count(crash.source_key)::bigint as crashes_24m
    from inspection_features features
    left join crashes crash
      on crash.usdot_number = features.usdot_number
     and crash.event_date >=
          ({{ dbt.dateadd('month', -24, 'features.scoring_date') }})::date
     and crash.event_date < features.scoring_date
     and crash.first_reported_date < features.scoring_date
    group by features.usdot_number, features.scoring_date
), outcomes as (
    select features.usdot_number, features.scoring_date,
        max(case when crash.source_key is not null then 1 else 0 end)::integer as label
    from inspection_features features
    left join crashes crash
      on crash.usdot_number = features.usdot_number
     and crash.event_date >= features.scoring_date
     and crash.event_date <
          ({{ dbt.dateadd('month', 6, 'features.scoring_date') }})::date
     and crash.first_reported_date < date '2026-09-03'
     and crash.federal_recordable = 1
    group by features.usdot_number, features.scoring_date
)
select features.usdot_number, features.scoring_date,
    features.inspections_4m, features.violations_4m, features.oos_violations_4m,
    crashes.crashes_24m,
    {{ portable_ratio('features.violations_4m', 'features.inspections_4m') }}
        as violations_per_inspection,
    {{ portable_ratio('features.oos_violations_4m', 'features.violations_4m') }}
        as oos_violation_rate,
    {{ dbt.datediff('features.last_inspection_date', 'features.scoring_date', 'day') }}::integer
        as days_since_last_inspection,
    case when features.scoring_date < date '2026-09-01'
        then outcomes.label else null end::integer as label
from inspection_features features
join crash_features crashes using (usdot_number, scoring_date)
join outcomes using (usdot_number, scoring_date)
