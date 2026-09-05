{% set scoring_dates = var('scoring_dates', ['2026-06-01']) %}

with scoring_grid(scoring_date) as (

    values
    {% for scoring_date in scoring_dates %}
        (date '{{ scoring_date }}'){% if not loop.last %},{% endif %}
    {% endfor %}

), eligible_carriers as (

    select distinct
        events.usdot_number,
        grid.scoring_date
    from scoring_grid grid
    join {{ ref('events_union') }} events
      on events.event_type = 'inspection'
     and events.knowledge_valid_from < grid.scoring_date::timestamp
     and (
            events.knowledge_valid_to is null
            or events.knowledge_valid_to > grid.scoring_date::timestamp
         )
     and not events.is_deleted
     and events.event_date >= (grid.scoring_date - interval '6 months')::date
     and events.event_date < grid.scoring_date
     and events.reported_date < grid.scoring_date

), aggregates as (

    select
        carriers.usdot_number,
        carriers.scoring_date,
        count(*) filter (
            where events.event_type = 'inspection'
              and events.event_date >=
                  (carriers.scoring_date - interval '6 months')::date
        )::bigint as inspections_6m,
        coalesce(sum(events.violation_count) filter (
            where events.event_type = 'inspection'
              and events.event_date >=
                  (carriers.scoring_date - interval '6 months')::date
        ), 0)::bigint as violations_6m,
        coalesce(sum(events.oos_violation_count) filter (
            where events.event_type = 'inspection'
              and events.event_date >=
                  (carriers.scoring_date - interval '6 months')::date
        ), 0)::bigint as oos_violations_6m,
        count(distinct events.carrier_crash_key) filter (
            where events.event_type = 'crash'
              and events.event_date >=
                  (carriers.scoring_date - interval '24 months')::date
        )::bigint as crashes_24m,
        max(events.event_date) filter (
            where events.event_type = 'inspection'
              and events.event_date >=
                  (carriers.scoring_date - interval '6 months')::date
        ) as last_inspection_date
    from eligible_carriers carriers
    left join {{ ref('events_union') }} events
      on events.usdot_number = carriers.usdot_number
     and events.knowledge_valid_from < carriers.scoring_date::timestamp
     and (
            events.knowledge_valid_to is null
            or events.knowledge_valid_to > carriers.scoring_date::timestamp
         )
     and not events.is_deleted
     and events.event_date < carriers.scoring_date
     and events.reported_date < carriers.scoring_date
     and events.event_date >=
         (carriers.scoring_date - interval '24 months')::date
    group by carriers.usdot_number, carriers.scoring_date

)

select
    usdot_number,
    scoring_date,
    inspections_6m,
    violations_6m,
    oos_violations_6m,
    crashes_24m,
    round(violations_6m::numeric / nullif(inspections_6m, 0), 6)
        as violations_per_inspection,
    round(oos_violations_6m::numeric / nullif(violations_6m, 0), 6)
        as oos_violation_rate,
    (scoring_date - last_inspection_date)::integer as days_since_last_inspection,
    last_inspection_date
from aggregates
