{{ config(tags=['temporal'], severity='error') }}

with feature_keys as (

    select
        usdot_number,
        scoring_date,
        scoring_date::timestamp at time zone 'UTC' as scoring_timestamp
    from {{ ref('training_features') }}

), expected as (

    select
        keys.usdot_number,
        keys.scoring_date,
        count(*) filter (
            where events.event_type = 'inspection'
              and events.event_date >=
                  (keys.scoring_date - interval '6 months')::date
        )::bigint as inspections_6m,
        coalesce(sum(events.violation_count) filter (
            where events.event_type = 'inspection'
              and events.event_date >=
                  (keys.scoring_date - interval '6 months')::date
        ), 0)::bigint as violations_6m,
        coalesce(sum(events.oos_violation_count) filter (
            where events.event_type = 'inspection'
              and events.event_date >=
                  (keys.scoring_date - interval '6 months')::date
        ), 0)::bigint as oos_violations_6m,
        count(distinct events.carrier_crash_key) filter (
            where events.event_type = 'crash'
              and events.event_date >=
                  (keys.scoring_date - interval '24 months')::date
        )::bigint as crashes_24m,
        max(events.event_date) filter (
            where events.event_type = 'inspection'
              and events.event_date >=
                  (keys.scoring_date - interval '6 months')::date
        ) as last_inspection_date
    from feature_keys keys
    left join {{ ref('events_union') }} events
      on events.usdot_number = keys.usdot_number
     and events.knowledge_valid_from < keys.scoring_timestamp
     and (
            events.knowledge_valid_to is null
            or events.knowledge_valid_to > keys.scoring_timestamp
         )
     and not events.is_deleted
     and events.event_date < keys.scoring_date
     and events.reported_date < keys.scoring_date
     and events.event_date >= (keys.scoring_date - interval '24 months')::date
    group by keys.usdot_number, keys.scoring_date

)

select
    features.usdot_number,
    features.scoring_date,
    features.inspections_6m,
    expected.inspections_6m as expected_inspections_6m,
    features.violations_6m,
    expected.violations_6m as expected_violations_6m,
    features.oos_violations_6m,
    expected.oos_violations_6m as expected_oos_violations_6m,
    features.crashes_24m,
    expected.crashes_24m as expected_crashes_24m,
    features.last_inspection_date,
    expected.last_inspection_date as expected_last_inspection_date
from {{ ref('training_features') }} features
join expected using (usdot_number, scoring_date)
where features.inspections_6m <> expected.inspections_6m
   or features.violations_6m <> expected.violations_6m
   or features.oos_violations_6m <> expected.oos_violations_6m
   or features.crashes_24m <> expected.crashes_24m
   or features.last_inspection_date is distinct from expected.last_inspection_date
