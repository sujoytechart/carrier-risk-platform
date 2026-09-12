{{ config(enabled=var('learning_demo', false)) }}

-- Conflicting retained rows must not silently multiply an inspection's counts.
select source_key from (
    select distinct source_key, usdot_number, event_date, reported_date,
        violation_count, oos_violation_count
    from {{ source('learning_demo', 'source_events') }}
    where event_type = 'inspection'
) inspections
group by source_key having count(*) > 1
