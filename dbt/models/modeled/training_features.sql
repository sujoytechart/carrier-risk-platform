{% set scoring_dates = var('scoring_dates', none) %}
{% if scoring_dates is not none %}
    {% if scoring_dates is string or scoring_dates is mapping
          or scoring_dates is not sequence or not scoring_dates %}
        {{ exceptions.raise_compiler_error('scoring_dates must be a nonempty list of ISO month-start dates') }}
    {% endif %}
    {% for scoring_date in scoring_dates %}
        {% if scoring_date is not string or not modules.re.fullmatch(
            '(?!0000)[0-9]{4}-(0[1-9]|1[0-2])-01', scoring_date
        ) %}
            {{ exceptions.raise_compiler_error('scoring_dates must contain only ISO month-start dates') }}
        {% endif %}
    {% endfor %}
{% endif %}

with requested_scoring_dates(scoring_date) as (

    {% if scoring_dates is not none %}
        select distinct scoring_date
        from (values
        {% for scoring_date in scoring_dates %}
            (date '{{ scoring_date }}'){% if not loop.last %},{% endif %}
        {% endfor %}
        ) explicit_dates(scoring_date)
    {% else %}
        -- Metadata bounds are reproducible: only successfully applied complete
        -- batches define the inclusive month range; the wall clock is irrelevant.
        select generate_series(
            date_trunc('month', min(observed_at) at time zone 'UTC'),
            date_trunc('month', max(observed_at) at time zone 'UTC'),
            interval '1 month'
        )::date
        from {{ this.schema }}.event_version_batches
    {% endif %}

), scoring_grid as (

    select
        scoring_date,
        scoring_date::timestamp at time zone 'UTC' as scoring_timestamp
    from requested_scoring_dates

), eligible_carriers as (

    select distinct
        events.usdot_number,
        grid.scoring_date,
        grid.scoring_timestamp
    from scoring_grid grid
    join {{ ref('events_union') }} events
      on events.event_type = 'inspection'
     and events.knowledge_valid_from < grid.scoring_timestamp
     and (
            events.knowledge_valid_to is null
            or events.knowledge_valid_to > grid.scoring_timestamp
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
     and events.knowledge_valid_from < carriers.scoring_timestamp
     and (
            events.knowledge_valid_to is null
            or events.knowledge_valid_to > carriers.scoring_timestamp
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
