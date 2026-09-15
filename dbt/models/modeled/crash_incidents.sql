with keyed_crash_versions as (

    select
        *,
        {{ portable_incident_key('usdot_number', 'state', 'report_number', 'event_date', 'report_time') }} as carrier_crash_key
    from {{ ref('crashes') }}

), incident_boundaries as (

    select distinct
        carrier_crash_key,
        knowledge_valid_from
    from keyed_crash_versions

    union

    select distinct
        carrier_crash_key,
        knowledge_valid_to as knowledge_valid_from
    from keyed_crash_versions
    where knowledge_valid_to is not null

), incident_segments as (

    select
        carrier_crash_key,
        knowledge_valid_from,
        lead(knowledge_valid_from) over (
            partition by carrier_crash_key
            order by knowledge_valid_from
        ) as knowledge_valid_to
    from incident_boundaries

), active_vehicle_versions as (

    select
        segments.knowledge_valid_from as segment_valid_from,
        segments.knowledge_valid_to as segment_valid_to,
        crashes.*
    from incident_segments segments
    join keyed_crash_versions crashes
      on crashes.carrier_crash_key = segments.carrier_crash_key
     and crashes.knowledge_valid_from <= segments.knowledge_valid_from
     and (
            crashes.knowledge_valid_to is null
            or crashes.knowledge_valid_to > segments.knowledge_valid_from
         )

)

select
    min(event_key) as event_key,
    min(event_version_key) as event_version_key,
    carrier_crash_key,
    usdot_number,
    min(event_date) as event_date,
    max(reported_date) as reported_date,
    {{ dbt.datediff('min(event_date)', 'max(reported_date)', 'day') }}::integer as report_lag_days,
    state,
    report_number,
    report_time,
    max(case when not is_deleted then fatalities end) as fatalities,
    max(case when not is_deleted then injuries end) as injuries,
    {{ portable_bool_or('case when not is_deleted then tow_away end') }} as tow_away,
    segment_valid_from as knowledge_valid_from,
    segment_valid_to as knowledge_valid_to,
    segment_valid_to is null as is_current,
    {{ portable_bool_and('is_deleted') }} as is_deleted,
    case when {{ portable_bool_and('is_deleted') }} then max(deletion_reason) end as deletion_reason,
    {{ portable_hash_aggregate('record_hash', 'source_record_key') }} as record_hash
from active_vehicle_versions
group by
    carrier_crash_key,
    usdot_number,
    state,
    report_number,
    report_time,
    segment_valid_from,
    segment_valid_to
