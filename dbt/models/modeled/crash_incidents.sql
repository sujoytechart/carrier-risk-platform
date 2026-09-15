with keyed_crash_versions as (

    select
        *,
        md5(concat_ws(chr(31),
            usdot_number,
            state,
            report_number,
            event_date::text,
            report_time::text
        )) as carrier_crash_key
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
    (max(reported_date) - min(event_date))::integer as report_lag_days,
    state,
    report_number,
    report_time,
    max(fatalities) filter (where not is_deleted) as fatalities,
    max(injuries) filter (where not is_deleted) as injuries,
    bool_or(tow_away) filter (where not is_deleted) as tow_away,
    segment_valid_from as knowledge_valid_from,
    segment_valid_to as knowledge_valid_to,
    segment_valid_to is null as is_current,
    bool_and(is_deleted) as is_deleted,
    case when bool_and(is_deleted) then max(deletion_reason) end as deletion_reason,
    md5(string_agg(record_hash, chr(31) order by source_record_key)) as record_hash
from active_vehicle_versions
group by
    carrier_crash_key,
    usdot_number,
    state,
    report_number,
    report_time,
    segment_valid_from,
    segment_valid_to
