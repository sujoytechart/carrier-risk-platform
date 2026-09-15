select
    event_key,
    event_version_key,
    'inspection'::text as event_type,
    null::text as carrier_crash_key,
    usdot_number,
    event_date,
    reported_date,
    report_lag_days,
    state,
    violation_count,
    oos_violation_count,
    null::integer as fatalities,
    null::integer as injuries,
    null::boolean as tow_away,
    knowledge_valid_from,
    knowledge_valid_to,
    is_current,
    is_deleted,
    deletion_reason,
    record_hash
from {{ ref('inspections') }}

union all

select
    event_key,
    event_version_key,
    'crash'::text as event_type,
    {{ portable_incident_key('usdot_number', 'state', 'report_number', 'event_date', 'report_time') }} as carrier_crash_key,
    usdot_number,
    event_date,
    reported_date,
    report_lag_days,
    state,
    null::integer as violation_count,
    null::integer as oos_violation_count,
    fatalities,
    injuries,
    tow_away,
    knowledge_valid_from,
    knowledge_valid_to,
    is_current,
    is_deleted,
    deletion_reason,
    record_hash
-- Keep vehicle-version intervals here. Apply both clocks before incident
-- aggregation so one vehicle's correction cannot hide unaffected vehicles.
from {{ ref('crashes') }}
