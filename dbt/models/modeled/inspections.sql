select
    event_version_key as event_key,
    event_version_key,
    source_record_key,
    usdot_number,
    event_date,
    reported_date,
    (reported_date - event_date)::integer as report_lag_days,
    state,
    violation_count,
    oos_violation_count,
    knowledge_valid_from,
    knowledge_valid_to,
    is_current,
    is_deleted,
    deletion_reason,
    record_hash
from {{ ref('event_versions') }}
where feed_name = 'inspections'
  and is_model_eligible
