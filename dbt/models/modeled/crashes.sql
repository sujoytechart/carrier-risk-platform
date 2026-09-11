select
    event_version_key as event_key,
    event_version_key,
    source_record_key,
    usdot_number,
    event_date,
    reported_date,
    {{ dbt.datediff('event_date', 'reported_date', 'day') }}::integer as report_lag_days,
    report_state as state,
    report_number,
    report_time,
    report_seq_no,
    fatalities,
    injuries,
    tow_away,
    knowledge_valid_from,
    knowledge_valid_to,
    is_current,
    is_deleted,
    deletion_reason,
    record_hash
from {{ ref('event_versions') }}
where feed_name = 'crashes'
  and is_model_eligible
