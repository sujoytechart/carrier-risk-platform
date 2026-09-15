{% set parse_reason_expressions %}
case when inspection_id is not null
          and parsed_source_key is null
     then 'invalid_source_record_key' end,
case when dot_number is not null
          and parsed_usdot_number is null
     then 'invalid_usdot_number' end,
case when insp_date is not null
          and parsed_event_date is null
     then 'invalid_event_date' end,
case when mcmis_add_date is not null
          and parsed_source_add_at is null
     then 'invalid_source_add_at' end,
case when change_date is not null
          and parsed_source_change_at is null
     then 'invalid_source_change_at' end,
case when viol_total is not null
          and parsed_violation_count is null
     then 'invalid_violation_count' end,
case when oos_total is not null
          and parsed_oos_count is null
     then 'invalid_oos_violation_count' end,
case when parsed_event_date is not null
          and parsed_source_add_at is not null
          and ({{ dbt.dateadd('day', 1, 'parsed_source_add_at') }})::date
              < parsed_event_date
     then 'impossible_event_chronology' end
{% endset %}

with source_rows as (

    select
        r.*,
        b.observed_at,
        {{ normalize_positive_integer('r.inspection_id') }} as parsed_source_key,
        {{ normalize_positive_integer('r.dot_number') }} as parsed_usdot_number,
        {{ parse_source_date('r.insp_date') }} as parsed_event_date,
        {{ parse_source_timestamp('r.mcmis_add_date') }} as parsed_source_add_at,
        {{ parse_source_timestamp('r.change_date') }} as parsed_source_change_at,
        {{ parse_nonnegative_integer('r.viol_total') }} as parsed_violation_count,
        {{ parse_nonnegative_integer('r.oos_total') }} as parsed_oos_count
    from {{ source('raw', 'inspection_rows') }} r
    join {{ ref('clean_snapshot_batches') }} b using (batch_id)
    where b.feed_name = 'inspections'

), normalized as (

    select
        *,
        {{ portable_reason_array(parse_reason_expressions) }} as parse_reasons
    from source_rows

)

select
    batch_id,
    source_row_number,
    'inspections'::text as feed_name,
    parsed_source_key as source_record_key,
    parsed_usdot_number as usdot_number,
    parsed_event_date as event_date,
    parsed_source_add_at as source_add_at,
    parsed_source_change_at as source_change_at,
    observed_at as first_observed_at,
    ({{ dbt.dateadd('day', 1, 'parsed_source_add_at') }})::date as source_proxy_reported_date,
    {{ dbt.dateadd('day', 1, 'parsed_source_add_at') }} as source_proxy_valid_from,
    nullif(trim(report_state), '') as state,
    parsed_violation_count as violation_count,
    parsed_oos_count as oos_violation_count,
    parse_reasons,
    {{ portable_payload_hash([
        'parsed_usdot_number',
        portable_payload_date('parsed_event_date'),
        portable_payload_timestamp('parsed_source_add_at'),
        portable_payload_timestamp('parsed_source_change_at'),
        "nullif(trim(report_state), '')",
        'parsed_violation_count',
        'parsed_oos_count',
        'parse_reasons'
    ]) }} as record_hash
from normalized
