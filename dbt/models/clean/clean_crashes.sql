{% set parse_reason_expressions %}
case when crash_id is not null
          and parsed_crash_id is null
     then 'invalid_source_record_key' end,
case when dot_number is not null
          and parsed_usdot_number is null
     then 'invalid_usdot_number' end,
case when report_date is not null
          and parsed_event_date is null
     then 'invalid_event_date' end,
case when add_date is not null
          and parsed_source_add_at is null
     then 'invalid_source_add_at' end,
case when change_date is not null
          and parsed_source_change_at is null
     then 'invalid_source_change_at' end,
case when report_time is not null
          and parsed_report_time is null
     then 'invalid_report_time' end,
case when report_seq_no is not null
          and parsed_report_seq_no is null
     then 'invalid_report_seq_no' end,
case when fatalities is not null
          and parsed_fatalities is null
     then 'invalid_fatalities' end,
case when injuries is not null
          and parsed_injuries is null
     then 'invalid_injuries' end,
case when tow_away is not null
          and parsed_tow_away is null
     then 'invalid_tow_away' end,
case when federal_recordable is not null
          and parsed_federal_recordable is null
     then 'invalid_federal_recordable' end,
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
        {{ normalize_positive_integer('r.crash_id') }} as parsed_crash_id,
        {{ normalize_positive_integer('r.dot_number') }} as parsed_usdot_number,
        {{ parse_source_date('r.report_date') }} as parsed_event_date,
        {{ parse_source_timestamp('r.add_date') }} as parsed_source_add_at,
        {{ parse_source_timestamp('r.change_date') }} as parsed_source_change_at,
        {{ parse_report_time('r.report_time') }} as parsed_report_time,
        {{ parse_nonnegative_integer('r.report_seq_no') }} as parsed_report_seq_no,
        {{ parse_nonnegative_integer('r.fatalities') }} as parsed_fatalities,
        {{ parse_nonnegative_integer('r.injuries') }} as parsed_injuries,
        {{ parse_source_boolean('r.tow_away') }} as parsed_tow_away,
        {{ parse_source_boolean('r.federal_recordable') }} as parsed_federal_recordable
    from {{ source('raw', 'crash_rows') }} r
    join {{ ref('clean_snapshot_batches') }} b using (batch_id)
    where b.feed_name = 'crashes'

), normalized as (

    select
        *,
        coalesce(
            parsed_crash_id,
            case when nullif(trim(report_state), '') is not null
                       and nullif(trim(report_number), '') is not null
                       and parsed_event_date is not null
                       and parsed_report_time is not null
                       and parsed_report_seq_no is not null
                 then {{ portable_crash_source_key(
                     'trim(report_state)', 'trim(report_number)',
                     'parsed_event_date', 'parsed_report_time', 'parsed_report_seq_no'
                 ) }}
            end
        ) as source_record_key,
        {{ portable_reason_array(parse_reason_expressions) }} as parse_reasons
    from source_rows

), classified as (

    select
        *,
        case
            when {{ portable_array_length('parse_reasons') }} > 0 then 'excluded'
            when source_record_key is null then 'excluded'
            when parsed_usdot_number is null then 'excluded'
            when parsed_federal_recordable is distinct from true then 'excluded'
            when nullif(trim(report_state), '') is null
              or nullif(trim(report_number), '') is null
              or parsed_event_date is null
              or parsed_report_time is null then 'excluded'
            else 'eligible'
        end as model_eligibility,
        case
            when {{ portable_array_contains('parse_reasons', "'invalid_event_date'") }} then 'invalid_event_date'
            when {{ portable_array_length('parse_reasons') }} > 0 then {{ portable_array_first('parse_reasons') }}
            when source_record_key is null then 'missing_source_record_key'
            when parsed_usdot_number is null then 'missing_usdot_number'
            when parsed_federal_recordable is distinct from true
                then 'not_federally_recordable'
            when nullif(trim(report_state), '') is null
              or nullif(trim(report_number), '') is null
              or parsed_event_date is null
              or parsed_report_time is null then 'invalid_incident_key'
        end as exclusion_reason
    from normalized

)

select
    batch_id,
    source_row_number,
    'crashes'::text as feed_name,
    source_record_key,
    parsed_usdot_number as usdot_number,
    parsed_event_date as event_date,
    parsed_source_add_at as source_add_at,
    parsed_source_change_at as source_change_at,
    observed_at as first_observed_at,
    ({{ dbt.dateadd('day', 1, 'parsed_source_add_at') }})::date as source_proxy_reported_date,
    {{ dbt.dateadd('day', 1, 'parsed_source_add_at') }} as source_proxy_valid_from,
    nullif(trim(report_state), '') as report_state,
    nullif(trim(report_number), '') as report_number,
    parsed_report_time as report_time,
    parsed_report_seq_no as report_seq_no,
    parsed_fatalities as fatalities,
    parsed_injuries as injuries,
    parsed_tow_away as tow_away,
    parsed_federal_recordable as federal_recordable,
    model_eligibility,
    exclusion_reason,
    parse_reasons,
    {{ portable_payload_hash([
        'parsed_usdot_number',
        portable_payload_date('parsed_event_date'),
        portable_payload_timestamp('parsed_source_add_at'),
        portable_payload_timestamp('parsed_source_change_at'),
        "nullif(trim(report_state), '')",
        "nullif(trim(report_number), '')",
        'parsed_report_time',
        'parsed_report_seq_no',
        'parsed_fatalities',
        'parsed_injuries',
        'parsed_tow_away',
        'parsed_federal_recordable',
        'parse_reasons'
    ]) }} as record_hash
from classified
