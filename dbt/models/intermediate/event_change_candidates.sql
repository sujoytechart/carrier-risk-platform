with inspections as (

    select
        batch_id,
        first_observed_at as observed_at,
        feed_name,
        'inspection'::text as event_type,
        source_record_key,
        usdot_number,
        event_date,
        source_proxy_reported_date,
        source_proxy_valid_from,
        source_add_at,
        source_change_at,
        state,
        violation_count,
        oos_violation_count,
        null::text as report_state,
        null::text as report_number,
        null::integer as report_time,
        null::integer as report_seq_no,
        null::integer as fatalities,
        null::integer as injuries,
        null::boolean as tow_away,
        record_hash,
        cardinality(parse_reasons) = 0
            and source_record_key is not null
            and usdot_number is not null
            and event_date is not null
            and source_proxy_reported_date is not null
            and source_proxy_valid_from::date > event_date
            as is_model_eligible
    from {{ ref('clean_inspections') }}

), crashes as (

    select
        batch_id,
        first_observed_at as observed_at,
        feed_name,
        'crash'::text as event_type,
        source_record_key,
        usdot_number,
        event_date,
        source_proxy_reported_date,
        source_proxy_valid_from,
        source_add_at,
        source_change_at,
        null::text as state,
        null::integer as violation_count,
        null::integer as oos_violation_count,
        report_state,
        report_number,
        report_time,
        report_seq_no,
        fatalities,
        injuries,
        tow_away,
        record_hash,
        model_eligibility = 'eligible'
            and source_proxy_valid_from::date > event_date
            as is_model_eligible
    from {{ ref('clean_crashes') }}

)

select * from inspections
union all
select * from crashes
