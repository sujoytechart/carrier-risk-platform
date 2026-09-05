with source_rows as (

    select
        r.*,
        b.observed_at,
        {{ normalize_positive_integer('r.crash_id') }} as parsed_crash_id,
        {{ normalize_positive_integer('r.dot_number') }} as parsed_usdot_number,
        {{ parse_source_date('r.report_date') }} as parsed_event_date,
        {{ parse_source_timestamp('r.add_date') }} as parsed_source_add_at,
        {{ parse_source_timestamp('r.change_date') }} as parsed_source_change_at,
        {{ parse_nonnegative_integer('r.report_time') }} as parsed_report_time,
        {{ parse_nonnegative_integer('r.report_seq_no') }} as parsed_report_seq_no,
        {{ parse_nonnegative_integer('r.fatalities') }} as parsed_fatalities,
        {{ parse_nonnegative_integer('r.injuries') }} as parsed_injuries,
        {{ parse_source_boolean('r.tow_away') }} as parsed_tow_away,
        {{ parse_source_boolean('r.federal_recordable') }} as parsed_federal_recordable
    from {{ source('raw', 'crash_rows') }} r
    join {{ source('raw', 'snapshot_batches') }} b using (batch_id)
    where b.status = 'loaded'

), normalized as (

    select
        *,
        coalesce(
            parsed_crash_id,
            case when nullif(btrim(report_state), '') is not null
                       and nullif(btrim(report_number), '') is not null
                       and parsed_event_date is not null
                       and parsed_report_time is not null
                       and parsed_report_seq_no is not null
                 then encode(convert_to(json_build_array(
                     btrim(report_state), btrim(report_number),
                     parsed_event_date, parsed_report_time, parsed_report_seq_no
                 )::text, 'UTF8'), 'hex')
            end
        ) as source_record_key,
        array_remove(array[
            case when nullif(btrim(crash_id), '') is not null
                       and parsed_crash_id is null
                 then 'invalid_source_record_key' end,
            case when nullif(btrim(dot_number), '') is not null
                       and parsed_usdot_number is null
                 then 'invalid_usdot_number' end,
            case when nullif(btrim(report_date), '') is not null
                       and parsed_event_date is null
                 then 'invalid_event_date' end,
            case when nullif(btrim(add_date), '') is not null
                       and parsed_source_add_at is null
                 then 'invalid_source_add_at' end,
            case when nullif(btrim(federal_recordable), '') is not null
                       and parsed_federal_recordable is null
                 then 'invalid_federal_recordable' end,
            case when parsed_event_date is not null
                       and parsed_source_add_at is not null
                       and (parsed_source_add_at + interval '1 day')::date
                           <= parsed_event_date
                 then 'impossible_event_chronology' end
        ]::text[], null) as parse_reasons
    from source_rows

), classified as (

    select
        *,
        case
            when cardinality(parse_reasons) > 0 then 'excluded'
            when parsed_usdot_number is null then 'excluded'
            when parsed_federal_recordable is distinct from true then 'excluded'
            when nullif(btrim(report_state), '') is null
              or nullif(btrim(report_number), '') is null
              or parsed_event_date is null
              or parsed_report_time is null then 'excluded'
            else 'eligible'
        end as model_eligibility,
        case
            when 'invalid_event_date' = any(parse_reasons) then 'invalid_event_date'
            when cardinality(parse_reasons) > 0 then parse_reasons[1]
            when parsed_usdot_number is null then 'missing_usdot_number'
            when parsed_federal_recordable is distinct from true
                then 'not_federally_recordable'
            when nullif(btrim(report_state), '') is null
              or nullif(btrim(report_number), '') is null
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
    (parsed_source_add_at + interval '1 day')::date as source_proxy_reported_date,
    parsed_source_add_at + interval '1 day' as source_proxy_valid_from,
    nullif(btrim(report_state), '') as report_state,
    nullif(btrim(report_number), '') as report_number,
    parsed_report_time as report_time,
    parsed_report_seq_no as report_seq_no,
    parsed_fatalities as fatalities,
    parsed_injuries as injuries,
    parsed_tow_away as tow_away,
    parsed_federal_recordable as federal_recordable,
    model_eligibility,
    exclusion_reason,
    parse_reasons,
    md5(concat_ws(chr(31),
        parsed_usdot_number,
        parsed_event_date::text,
        nullif(btrim(report_state), ''),
        nullif(btrim(report_number), ''),
        coalesce(parsed_report_time, 0)::text,
        coalesce(parsed_report_seq_no, 0)::text,
        coalesce(parsed_fatalities, 0)::text,
        coalesce(parsed_injuries, 0)::text,
        coalesce(parsed_tow_away, false)::text,
        coalesce(parsed_federal_recordable, false)::text
    )) as record_hash
from classified
