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
    join {{ source('raw', 'snapshot_batches') }} b using (batch_id)
    where b.status = 'loaded'

), normalized as (

    select
        *,
        array_remove(array[
            case when nullif(btrim(inspection_id), '') is not null
                       and parsed_source_key is null
                 then 'invalid_source_record_key' end,
            case when nullif(btrim(dot_number), '') is not null
                       and parsed_usdot_number is null
                 then 'invalid_usdot_number' end,
            case when nullif(btrim(insp_date), '') is not null
                       and parsed_event_date is null
                 then 'invalid_event_date' end,
            case when nullif(btrim(mcmis_add_date), '') is not null
                       and parsed_source_add_at is null
                 then 'invalid_source_add_at' end,
            case when nullif(btrim(viol_total), '') is not null
                       and parsed_violation_count is null
                 then 'invalid_violation_count' end,
            case when nullif(btrim(oos_total), '') is not null
                       and parsed_oos_count is null
                 then 'invalid_oos_violation_count' end,
            case when parsed_event_date is not null
                       and parsed_source_add_at is not null
                       and (parsed_source_add_at + interval '1 day')::date
                           <= parsed_event_date
                 then 'impossible_event_chronology' end
        ]::text[], null) as parse_reasons
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
    (parsed_source_add_at + interval '1 day')::date as source_proxy_reported_date,
    parsed_source_add_at + interval '1 day' as source_proxy_valid_from,
    nullif(btrim(report_state), '') as state,
    parsed_violation_count as violation_count,
    parsed_oos_count as oos_violation_count,
    parse_reasons,
    md5(concat_ws(chr(31),
        parsed_usdot_number,
        parsed_event_date::text,
        nullif(btrim(report_state), ''),
        coalesce(parsed_violation_count, 0)::text,
        coalesce(parsed_oos_count, 0)::text
    )) as record_hash
from normalized
