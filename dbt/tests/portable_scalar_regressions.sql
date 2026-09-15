-- These fixtures exercise warehouse behavior, including strict parsing and
-- encodings. They deliberately use no federal data or mutable history relations.
with integer_cases as (
    select '+000123.00' as raw_value, 123::bigint as expected
    union all select '0', 0
    union all select '2147483648', null
    union all select '1.2', null
    union all select '-1', null
    union all select '', null
    union all select '999999999999999999999999999999999999999', null
    union all select null, null
    union all select '000000000000000000000000000000000000123', 123
), date_cases as (
    select '20240229' as raw_value, date '2024-02-29' as expected
    union all select '20230229', null
    union all select '20240431', null
    union all select '00000101', null
    union all select '20241301', null
    union all select '', null
), timestamp_cases as (
    select '20240229 2359' as raw_value, cast('2024-02-29 23:59:00' as {{ 'timestamp_ntz' if target.type == 'snowflake' else 'timestamp' }}) as expected
    union all select '20240229 2400', null
    union all select '20240229 1260', null
    union all select '20230229 1200', null
    union all select '20240101 1', null
    union all select '', null
), boolean_cases as (
    select 'yes' as raw_value, true as expected
    union all select ' FALSE ', false
    union all select '1', true
    union all select '0', false
    union all select 'maybe', null
    union all select '', null
    union all select null, null
), failures as (
    select 'integer:' || raw_value as failure
    from integer_cases
    where {{ parse_nonnegative_integer('raw_value') }} is distinct from expected
    union all
    select 'date:' || raw_value from date_cases
    where {{ parse_source_date('raw_value') }} is distinct from expected
    union all
    select 'timestamp:' || raw_value from timestamp_cases
    where {{ parse_source_timestamp('raw_value') }} is distinct from expected
    union all
    select 'boolean:' || coalesce(raw_value, '<null>') from boolean_cases
    where {{ parse_source_boolean('raw_value') }} is distinct from expected
    union all
    select 'report_time_invalid_minutes'
    where {{ parse_report_time("'2360'") }} is not null
    union all
    select 'report_time_integral'
    where {{ parse_report_time("'0930.00'") }} is distinct from 930
    union all
    select 'positive_identifier_maximum'
    where {{ normalize_positive_integer("'+09223372036854775807.0'") }} is distinct from '9223372036854775807'
    union all
    select 'positive_identifier_overflow'
    where {{ normalize_positive_integer("'9223372036854775808'") }} is not null
    union all
    select 'positive_identifier_zero'
    where {{ normalize_positive_integer("'0'") }} is not null
    union all
    select 'fractional_ratio'
    where {{ portable_ratio('1', '3') }} is distinct from 0.333333
    union all
    select 'ratio_below_half_millionth'
    where {{ portable_ratio('1', '2000001') }} is distinct from 0.000000
    union all
    select 'ratio_exact_half_millionth'
    where {{ portable_ratio('1', '2000000') }} is distinct from 0.000001
    union all
    select 'ratio_above_half_millionth'
    where {{ portable_ratio('1', '1999999') }} is distinct from 0.000001
    union all
    select 'ratio_below_one_and_half_millionths'
    where {{ portable_ratio('3', '2000001') }} is distinct from 0.000001
    union all
    select 'ratio_exact_one_and_half_millionths'
    where {{ portable_ratio('3', '2000000') }} is distinct from 0.000002
    union all
    select 'ratio_above_one_and_half_millionths'
    where {{ portable_ratio('3', '1999999') }} is distinct from 0.000002
    union all
    select 'ratio_bigint_maximum'
    where {{ portable_ratio('9223372036854775807', '1') }}
        is distinct from cast(9223372036854775807 as decimal(38, 6))
    union all
    select 'ratio_large_equal_counts'
    where {{ portable_ratio('9223372036854775807', '9223372036854775807') }}
        is distinct from 1.000000
    union all
    select 'null_numerator_ratio'
    where {{ portable_ratio('null', '3') }} is not null
    union all
    select 'null_denominator_ratio'
    where {{ portable_ratio('1', 'null') }} is not null
    union all
    select 'null_ratio'
    where {{ portable_ratio('1', '0') }} is not null
    union all
    select 'quarantine_order'
    where {{ portable_array_first(portable_reason_array(["null", "'second'", "'third'"])) }} is distinct from 'second'
    union all
    select 'empty_quarantine'
    where {{ portable_array_length(portable_reason_array(["null"])) }} is distinct from 0
    union all
    select 'quarantine_membership'
    where {{ portable_array_contains(portable_reason_array(["null", "'invalid_event_date'"]), "'invalid_event_date'") }} is distinct from true
    union all
    select 'payload_null_zero'
    where {{ portable_payload_hash(['null', '1']) }} is not distinct from {{ portable_payload_hash(['0', '1']) }}
    union all
    select 'payload_null_position'
    where {{ portable_payload_hash(['null', '1']) }} is not distinct from {{ portable_payload_hash(['1', 'null']) }}
    union all
    {% if target.type == 'postgres' %}
    select 'postgres_payload_compatibility'
    where {{ portable_payload_hash([
        "'123'::text", "date '2024-02-29'", "timestamp '2024-02-29 23:59:00'",
        'null', '0', 'false', portable_reason_array(["'invalid_event_date'"])
    ]) }} is distinct from md5('["123", "2024-02-29", "2024-02-29T23:59:00", null, 0, false, ["invalid_event_date"]]')
    union all
    {% endif %}
    select 'utc_midnight'
    where {{ portable_utc_timestamp("date '2024-02-29'") }} is distinct from
        cast('2024-02-29 00:00:00+00:00' as {{ 'timestamp_tz' if target.type == 'snowflake' else 'timestamptz' }})
    union all
    select 'source_identity'
    where {{ portable_crash_source_key("'MI'", "'A42'", "date '2024-02-29'", '930', '2') }}
        is distinct from 'fallback:5b224d49222c2022413432222c2022323032342d30322d3239222c203933302c20325d'
    union all
    select 'incident_identity'
    where {{ portable_incident_key("'123'", "'MI'", "'A42'", "date '2024-02-29'", '930') }}
        is distinct from md5('123' || chr(31) || 'MI' || chr(31) || 'A42' || chr(31) || '2024-02-29' || chr(31) || '930')
)
select * from failures
