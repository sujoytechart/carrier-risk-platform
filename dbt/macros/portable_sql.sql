{# Adapter boundaries keep warehouse syntax out of temporal business rules. #}
{% macro portable_array_length(expression) -%}
    {{ return(adapter.dispatch('portable_array_length', 'carrier_risk_platform')(expression)) }}
{%- endmacro %}
{% macro postgres__portable_array_length(expression) -%}cardinality({{ expression }}){%- endmacro %}
{% macro snowflake__portable_array_length(expression) -%}array_size({{ expression }}){%- endmacro %}

{% macro portable_array_first(expression) -%}
    {{ return(adapter.dispatch('portable_array_first', 'carrier_risk_platform')(expression)) }}
{%- endmacro %}
{% macro postgres__portable_array_first(expression) -%}({{ expression }})[1]{%- endmacro %}
{% macro snowflake__portable_array_first(expression) -%}get({{ expression }}, 0)::varchar{%- endmacro %}

{% macro portable_array_contains(expression, value) -%}
    {{ return(adapter.dispatch('portable_array_contains', 'carrier_risk_platform')(expression, value)) }}
{%- endmacro %}
{% macro postgres__portable_array_contains(expression, value) -%}{{ value }} = any({{ expression }}){%- endmacro %}
{% macro snowflake__portable_array_contains(expression, value) -%}array_contains({{ value }}::variant, {{ expression }}){%- endmacro %}

{% macro portable_reason_array(reasons) -%}
    {{ return(adapter.dispatch('portable_reason_array', 'carrier_risk_platform')(reasons)) }}
{%- endmacro %}
{% macro postgres__portable_reason_array(reasons) -%}array_remove(array[{{ reasons if reasons is string else reasons | join(',\n') }}]::text[], null){%- endmacro %}
{% macro snowflake__portable_reason_array(reasons) -%}array_construct_compact({{ reasons if reasons is string else reasons | join(',\n') }}){%- endmacro %}

{% macro portable_utc_timestamp(expression) -%}
    {{ return(adapter.dispatch('portable_utc_timestamp', 'carrier_risk_platform')(expression)) }}
{%- endmacro %}
{% macro postgres__portable_utc_timestamp(expression) -%}({{ expression }})::timestamp at time zone 'UTC'{%- endmacro %}
{% macro snowflake__portable_utc_timestamp(expression) -%}
    to_timestamp_tz(to_char(({{ expression }})::timestamp_ntz, 'YYYY-MM-DD HH24:MI:SS.FF6') || ' +00:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
{%- endmacro %}

{% macro portable_utc_date(expression) -%}
    {{ return(adapter.dispatch('portable_utc_date', 'carrier_risk_platform')(expression)) }}
{%- endmacro %}
{% macro postgres__portable_utc_date(expression) -%}({{ expression }} at time zone 'UTC')::date{%- endmacro %}
{% macro snowflake__portable_utc_date(expression) -%}convert_timezone('UTC', {{ expression }})::date{%- endmacro %}

{% macro portable_month_grid(relation) -%}
    {{ return(adapter.dispatch('portable_month_grid', 'carrier_risk_platform')(relation)) }}
{%- endmacro %}
{% macro postgres__portable_month_grid(relation) -%}
    select generate_series(
        date_trunc('month', min(observed_at) at time zone 'UTC'),
        date_trunc('month', max(observed_at) at time zone 'UTC'),
        interval '1 month'
    )::date as scoring_date from {{ relation }}
{%- endmacro %}
{% macro snowflake__portable_month_grid(relation) -%}
    with recursive bounds as (
        select date_trunc('month', {{ portable_utc_date('min(observed_at)') }}) as first_month,
               date_trunc('month', {{ portable_utc_date('max(observed_at)') }}) as last_month
        from {{ relation }}
    ), months(scoring_date, last_month) as (
        select first_month, last_month from bounds where first_month is not null
        union all
        select dateadd(month, 1, scoring_date)::date, last_month
        from months where scoring_date < last_month
    )
    select scoring_date from months
{%- endmacro %}

{% macro portable_ratio(numerator, denominator) -%}
    {# Inputs are nonnegative bigint counts. Work in millionths: dividing a
       multiple of the denominator yields an exact integer, then the remainder
       decides half-up rounding. This avoids Snowflake division rounding a value
       just below a halfway point up before a second round to six places.
       bigint * 1,000,000 fits in 25 digits; no fractional division is needed.
       https://docs.snowflake.com/en/sql-reference/operators-arithmetic #}
    {%- set scaled = '(cast(' ~ numerator ~ ' as decimal(38, 0)) * 1000000)' -%}
    {%- set divisor = 'nullif(' ~ denominator ~ ', 0)' -%}
    {%- set remainder = 'mod(' ~ scaled ~ ', ' ~ divisor ~ ')' -%}
    cast((
        cast(({{ scaled }} - {{ remainder }}) / {{ divisor }} as decimal(25, 0))
        + case when 2 * {{ remainder }} >= {{ divisor }} then 1 else 0 end
    ) * 0.000001 as decimal(38, 6))
{%- endmacro %}

{% macro portable_incident_key(usdot, state, report_number, event_date, report_time) -%}
    md5(concat_ws(chr(31), {{ usdot }}, {{ state }}, {{ report_number }},
        to_char({{ event_date }}, 'YYYY-MM-DD'), cast({{ report_time }} as varchar)))
{%- endmacro %}

{% macro portable_crash_source_key(state, report_number, event_date, report_time, sequence) -%}
    {{ return(adapter.dispatch('portable_crash_source_key', 'carrier_risk_platform')(state, report_number, event_date, report_time, sequence)) }}
{%- endmacro %}
{% macro postgres__portable_crash_source_key(state, report_number, event_date, report_time, sequence) -%}
    'fallback:' || encode(convert_to(json_build_array(
        {{ state }}, {{ report_number }}, {{ event_date }}, {{ report_time }}, {{ sequence }}
    )::text, 'UTF8'), 'hex')
{%- endmacro %}

{# PostgreSQL json_build_array scalar encoding, including its spaces, is part of
   existing source identity. Serialize each already-formatted string separately
   so Snowflake owns JSON escaping while this macro retains PostgreSQL's array
   separators and explicit date/number formatting. #}
{% macro snowflake__portable_json_string(expression) -%}
    to_json(to_variant({{ expression }}))
{%- endmacro %}
{% macro snowflake__portable_crash_source_key(state, report_number, event_date, report_time, sequence) -%}
    'fallback:' || lower(hex_encode('[' ||
        {{ snowflake__portable_json_string(state) }} || ', ' ||
        {{ snowflake__portable_json_string(report_number) }} || ', ' ||
        {{ snowflake__portable_json_string("to_char(" ~ event_date ~ ", 'YYYY-MM-DD')") }} || ', ' ||
        cast({{ report_time }} as varchar) || ', ' || cast({{ sequence }} as varchar) || ']'))
{%- endmacro %}

{% macro portable_payload_hash(expressions) -%}
    {{ return(adapter.dispatch('portable_payload_hash', 'carrier_risk_platform')(expressions)) }}
{%- endmacro %}
{% macro postgres__portable_payload_hash(expressions) -%}
    md5(jsonb_build_array({{ expressions | join(',\n') }})::text)
{%- endmacro %}
{% macro snowflake__portable_payload_hash(expressions) -%}
    {# Snowflake-specific JSON bytes: encode SQL NULL as JSON null explicitly.
       Dates/timestamps must be explicitly formatted by the caller. #}
    md5(to_json(array_construct(
        {% for expression in expressions %}
        coalesce(to_variant({{ expression }}), parse_json('null')){% if not loop.last %},{% endif %}
        {% endfor %}
    )))
{%- endmacro %}

{% macro portable_payload_date(expression) -%}
    {% if target.type == 'snowflake' %}to_char({{ expression }}, 'YYYY-MM-DD'){% else %}{{ expression }}{% endif %}
{%- endmacro %}
{% macro portable_payload_timestamp(expression) -%}
    {% if target.type == 'snowflake' %}to_char({{ expression }}, 'YYYY-MM-DD"T"HH24:MI:SS.FF6'){% else %}{{ expression }}{% endif %}
{%- endmacro %}

{% macro portable_bool_or(expression) -%}
    {{ return(adapter.dispatch('portable_bool_or', 'carrier_risk_platform')(expression)) }}
{%- endmacro %}
{% macro postgres__portable_bool_or(expression) -%}bool_or({{ expression }}){%- endmacro %}
{% macro snowflake__portable_bool_or(expression) -%}boolor_agg({{ expression }}){%- endmacro %}
{% macro portable_bool_and(expression) -%}
    {{ return(adapter.dispatch('portable_bool_and', 'carrier_risk_platform')(expression)) }}
{%- endmacro %}
{% macro postgres__portable_bool_and(expression) -%}bool_and({{ expression }}){%- endmacro %}
{% macro snowflake__portable_bool_and(expression) -%}booland_agg({{ expression }}){%- endmacro %}

{% macro portable_hash_aggregate(expression, ordering) -%}
    {{ return(adapter.dispatch('portable_hash_aggregate', 'carrier_risk_platform')(expression, ordering)) }}
{%- endmacro %}
{% macro postgres__portable_hash_aggregate(expression, ordering) -%}md5(string_agg({{ expression }}, chr(31) order by {{ ordering }})){%- endmacro %}
{% macro snowflake__portable_hash_aggregate(expression, ordering) -%}md5(listagg({{ expression }}, '\u001f') within group (order by {{ ordering }})){%- endmacro %}
