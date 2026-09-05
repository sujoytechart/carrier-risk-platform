{# Nested CASE guards are deliberate: PostgreSQL may reorder AND predicates. #}
{% macro parse_source_date(expression) -%}
    case when btrim({{ expression }}) ~ '^[0-9]{8}$' then
        case when substring(btrim({{ expression }}), 1, 4)::integer between 1 and 9999
                   and substring(btrim({{ expression }}), 5, 2)::integer between 1 and 12
        then
            case when substring(btrim({{ expression }}), 7, 2)::integer between 1 and
                extract(day from (
                    make_date(substring(btrim({{ expression }}), 1, 4)::integer,
                              substring(btrim({{ expression }}), 5, 2)::integer, 1)
                    + interval '1 month' - interval '1 day'
                ))
            then make_date(substring(btrim({{ expression }}), 1, 4)::integer,
                           substring(btrim({{ expression }}), 5, 2)::integer,
                           substring(btrim({{ expression }}), 7, 2)::integer)
            end
        end
    end
{%- endmacro %}

{% macro parse_source_timestamp(expression) -%}
    case when btrim({{ expression }}) ~ '^[0-9]{8} [0-9]{4}$' then
        case when substring(btrim({{ expression }}), 10, 2)::integer between 0 and 23
                   and substring(btrim({{ expression }}), 12, 2)::integer between 0 and 59
        then {{ parse_source_date('substring(btrim(' ~ expression ~ '), 1, 8)') }}
            + make_interval(hours => substring(btrim({{ expression }}), 10, 2)::integer,
                            mins => substring(btrim({{ expression }}), 12, 2)::integer)
        end
    end
{%- endmacro %}

{% macro parse_nonnegative_integer(expression, maximum=2147483647) -%}
    {%- set digits = "coalesce(nullif(ltrim(split_part(ltrim(btrim(" ~ expression
        ~ "), '+'), '.', 1), '0'), ''), '0')" -%}
    case when btrim({{ expression }}) ~ '^[+]?[0-9]+([.]0+)?$' then
        case when length({{ digits }}) <= length('{{ maximum }}') then
            case when {{ digits }}::numeric <= {{ maximum }}
                 then {{ digits }}::bigint end
        end
    end
{%- endmacro %}

{% macro normalize_positive_integer(expression) -%}
    nullif({{ parse_nonnegative_integer(expression, 9223372036854775807) }}, 0)::text
{%- endmacro %}

{% macro parse_report_time(expression) -%}
    case when {{ parse_nonnegative_integer(expression, 2359) }} % 100 <= 59
         then {{ parse_nonnegative_integer(expression, 2359) }} end
{%- endmacro %}

{% macro parse_source_boolean(expression) -%}
    case lower(btrim({{ expression }}))
        when 'y' then true
        when 'yes' then true
        when '1' then true
        when 'true' then true
        when 'n' then false
        when 'no' then false
        when '0' then false
        when 'false' then false
        else null
    end
{%- endmacro %}
