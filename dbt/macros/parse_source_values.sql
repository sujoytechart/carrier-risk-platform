{% macro parse_source_date(expression) -%}
    case
        when nullif(btrim({{ expression }}), '') is null then null
        when btrim({{ expression }}) ~ '^[0-9]{8}$'
         and substring(btrim({{ expression }}), 1, 4)::integer between 1 and 9999
         and substring(btrim({{ expression }}), 5, 2)::integer between 1 and 12
         and substring(btrim({{ expression }}), 7, 2)::integer between 1 and 31
         and to_char(to_date(btrim({{ expression }}), 'YYYYMMDD'), 'YYYYMMDD')
             = btrim({{ expression }})
        then to_date(btrim({{ expression }}), 'YYYYMMDD')
        else null
    end
{%- endmacro %}

{% macro parse_source_timestamp(expression) -%}
    case
        when nullif(btrim({{ expression }}), '') is null then null
        when btrim({{ expression }}) ~ '^[0-9]{8} [0-9]{4}$'
         and substring(btrim({{ expression }}), 1, 4)::integer between 1 and 9999
         and substring(btrim({{ expression }}), 5, 2)::integer between 1 and 12
         and substring(btrim({{ expression }}), 7, 2)::integer between 1 and 31
         and substring(btrim({{ expression }}), 10, 2)::integer between 0 and 23
         and substring(btrim({{ expression }}), 12, 2)::integer between 0 and 59
         and to_char(
                to_timestamp(btrim({{ expression }}), 'YYYYMMDD HH24MI'),
                'YYYYMMDD HH24MI'
             ) = btrim({{ expression }})
        then to_timestamp(btrim({{ expression }}), 'YYYYMMDD HH24MI')::timestamp
        else null
    end
{%- endmacro %}

{% macro normalize_positive_integer(expression) -%}
    case
        when nullif(btrim({{ expression }}), '') is null then null
        when btrim({{ expression }}) ~ '^[+]?[0-9]+([.]0+)?$'
         and btrim({{ expression }})::numeric > 0
        then btrim({{ expression }})::numeric::bigint::text
        else null
    end
{%- endmacro %}

{% macro parse_nonnegative_integer(expression) -%}
    case
        when nullif(btrim({{ expression }}), '') is null then null
        when btrim({{ expression }}) ~ '^[+]?[0-9]+([.]0+)?$'
        then btrim({{ expression }})::numeric::bigint
        else null
    end
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
