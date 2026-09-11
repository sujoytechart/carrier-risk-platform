{# Integer aliases are NUMBER(38,0) on Snowflake. Explicit TEXT width prevents
   dbt's normalizing introspection from accepting a narrowed VARCHAR column. #}
{% macro snowflake_history_type(data_type) -%}
    {% if data_type | lower == 'text' %}varchar(16777216)
    {% elif data_type | lower in ['bigint', 'integer'] %}number(38,0)
    {% elif data_type | lower == 'timestamp_tz' %}timestamp_tz(9)
    {% else %}{{ data_type }}{% endif %}
{%- endmacro %}

{% macro snowflake_type_matches(data_type) %}
    {% if data_type | lower == 'text' %}
        data_type = 'TEXT' and character_maximum_length >= 16777216
    {% elif data_type | lower in ['bigint', 'integer'] %}
        data_type = 'NUMBER' and numeric_precision = 38 and numeric_scale = 0
    {% elif data_type | lower == 'timestamp_tz' %}
        data_type = 'TIMESTAMP_TZ'
    {% else %}
        data_type = '{{ data_type | upper }}'
    {% endif %}
{% endmacro %}

{% macro snowflake_assert_relation_contract(relation, columns, identity=false) %}
    {# INFORMATION_SCHEMA.COLUMNS.DATETIME_PRECISION is not applicable on
       Snowflake. DESCRIBE retains the persisted timestamp precision.
       https://docs.snowflake.com/en/sql-reference/info-schema/columns
       https://docs.snowflake.com/en/sql-reference/sql/desc-table #}
    {% if execute %}
        {% set described_columns = run_query('describe table ' ~ relation) %}
        {% for actual in described_columns.rows %}
            {% set expected = columns.get(actual['name'] | lower) %}
            {% if expected and expected.data_type | lower == 'timestamp_tz'
                  and actual['type'] | upper != 'TIMESTAMP_TZ(9)' %}
                {{ exceptions.raise_compiler_error(
                    'Persisted event history schema contract: ' ~ relation ~ '.' ~ actual['name']
                    ~ ' requires TIMESTAMP_TZ(9), found ' ~ actual['type']
                ) }}
            {% endif %}
        {% endfor %}
    {% endif %}
    {% set required_history_columns = ['event_version_key', 'feed_name', 'event_type', 'source_record_key',
        'reported_date', 'knowledge_valid_from', 'availability_quality', 'first_observed_at',
        'is_model_eligible', 'is_current', 'is_deleted', 'record_hash', 'first_seen_batch_id', 'last_seen_batch_id'] %}
    {% set database_name = relation.database if relation.quote_policy.database else relation.database | upper %}
    {% set schema_name = relation.schema if relation.quote_policy.schema else relation.schema | upper %}
    {% set table_name = relation.identifier if relation.quote_policy.identifier else relation.identifier | upper %}
    execute immediate $$
    declare
        violations integer;
        invalid_contract exception (-20004, 'Persisted event history schema contract mismatch; explicit migration is required');
    begin
        select count(*) into :violations from (
            select 1 as invalid
            where (select count(*) from {{ adapter.quote(database_name) }}.information_schema.columns
                   where table_schema = '{{ schema_name }}' and table_name = '{{ table_name }}') <> {{ columns | length }}
            {% for name, column in columns.items() %}
            union all
            select 1 where not exists (
                select 1 from {{ adapter.quote(database_name) }}.information_schema.columns
                where table_schema = '{{ schema_name }}' and table_name = '{{ table_name }}'
                  and column_name = '{{ name | upper }}'
                  and {{ snowflake_type_matches(column.data_type) }}
                  and is_nullable = '{{ 'NO' if not identity or name in required_history_columns else 'YES' }}'
                  {% if identity and name == 'event_version_key' %}
                  and is_identity = 'YES'
                  {% endif %}
            )
            {% endfor %}
        ) invalid_columns;
        if (violations > 0) then
            raise invalid_contract;
        end if;
    end;
    $$;
{% endmacro %}

{% macro snowflake_create_history_tables(target, applied, retention) %}
    create table if not exists {{ target }} (
        {% for name, column in model.columns.items() %}
        {{ name }} {{ snowflake_history_type(column.data_type) }}
        {% if name == 'event_version_key' %}autoincrement start 1 increment 1{% endif %}
        {% if name in ['event_version_key', 'feed_name', 'event_type', 'source_record_key',
                       'reported_date', 'knowledge_valid_from', 'availability_quality',
                       'first_observed_at', 'is_model_eligible', 'is_current', 'is_deleted',
                       'record_hash', 'first_seen_batch_id', 'last_seen_batch_id'] %}not null{% endif %}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    );
    create table if not exists {{ applied }} (
        batch_id varchar(16777216) not null,
        feed_name varchar(16777216) not null,
        observed_at timestamp_tz(9) not null,
        applied_at timestamp_tz(9) not null default current_timestamp()
    );
    create table if not exists {{ retention }} (
        source_record_key varchar(16777216) not null,
        batch_id varchar(16777216) not null,
        event_version_key number(38,0) not null,
        observed_at timestamp_tz(9) not null,
        retention_cutoff date not null
    );
{% endmacro %}
