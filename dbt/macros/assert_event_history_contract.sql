{% macro assert_event_history_contract(target) %}
    {% do get_assert_columns_equivalent('select * from ' ~ target) %}
    {# dbt's query-schema inference normalizes varchar to text. The history
       contract uses unmodified scalar types; inspect the catalog as well so
       narrowing a text column to varchar(n) cannot pass that normalization. #}
    {% call statement('actual_type_contract', auto_begin=true) %}
        do $contract$
        begin
            {% for name, column in model.columns.items() %}
                if not exists (
                    select 1 from pg_attribute
                    where attrelid = '{{ target }}'::regclass
                      and attname = '{{ name }}' and not attisdropped
                      and atttypid = '{{ column.data_type }}'::regtype
                      and atttypmod = -1
                ) then
                    raise exception 'event_versions contract: {{ name }} requires {{ column.data_type }}';
                end if;
            {% endfor %}
        end
        $contract$;
    {% endcall %}
{% endmacro %}
