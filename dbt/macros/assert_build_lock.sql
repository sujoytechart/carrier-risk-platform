{% macro assert_build_lock() %}
    {# dbt closes its hook connection before workers start. The launcher owns
       this session lock for the entire subprocess, including relation swaps. #}
    {% if execute and flags.WHICH in ['run', 'build', 'seed'] %}
        {% set owner = env_var('CARRIER_RISK_DBT_LOCK_PID', '') %}
        {% if target.type == 'snowflake' %}
            {% if not modules.re.fullmatch('[1-9][0-9]{0,37}', owner) %}
                {{ exceptions.raise_compiler_error(
                    'A warehouse build lock is required. Use python -m orchestration.dbt_cli '
                    ~ '--warehouse-target snowflake ' ~ flags.WHICH
                ) }}
            {% endif %}
            {% set guard_table = target.database | upper ~ '.CARRIER_RISK_CONTROL.BUILD_GUARD' %}
            {% set locks = run_query('SHOW LOCKS') %}
            {% set found = namespace(owned=false) %}
            {% for lock in locks.rows %}
                {% if lock['resource'] == guard_table
                      and lock['transaction'] | string == owner
                      and lock['status'] == 'HOLDING' %}
                    {% set found.owned = true %}
                {% endif %}
            {% endfor %}
            {% if not found.owned %}
                {{ exceptions.raise_compiler_error(
                    'The exact Snowflake build lock is absent; use the locked launcher '
                    ~ 'and recover any unfinished run before retrying'
                ) }}
            {% endif %}
            select 1;
        {% else %}
        {% if not modules.re.fullmatch('[0-9]{1,10}', owner) %}
            {{ exceptions.raise_compiler_error(
                'A warehouse build lock is required. Use python -m orchestration.dbt_cli '
                ~ flags.WHICH
            ) }}
        {% endif %}
        do $build_lock$
        begin
            if not exists (
                select 1 from pg_locks
                where locktype = 'advisory' and granted
                  and mode = 'ExclusiveLock'
                  and pid = {{ owner }}::bigint
                  and classid = 0 and objid = 764301234 and objsubid = 1
                  and database = (select oid from pg_database
                                  where datname = current_database())
            ) then
                raise exception
                    'A warehouse build lock is required. Use python -m orchestration.dbt_cli {{ flags.WHICH }}';
            end if;
        end
        $build_lock$;
        {% endif %}
    {% else %}
        select 1;
    {% endif %}
{% endmacro %}
