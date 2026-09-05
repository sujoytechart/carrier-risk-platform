{% macro assert_build_lock() %}
    {# dbt closes its hook connection before workers start. The launcher owns
       this session lock for the entire subprocess, including relation swaps. #}
    {% if execute and flags.WHICH in ['run', 'build', 'seed'] %}
        {% set owner = env_var('CARRIER_RISK_DBT_LOCK_PID', '') %}
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
    {% else %}
        select 1;
    {% endif %}
{% endmacro %}
