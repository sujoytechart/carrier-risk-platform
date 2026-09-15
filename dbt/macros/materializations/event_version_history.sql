{% materialization event_version_history, adapter='postgres' %}
    {%- set target = this -%}
    {%- set publication = ref('event_change_candidates') -%}

    {# Repeating index DDL on existing history can hold relation locks while
       waiting for a feed lock. Initialize once; always verify the contract. #}
    {% if load_cached_relation(target) is none %}
        {% call statement('create_history', auto_begin=true) %}
            {{ create_event_history_tables(target) }}
        {% endcall %}
    {% endif %}

    {# The model SQL is candidate input, so validate the actual persisted output. #}
    {% if not config.get('contract').enforced %}
        {{ exceptions.raise_compiler_error('event_versions requires an enforced contract') }}
    {% endif %}
    {% do assert_event_history_contract(target) %}

    {% call statement('main', auto_begin=true) %}
        create temporary table event_change_candidates on commit drop as
        select candidates.*
        from ({{ sql }}) candidates
        where not exists (
            select 1 from {{ target.schema }}.event_version_batches applied
            where applied.batch_id = candidates.batch_id
        );
        create index on event_change_candidates (batch_id, feed_name, source_record_key);
        analyze event_change_candidates;

        do $event_history$
        declare
            pending_batch record;
            feed_has_history boolean;
            minimum_inspection_date date;
        begin
            for pending_batch in
                select batch_id, feed_name, observed_at
                from {{ publication.schema }}.event_candidate_batches batches
                where not exists (
                    select 1 from {{ target.schema }}.event_version_batches applied
                    where applied.batch_id = batches.batch_id
                )
                order by observed_at, batch_id
            loop
                perform pg_advisory_xact_lock(hashtextextended(pending_batch.feed_name, 0));
                -- A cursor can predate a competing transaction's commit.
                if exists (
                    select 1 from {{ target.schema }}.event_version_batches applied
                    where applied.batch_id = pending_batch.batch_id
                ) then
                    continue;
                end if;
                if exists (
                    select 1 from {{ target.schema }}.event_version_batches applied
                    where applied.feed_name = pending_batch.feed_name
                      and applied.observed_at > pending_batch.observed_at
                ) then
                    raise exception
                        'Batch % is older than applied % history; run the controlled rebuild',
                        pending_batch.batch_id, pending_batch.feed_name;
                end if;
                {{ apply_event_history_batch(target) }}
            end loop;
        end
        $event_history$;
    {% endcall %}

    {{ adapter.commit() }}
    {{ return({'relations': [target]}) }}
{% endmaterialization %}
