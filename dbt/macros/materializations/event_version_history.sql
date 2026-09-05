{% materialization event_version_history, adapter='postgres' %}
    {%- set target = this -%}

    {% call statement('main', auto_begin=true) %}
        create schema if not exists {{ target.schema }};

        create table if not exists {{ target }} (
            event_version_key bigint generated always as identity primary key,
            feed_name text not null,
            source_record_key text not null,
            usdot_number text not null,
            event_date date not null,
            reported_date date not null,
            knowledge_valid_from timestamptz not null,
            knowledge_valid_to timestamptz,
            availability_quality text not null
                check (availability_quality in ('source_proxy', 'observed')),
            is_current boolean not null,
            is_deleted boolean not null,
            deletion_reason text,
            record_hash text not null,
            first_seen_batch_id text not null,
            last_seen_batch_id text not null,
            predecessor_version_key bigint,
            superseded_by_version_key bigint,
            state text,
            violation_count integer,
            oos_violation_count integer,
            report_state text,
            report_number text,
            report_time integer,
            report_seq_no integer,
            fatalities integer,
            injuries integer,
            tow_away boolean,
            check (event_date < reported_date),
            check (
                knowledge_valid_to is null
                or knowledge_valid_from < knowledge_valid_to
            ),
            check (is_current = (knowledge_valid_to is null)),
            check ((not is_deleted) or deletion_reason is not null)
        );

        create table if not exists {{ target.schema }}.event_version_batches (
            batch_id text primary key,
            feed_name text not null,
            observed_at timestamptz not null,
            applied_at timestamptz not null default current_timestamp
        );

        create unique index if not exists event_versions_batch_source_version
            on {{ target }} (
                first_seen_batch_id,
                feed_name,
                source_record_key,
                is_deleted
            );

        create temporary table event_change_candidates
        on commit drop
        as {{ sql }};

        do $event_history$
        declare
            pending_batch record;
            feed_has_history boolean;
            minimum_inspection_date date;
        begin
            for pending_batch in
                select batch_id, feed_name, observed_at
                from raw.snapshot_batches batches
                where batches.status = 'loaded'
                  and not exists (
                      select 1
                      from {{ target.schema }}.event_version_batches applied
                      where applied.batch_id = batches.batch_id
                  )
                order by observed_at, batch_id
            loop
                perform pg_advisory_xact_lock(
                    hashtextextended(pending_batch.feed_name, 0)
                );

                if exists (
                    select 1
                    from {{ target.schema }}.event_version_batches applied
                    where applied.feed_name = pending_batch.feed_name
                      and applied.observed_at > pending_batch.observed_at
                ) then
                    raise exception
                        'Batch % is older than applied % history; run the controlled rebuild',
                        pending_batch.batch_id,
                        pending_batch.feed_name;
                end if;

                select exists (
                    select 1
                    from {{ target.schema }}.event_version_batches applied
                    where applied.feed_name = pending_batch.feed_name
                ) into feed_has_history;

                select min(event_date)
                into minimum_inspection_date
                from event_change_candidates candidates
                where candidates.batch_id = pending_batch.batch_id
                  and candidates.feed_name = 'inspections'
                  and candidates.event_date is not null;

                create temporary table batch_prior on commit drop as
                select versions.*
                from {{ target }} versions
                where versions.feed_name = pending_batch.feed_name
                  and versions.is_current;

                update {{ target }} versions
                set last_seen_batch_id = pending_batch.batch_id
                from event_change_candidates candidates
                where candidates.batch_id = pending_batch.batch_id
                  and candidates.feed_name = pending_batch.feed_name
                  and candidates.is_model_eligible
                  and versions.feed_name = candidates.feed_name
                  and versions.source_record_key = candidates.source_record_key
                  and versions.is_current
                  and not versions.is_deleted
                  and versions.record_hash = candidates.record_hash;

                insert into {{ target }} (
                    feed_name,
                    source_record_key,
                    usdot_number,
                    event_date,
                    reported_date,
                    knowledge_valid_from,
                    availability_quality,
                    is_current,
                    is_deleted,
                    deletion_reason,
                    record_hash,
                    first_seen_batch_id,
                    last_seen_batch_id,
                    predecessor_version_key,
                    state,
                    violation_count,
                    oos_violation_count,
                    report_state,
                    report_number,
                    report_time,
                    report_seq_no,
                    fatalities,
                    injuries,
                    tow_away
                )
                select
                    candidates.feed_name,
                    candidates.source_record_key,
                    candidates.usdot_number,
                    candidates.event_date,
                    case
                        when feed_has_history then pending_batch.observed_at::date
                        else candidates.source_proxy_reported_date
                    end,
                    case
                        when feed_has_history then pending_batch.observed_at
                        else candidates.source_proxy_valid_from
                    end,
                    case
                        when feed_has_history then 'observed'
                        else 'source_proxy'
                    end,
                    true,
                    false,
                    null,
                    candidates.record_hash,
                    pending_batch.batch_id,
                    pending_batch.batch_id,
                    prior.event_version_key,
                    candidates.state,
                    candidates.violation_count,
                    candidates.oos_violation_count,
                    candidates.report_state,
                    candidates.report_number,
                    candidates.report_time,
                    candidates.report_seq_no,
                    candidates.fatalities,
                    candidates.injuries,
                    candidates.tow_away
                from event_change_candidates candidates
                left join batch_prior prior
                  on prior.feed_name = candidates.feed_name
                 and prior.source_record_key = candidates.source_record_key
                where candidates.batch_id = pending_batch.batch_id
                  and candidates.feed_name = pending_batch.feed_name
                  and candidates.is_model_eligible
                  and (
                      prior.event_version_key is null
                      or prior.is_deleted
                      or prior.record_hash <> candidates.record_hash
                  );

                insert into {{ target }} (
                    feed_name,
                    source_record_key,
                    usdot_number,
                    event_date,
                    reported_date,
                    knowledge_valid_from,
                    availability_quality,
                    is_current,
                    is_deleted,
                    deletion_reason,
                    record_hash,
                    first_seen_batch_id,
                    last_seen_batch_id,
                    predecessor_version_key,
                    state,
                    violation_count,
                    oos_violation_count,
                    report_state,
                    report_number,
                    report_time,
                    report_seq_no,
                    fatalities,
                    injuries,
                    tow_away
                )
                select
                    prior.feed_name,
                    prior.source_record_key,
                    prior.usdot_number,
                    prior.event_date,
                    pending_batch.observed_at::date,
                    pending_batch.observed_at,
                    'observed',
                    true,
                    true,
                    'source_deleted',
                    prior.record_hash,
                    pending_batch.batch_id,
                    pending_batch.batch_id,
                    prior.event_version_key,
                    prior.state,
                    prior.violation_count,
                    prior.oos_violation_count,
                    prior.report_state,
                    prior.report_number,
                    prior.report_time,
                    prior.report_seq_no,
                    prior.fatalities,
                    prior.injuries,
                    prior.tow_away
                from batch_prior prior
                where not prior.is_deleted
                  and not exists (
                      select 1
                      from event_change_candidates candidates
                      where candidates.batch_id = pending_batch.batch_id
                        and candidates.feed_name = prior.feed_name
                        and candidates.source_record_key = prior.source_record_key
                  )
                  and (
                      pending_batch.feed_name = 'crashes'
                      or (
                          minimum_inspection_date is not null
                          and prior.event_date >= minimum_inspection_date
                      )
                  );

                update {{ target }} predecessor
                set
                    knowledge_valid_to = pending_batch.observed_at,
                    is_current = false,
                    superseded_by_version_key = successor.event_version_key
                from {{ target }} successor
                where successor.first_seen_batch_id = pending_batch.batch_id
                  and successor.feed_name = pending_batch.feed_name
                  and successor.predecessor_version_key = predecessor.event_version_key
                  and predecessor.is_current
                  and predecessor.event_version_key <> successor.event_version_key;

                insert into {{ target.schema }}.event_version_batches (
                    batch_id,
                    feed_name,
                    observed_at
                ) values (
                    pending_batch.batch_id,
                    pending_batch.feed_name,
                    pending_batch.observed_at
                );

                drop table batch_prior;
            end loop;
        end
        $event_history$;
    {% endcall %}

    {{ adapter.commit() }}

    {{ return({'relations': [target]}) }}
{% endmaterialization %}
