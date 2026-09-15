{% macro create_event_history_tables(target) %}
        create schema if not exists {{ target.schema }};

        create table if not exists {{ target }} (
            event_version_key bigint generated always as identity primary key,
            feed_name text not null,
            event_type text not null check (event_type in ('inspection', 'crash')),
            source_record_key text not null,
            usdot_number text,
            event_date date,
            reported_date date not null,
            knowledge_valid_from timestamptz not null,
            knowledge_valid_to timestamptz,
            availability_quality text not null
                check (availability_quality in ('source_proxy', 'observed')),
            source_add_at timestamptz,
            source_change_at timestamptz,
            first_observed_at timestamptz not null,
            is_model_eligible boolean not null default true,
            exclusion_reason text,
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
            check (event_date <= reported_date),
            check (not is_model_eligible or (usdot_number is not null and event_date is not null)),
            check (is_model_eligible or exclusion_reason is not null),
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


        create index if not exists event_versions_current_source
            on {{ target }} (feed_name, source_record_key) where is_current;
        create index if not exists event_version_batches_feed_observed
            on {{ target.schema }}.event_version_batches (feed_name, observed_at);
        create table if not exists {{ target.schema }}.inspection_retention_expiries (
            source_record_key text not null,
            batch_id text not null,
            event_version_key bigint not null references {{ target }} (event_version_key),
            observed_at timestamptz not null,
            retention_cutoff date not null,
            primary key (source_record_key, batch_id)
        );
{% endmacro %}
