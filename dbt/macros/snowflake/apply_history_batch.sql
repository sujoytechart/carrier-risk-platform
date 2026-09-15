{% macro snowflake_apply_history_batch(target, applied, retention, candidates, prior) %}
    select count(*) > 0 into :feed_has_history
    from {{ applied }} applied
    where applied.feed_name = :batch_feed_name;

    select min(event_date)
    into :minimum_inspection_date
    from {{ candidates }} candidates
    where candidates.batch_id = :batch_id
      and candidates.feed_name = 'inspections'
      and candidates.event_date is not null;

    delete from {{ prior }};
    insert into {{ prior }}
    select versions.*
    from {{ target }} versions
    where versions.feed_name = :batch_feed_name
      and versions.is_current;


    update {{ target }} versions
    set last_seen_batch_id = :batch_id
    from {{ candidates }} candidates
    where candidates.batch_id = :batch_id
      and candidates.feed_name = :batch_feed_name
      and versions.feed_name = candidates.feed_name
      and versions.source_record_key = candidates.source_record_key
      and versions.is_current
      and not versions.is_deleted
      and versions.record_hash = candidates.record_hash
      and versions.is_model_eligible = candidates.is_model_eligible;

    -- Every history transition shares one positional insert contract.
    -- Adding a target column cannot leave one transition on a stale list.
    insert into {{ target }} (
        feed_name,
        event_type,
        source_record_key,
        usdot_number,
        event_date,
        reported_date,
        knowledge_valid_from,
        availability_quality,
        source_add_at,
        source_change_at,
        first_observed_at,
        is_model_eligible,
        exclusion_reason,
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
    with candidate_versions as (
        select
            candidates.feed_name,
            candidates.event_type,
            candidates.source_record_key,
            candidates.usdot_number,
            case
                -- Preserve the rejected date in raw/clean quarantine.
                -- An exclusion transition does not assert a future event.
                when not candidates.is_model_eligible
                 and candidates.event_date > convert_timezone('UTC', :batch_observed_at)::date
                then null
                else candidates.event_date
            end,
            case
                when :feed_has_history then convert_timezone('UTC', :batch_observed_at)::date
                else candidates.source_proxy_reported_date
            end,
            case
                when :feed_has_history then :batch_observed_at
                else {{ snowflake_history_source_clock('candidates.source_proxy_valid_from') }}
            end,
            case
                when :feed_has_history then 'observed'
                else 'source_proxy'
            end,
            {{ snowflake_history_source_clock('candidates.source_add_at') }},
            {{ snowflake_history_source_clock('candidates.source_change_at') }},
            :batch_observed_at,
            candidates.is_model_eligible,
            candidates.exclusion_reason,
            true,
            false,
            null,
            candidates.record_hash,
            :batch_id,
            :batch_id,
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
        from {{ candidates }} candidates
        left join {{ prior }} prior
          on prior.feed_name = candidates.feed_name
         and prior.source_record_key = candidates.source_record_key
        where candidates.batch_id = :batch_id
          and candidates.feed_name = :batch_feed_name
          and candidates.source_record_key is not null
          and (
              candidates.is_model_eligible
              or prior.event_version_key is not null
          )
          and (
              prior.event_version_key is null
              or prior.is_deleted
              or prior.record_hash <> candidates.record_hash
              or prior.is_model_eligible <> candidates.is_model_eligible
          )
    ),
    deletion_versions as (
        select
            prior.feed_name,
            prior.event_type,
            prior.source_record_key,
            prior.usdot_number,
            prior.event_date,
            convert_timezone('UTC', :batch_observed_at)::date,
            :batch_observed_at,
            'observed',
            prior.source_add_at,
            prior.source_change_at,
            :batch_observed_at,
            prior.is_model_eligible,
            prior.exclusion_reason,
            true,
            true,
            'source_deleted',
            prior.record_hash,
            :batch_id,
            :batch_id,
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
        from {{ prior }} prior
        where not prior.is_deleted
          and not exists (
              select 1
              from {{ candidates }} candidates
              where candidates.batch_id = :batch_id
                and candidates.feed_name = prior.feed_name
                and candidates.source_record_key = prior.source_record_key
          )
          and (
              :batch_feed_name = 'crashes'
              or (
                  :minimum_inspection_date is not null
                  and prior.event_date >= :minimum_inspection_date
              )
          )
    )
    select * from candidate_versions
    union all
    select * from deletion_versions;


    insert into {{ retention }} (
        source_record_key, batch_id, event_version_key,
        observed_at, retention_cutoff
    )
    select prior.source_record_key, :batch_id,
           prior.event_version_key, :batch_observed_at,
           :minimum_inspection_date
    from {{ prior }} prior
    where :batch_feed_name = 'inspections'
      and not prior.is_deleted
      and prior.event_date < :minimum_inspection_date
      and not exists (
          select 1 from {{ candidates }} candidates
          where candidates.batch_id = :batch_id
            and candidates.feed_name = prior.feed_name
            and candidates.source_record_key = prior.source_record_key
      );

    update {{ target }} predecessor
    set
        knowledge_valid_to = :batch_observed_at,
        is_current = false,
        superseded_by_version_key = successor.event_version_key
    from {{ target }} successor
    where successor.first_seen_batch_id = :batch_id
      and successor.feed_name = :batch_feed_name
      and successor.predecessor_version_key = predecessor.event_version_key
      and predecessor.is_current
      and predecessor.event_version_key <> successor.event_version_key;

    insert into {{ applied }} (
        batch_id,
        feed_name,
        observed_at
    ) values (
        :batch_id,
        :batch_feed_name,
        :batch_observed_at
    );

{% endmacro %}

{# Source fields are NTZ wall-clock values interpreted as UTC. Never let an
   implicit NTZ -> TZ cast depend on the worker session's timezone. #}
{% macro snowflake_history_source_clock(expression) -%}
    to_timestamp_tz(to_char({{ expression }}, 'YYYY-MM-DD HH24:MI:SS.FF9') || ' +00:00', 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')
{%- endmacro %}
