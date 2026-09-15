{# Reject incomplete publication before deleting either side of the prior pair.
   The same validation is repeated by history, which may run independently. #}
{% macro snowflake_validate_candidates(candidates, batches) %}
    select count(*) into :violations from (
        select 1 as invalid from {{ batches }}
        where batch_id is null or feed_name is null or observed_at is null
           or feed_name not in ('inspections', 'crashes')
           or row_count is null or row_count < 0 or row_count <> trunc(row_count)
        union all
        select 1 from {{ batches }} group by batch_id having count(*) > 1
        union all
        select 1 from {{ candidates }}
        where batch_id is null or feed_name is null or observed_at is null
           or record_hash is null or is_model_eligible is null
           or event_type is null
           or (feed_name = 'inspections' and event_type <> 'inspection')
           or (feed_name = 'crashes' and event_type <> 'crash')
        union all
        select 1 from {{ candidates }} where source_record_key is not null
        group by batch_id, feed_name, source_record_key having count(*) > 1
        union all
        select 1 from {{ batches }} batches
        left join (
            select batch_id, feed_name, count(*) as candidate_count
            from {{ candidates }} group by batch_id, feed_name
        ) candidates using (batch_id, feed_name)
        where coalesce(candidates.candidate_count, 0) <> batches.row_count
        union all
        select 1 from {{ candidates }} candidates
        left join {{ batches }} batches using (batch_id, feed_name)
        where batches.batch_id is null or candidates.observed_at <> batches.observed_at
    ) invalid_publication;
    if (violations > 0) then
        raise invalid_publication;
    end if;
{% endmacro %}

{% macro snowflake_publish_candidates(target, batches, staged_candidates, staged_batches) %}
    execute immediate $$
    declare
        violations integer;
        invalid_publication exception (-20001, 'Clean candidates do not match pinned complete batches or contain ambiguous identities; rebuild clean models');
    begin
        begin transaction;
        {{ snowflake_validate_candidates(staged_candidates, staged_batches) }}
        delete from {{ target }};
        insert into {{ target }} ({{ snowflake_candidate_columns() }})
        select {{ snowflake_candidate_columns() }} from {{ staged_candidates }};
        delete from {{ batches }};
        insert into {{ batches }} (batch_id, feed_name, observed_at, row_count)
        select batch_id, feed_name, observed_at, row_count from {{ staged_batches }};
        {{ snowflake_validate_candidates(target, batches) }}
        commit;
    exception
        when other then
            rollback;
            raise;
    end;
    $$;
{% endmacro %}

{% macro snowflake_candidate_columns() %}
    batch_id, observed_at, feed_name, event_type, source_record_key, usdot_number,
    event_date, source_proxy_reported_date, source_proxy_valid_from, source_add_at,
    source_change_at, state, violation_count, oos_violation_count, report_state,
    report_number, report_time, report_seq_no, fatalities, injuries, tow_away,
    record_hash, is_model_eligible, exclusion_reason
{% endmacro %}
