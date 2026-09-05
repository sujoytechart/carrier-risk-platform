{% macro apply_event_history_batch(target) %}
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

                create index on batch_prior (feed_name, source_record_key);
                analyze batch_prior;

                update {{ target }} versions
                set last_seen_batch_id = pending_batch.batch_id
                from event_change_candidates candidates
                where candidates.batch_id = pending_batch.batch_id
                  and candidates.feed_name = pending_batch.feed_name
                  and versions.feed_name = candidates.feed_name
                  and versions.source_record_key = candidates.source_record_key
                  and versions.is_current
                  and not versions.is_deleted
                  and versions.record_hash = candidates.record_hash
                  and versions.is_model_eligible = candidates.is_model_eligible;

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
                select
                    candidates.feed_name,
                    candidates.event_type,
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
                    candidates.source_add_at,
                    candidates.source_change_at,
                    pending_batch.observed_at,
                    candidates.is_model_eligible,
                    candidates.exclusion_reason,
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
                  and candidates.source_record_key is not null
                  and (candidates.is_model_eligible or prior.event_version_key is not null)
                  and (
                      prior.event_version_key is null
                      or prior.is_deleted
                      or prior.record_hash <> candidates.record_hash
                      or prior.is_model_eligible <> candidates.is_model_eligible
                  );

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
                select
                    prior.feed_name,
                    prior.event_type,
                    prior.source_record_key,
                    prior.usdot_number,
                    prior.event_date,
                    pending_batch.observed_at::date,
                    pending_batch.observed_at,
                    'observed',
                    prior.source_add_at,
                    prior.source_change_at,
                    pending_batch.observed_at,
                    prior.is_model_eligible,
                    prior.exclusion_reason,
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


                insert into {{ target.schema }}.inspection_retention_expiries (
                    source_record_key, batch_id, event_version_key,
                    observed_at, retention_cutoff
                )
                select prior.source_record_key, pending_batch.batch_id,
                       prior.event_version_key, pending_batch.observed_at,
                       minimum_inspection_date
                from batch_prior prior
                where pending_batch.feed_name = 'inspections'
                  and not prior.is_deleted
                  and prior.event_date < minimum_inspection_date
                  and not exists (
                      select 1 from event_change_candidates candidates
                      where candidates.batch_id = pending_batch.batch_id
                        and candidates.feed_name = prior.feed_name
                        and candidates.source_record_key = prior.source_record_key
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
{% endmacro %}
