{% macro snowflake_apply_history(target, applied, retention, candidates, batches, prior) %}
    execute immediate $$
    declare
        pending_batches cursor for
            select batch_id, feed_name, observed_at from {{ batches }} batches
            where not exists (
                select 1 from {{ applied }} applied where applied.batch_id = batches.batch_id
            )
            order by observed_at, batch_id;
        batch_id varchar;
        batch_feed_name varchar;
        batch_observed_at timestamp_tz;
        feed_has_history boolean;
        minimum_inspection_date date;
        violations integer;
        invalid_publication exception (-20001, 'Clean candidates do not match pinned complete batches or contain ambiguous identities; rebuild clean models');
        invalid_history exception (-20002, 'Persisted event history violates its contract or lineage invariants');
        stale_batch exception (-20003, 'Batch is older than applied history; run the controlled rebuild');
    begin
        begin transaction;
        {{ snowflake_validate_candidates(candidates, batches) }}
        {{ snowflake_validate_history(target, applied, retention) }}
        select count(*) into :violations
        from {{ batches }} batches join {{ applied }} applied using (batch_id)
        where batches.feed_name <> applied.feed_name
           or batches.observed_at <> applied.observed_at;
        if (violations > 0) then
            raise invalid_publication;
        end if;
        for pending_batch in pending_batches do
            batch_id := pending_batch.batch_id;
            batch_feed_name := pending_batch.feed_name;
            batch_observed_at := pending_batch.observed_at;
            select count(*) into :violations from {{ applied }} applied
            where applied.feed_name = :batch_feed_name
              and applied.observed_at > :batch_observed_at;
            if (violations > 0) then
                raise stale_batch;
            end if;
            {{ snowflake_apply_history_batch(target, applied, retention, candidates, prior) }}
        end for;
        {{ snowflake_validate_history(target, applied, retention) }}
        commit;
    exception
        when other then
            rollback;
            raise;
    end;
    $$;
{% endmacro %}
