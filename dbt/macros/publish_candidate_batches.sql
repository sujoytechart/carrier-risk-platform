{% macro publish_candidate_batches() %}
    {# A table model's transactional post-hook publishes this registry together
       with its candidates. Counts include excluded/quarantined source rows. #}
    drop table if exists {{ this.schema }}.event_candidate_batches;
    create table {{ this.schema }}.event_candidate_batches as
    select batches.*
    from {{ ref('clean_snapshot_batches') }} batches;
    alter table {{ this.schema }}.event_candidate_batches add primary key (batch_id);

    do $publication$
    begin
        if exists (
            select 1
            from {{ this.schema }}.event_candidate_batches batches
            left join (
                select batch_id, feed_name, count(*) as candidate_count
                from {{ this }} group by batch_id, feed_name
            ) candidates using (batch_id, feed_name)
            where coalesce(candidates.candidate_count, 0) <> batches.row_count
        ) or exists (
            select 1 from {{ this }} candidates
            left join {{ this.schema }}.event_candidate_batches batches
                using (batch_id, feed_name)
            where batches.batch_id is null
        ) then
            raise exception 'Clean candidates do not match pinned complete batches; rebuild clean models';
        end if;
    end
    $publication$;
{% endmacro %}
