{# Standard Snowflake table constraints do not enforce keys/checks. Validate
   both existing state and the complete proposed state inside the transaction. #}
{% macro snowflake_validate_history(target, applied, retention) %}
    select count(*) into :violations from (
        select 1 as invalid from {{ target }}
        where event_version_key is null or feed_name is null or event_type is null
           or source_record_key is null or reported_date is null
           or knowledge_valid_from is null or availability_quality is null
           or first_observed_at is null or is_model_eligible is null
           or is_current is null or is_deleted is null or record_hash is null
           or first_seen_batch_id is null or last_seen_batch_id is null
           or feed_name not in ('inspections', 'crashes')
           or event_type <> case feed_name when 'inspections' then 'inspection' else 'crash' end
           or availability_quality not in ('source_proxy', 'observed')
           or event_date > reported_date
           or (is_model_eligible and (usdot_number is null or event_date is null))
           or (not is_model_eligible and exclusion_reason is null)
           or knowledge_valid_from >= knowledge_valid_to
           or is_current <> (knowledge_valid_to is null)
           or (is_deleted and deletion_reason is null)
           or reported_date <> convert_timezone('UTC', knowledge_valid_from)::date
           or (availability_quality = 'observed' and knowledge_valid_from <> first_observed_at)
        union all
        select 1 from {{ target }} group by event_version_key having count(*) > 1
        union all
        select 1 from {{ target }}
        group by first_seen_batch_id, feed_name, source_record_key, is_deleted
        having count(*) > 1
        union all
        select 1 from {{ target }} where is_current
        group by feed_name, source_record_key having count(*) > 1
        union all
        select 1 from {{ target }} earlier
        join {{ target }} later on earlier.feed_name = later.feed_name
         and earlier.source_record_key = later.source_record_key
         and earlier.event_version_key < later.event_version_key
        where (earlier.knowledge_valid_to is null or later.knowledge_valid_from < earlier.knowledge_valid_to)
          and (later.knowledge_valid_to is null or earlier.knowledge_valid_from < later.knowledge_valid_to)
        union all
        select 1 from {{ target }} versions
        left join {{ target }} predecessor
          on predecessor.event_version_key = versions.predecessor_version_key
        where versions.predecessor_version_key is not null and (
            predecessor.event_version_key is null
            or predecessor.event_version_key = versions.event_version_key
            or predecessor.feed_name <> versions.feed_name
            or predecessor.source_record_key <> versions.source_record_key
            or predecessor.superseded_by_version_key is distinct from versions.event_version_key
            or predecessor.knowledge_valid_to is distinct from versions.knowledge_valid_from
        )
        union all
        select 1 from {{ target }} versions
        left join {{ target }} successor
          on successor.event_version_key = versions.superseded_by_version_key
        where (versions.is_current and versions.superseded_by_version_key is not null)
           or (not versions.is_current and (
                successor.event_version_key is null
                or successor.predecessor_version_key is distinct from versions.event_version_key
           ))
        union all
        select 1 from {{ applied }}
        where batch_id is null or feed_name is null or observed_at is null or applied_at is null
           or feed_name not in ('inspections', 'crashes')
        union all
        select 1 from {{ applied }} group by batch_id having count(*) > 1
        union all
        select 1 from {{ target }} versions
        left join {{ applied }} first_batch on first_batch.batch_id = versions.first_seen_batch_id
        left join {{ applied }} last_batch on last_batch.batch_id = versions.last_seen_batch_id
        where first_batch.batch_id is null or last_batch.batch_id is null
           or first_batch.feed_name <> versions.feed_name or last_batch.feed_name <> versions.feed_name
           or first_batch.observed_at <> versions.first_observed_at
           or last_batch.observed_at < first_batch.observed_at
        union all
        select 1 from {{ retention }}
        group by source_record_key, batch_id having count(*) > 1
        union all
        select 1 from {{ retention }} expiry
        left join {{ target }} versions on versions.event_version_key = expiry.event_version_key
        left join {{ applied }} batch on batch.batch_id = expiry.batch_id
        where expiry.source_record_key is null or expiry.batch_id is null
           or expiry.event_version_key is null or expiry.observed_at is null
           or expiry.retention_cutoff is null or versions.event_version_key is null
           or versions.feed_name <> 'inspections'
           or versions.source_record_key <> expiry.source_record_key
           or batch.batch_id is null or batch.feed_name <> 'inspections'
           or batch.observed_at <> expiry.observed_at
           or versions.event_date is null or versions.event_date >= expiry.retention_cutoff
    ) invalid_state;
    if (violations > 0) then
        raise invalid_history;
    end if;
{% endmacro %}
