{{ config(tags=['temporal'], severity='error') }}

select
    earlier.feed_name,
    earlier.source_record_key,
    earlier.event_version_key as earlier_version_key,
    later.event_version_key as later_version_key
from {{ ref('event_versions') }} earlier
join {{ ref('event_versions') }} later
  on earlier.feed_name = later.feed_name
 and earlier.source_record_key = later.source_record_key
 and earlier.event_version_key < later.event_version_key
 and earlier.knowledge_valid_from < coalesce(
        later.knowledge_valid_to,
        {{ portable_utc_timestamp("'9999-12-31 00:00:00'") }}
     )
 and later.knowledge_valid_from < coalesce(
        earlier.knowledge_valid_to,
        {{ portable_utc_timestamp("'9999-12-31 00:00:00'") }}
     )
