{{ config(tags=['temporal'], severity='error') }}

select event_version_key, event_date, reported_date
from {{ ref('event_versions') }}
where reported_date < event_date
