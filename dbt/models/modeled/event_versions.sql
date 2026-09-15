{{ config(materialized='event_version_history') }}

select *
from {{ ref('event_change_candidates') }}
