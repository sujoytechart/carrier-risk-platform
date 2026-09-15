{{ config(indexes=[{'columns': ['batch_id'], 'unique': true}]) }}

-- Pin complete raw input once, before either feed is conformed. Empty complete
-- batches remain explicit rows here and in the atomic candidate publication.
select batch_id, feed_name, observed_at, row_count
from {{ source('raw', 'snapshot_batches') }}
where status = 'loaded'
