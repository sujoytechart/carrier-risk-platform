select
    batch_id,
    source_row_number,
    feed_name,
    parse_reasons
from {{ ref('clean_inspections') }}
where cardinality(parse_reasons) > 0

union all

select
    batch_id,
    source_row_number,
    feed_name,
    parse_reasons
from {{ ref('clean_crashes') }}
where cardinality(parse_reasons) > 0
