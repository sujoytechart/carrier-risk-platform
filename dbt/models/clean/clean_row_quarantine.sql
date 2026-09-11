select
    batch_id,
    source_row_number,
    feed_name,
    parse_reasons
from {{ ref('clean_inspections') }}
where {{ portable_array_length('parse_reasons') }} > 0

union all

select
    batch_id,
    source_row_number,
    feed_name,
    parse_reasons
from {{ ref('clean_crashes') }}
where {{ portable_array_length('parse_reasons') }} > 0
