select *
from {{ ref('events_union') }}
where is_current
  and not is_deleted
