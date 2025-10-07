select
    row_number() over(order by set_id) as set_key,
    *
from {{ ref("stg_sets") }}