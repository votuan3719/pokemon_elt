select
    row_number() over(order by edition) as edition_key,
    *
from {{ ref("stg_editions") }}
where edition is not null