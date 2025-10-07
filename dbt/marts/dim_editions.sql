select
    edition_key::int as edition_key,
    edition::varchar as edition
from {{ ref("init_editions") }}
order by edition_key