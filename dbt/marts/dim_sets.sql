select
    set_key::int as set_key,
    set_id::varchar as set_id,
    set_name::varchar as set_name,
    series_name::varchar as series_name,
    printed_total::int as printed_total,
    to_date(release_date::varchar, 'YYYY/MM/DD') as release_date,
    logo_url::varchar as logo_url
from {{ ref("init_sets") }}
order by set_key