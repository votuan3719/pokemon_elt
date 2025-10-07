select
    row_number() over(order by rarity) as rarity_key,
    *
from {{ ref("stg_rarities") }}
where rarity is not null