select
    rarity_key::int as rarity_key,
    rarity::varchar as rarity
from {{ ref("init_rarities") }}
order by rarity_key