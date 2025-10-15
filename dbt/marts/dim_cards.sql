select
    c.card_key::int as card_key,
    c.card_id::varchar as card_id,
    c.name::varchar as name,
    c.supertype::varchar as supertype,
    r.rarity_key::int as rarity_key,
    c.artist::varchar as artist,
    c.img_url::varchar as img_url
from {{ ref("init_cards") }} as c
left join {{ ref("init_rarities" ) }} as r
    using(rarity)
order by card_key
