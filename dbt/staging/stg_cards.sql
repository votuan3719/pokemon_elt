select 
    json_data:id as card_id,
    json_data:name as name,
    json_data:supertype as supertype,
    json_data:rarity as rarity,
    json_data:artist as artist,
    json_data:images:large as img_url
from {{ source("dbt_pokemon", "tcg_pokemon_api") }}