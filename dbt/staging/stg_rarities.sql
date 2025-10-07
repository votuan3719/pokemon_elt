select distinct
    json_data:rarity as rarity
from {{ source("dbt_pokemon", "tcg_pokemon_api") }}