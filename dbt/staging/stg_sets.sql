select
    pokemon_set.value:id as set_id,
    pokemon_set.value:name as set_name,
    pokemon_set.value:series as series_name,
    pokemon_set.value:printedTotal as printed_total,
    pokemon_set.value:releaseDate as release_date,
    pokemon_set.value:images.logo as logo_url
from {{ source("dbt_pokemon", "tcg_sets_api")}},
    lateral flatten (json_data) pokemon_set