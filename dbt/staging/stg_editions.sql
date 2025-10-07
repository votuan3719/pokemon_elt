select distinct
    card_edition as edition
from {{ ref("stg_card_prices") }}