select 
    row_number() over(order by card_id) as card_key,
    *
from {{ ref("stg_cards") }}