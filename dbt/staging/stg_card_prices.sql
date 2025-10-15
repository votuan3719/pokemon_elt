with cards as (
    select
        card.value:cardId as card_id,
        card.value:set.id as set_id,
        card.value:tcg,
        card.value:tcgplayerHistory.priceHistory._doc as tcg_history
    from {{ source("dbt_pokemon", "price_tracker_api") }},
        lateral flatten (json_data) card
),
editions as (
    select
        card_id,
        set_id,
        edition.key as card_edition,
        edition.value:low as price_low,
        edition.value:mid as price_mid,
        edition.value:high as price_high,
        edition.value:market as market_price
    from cards,
        lateral flatten (tcg_history) edition
),
low_prices as (
    select
        card_id,
        set_id,
        card_edition,
        low.value:date as date,
        low.value:price as price_low
    from editions,
        lateral flatten (price_low) as low
),
mid_prices as (
    select
        card_id,
        set_id,
        card_edition,
        mid.value:date as date,
        mid.value:price as price_mid
    from editions,
        lateral flatten (price_mid) as mid
),
high_prices as (
    select
        card_id,
        set_id,
        card_edition,
        high.value:date as date,
        high.value:price as price_high
    from editions,
        lateral flatten (price_high) as high
),
market_prices as (
    select
        card_id,
        set_id,
        card_edition,
        market.value:date as date,
        market.value:price as market_price
    from editions,
        lateral flatten (market_price) market
),
prices as (
    select *
    from low_prices
    join mid_prices
        using(card_id, set_id, card_edition, date)
    join high_prices
        using(card_id, set_id, card_edition, date)
    join market_prices
        using(card_id, set_id, card_edition, date)
)
select * from prices