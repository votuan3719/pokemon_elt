with
    prices as (
        select
            c.card_key::int as card_key,
            s.set_key::int as set_key,
            e.edition_key::int as edition_key,
            p.date::date as date,
            p.price_low::float as price_low,
            p.price_mid::float as price_mid,
            p.price_high::float as price_high,
            p.market_price::float as price_market
        from {{ ref("stg_card_prices") }} as p
        left join {{ ref("init_cards") }} as c using (card_id)
        left join {{ ref("init_sets") }} as s using (set_id)
        left join {{ ref("init_editions") }} as e on p.card_edition = e.edition
        order by date, set_key, card_key, edition_key
    )
select * from prices
