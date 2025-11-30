
{{
    config(
        materialized = 'incremental',
        unique_key = ['product_id', 'date_key']
    )
}}

{% set run_date = var('run_date', 'current_date') %}


with price_update as (
    select oi.product_id
        , unit_price as product_price
    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('stg_orders') }} o
        on oi.order_id = o.order_id
    where o.order_date = '{{ run_date }}'
    group by oi.product_id
        , unit_price
)

select p.product_id
    , p.product_category
    , p.product_brand
    , p.product_name
    , coalesce(pu.product_price, p.product_price) as product_price
    , '{{ run_date }}'::date as date_key
from {{ ref('stg_products') }} p
left join price_update pu
    on p.product_id = pu.product_id