

select order_item_id
    , order_id
    , product_id
    , product_name
    , product_category
    , quantity
    , unit_price
    , line_total
    , now()::timestamp(0) as load_timestamp
from {{ source('raw', 'order_items')}}
