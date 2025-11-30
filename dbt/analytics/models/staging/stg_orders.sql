

select order_id
    , date(order_date) as order_date
    , date_part('hour', order_date) as order_hour
    , user_id
    , session_id
    , subtotal
    , discount_amount
    , tax
    , shipping
    , total
    , now()::timestamp(0) as load_timestamp
    , order_date::timestamp(0) as event_timestamp
from {{ source('raw', 'orders')}}