

select event_id
    , date(event_date) as refund_return_date
    , date_part('hour', event_date) as refund_return_hour
    , order_id
    , event_type
    , refund_amount
    , returned_items
    , reason
    , status
    , now()::timestamp(0) as load_timestamp
    , event_date::timestamp(0) as event_timestamp
from {{ source('raw', 'refund_return_events')}}