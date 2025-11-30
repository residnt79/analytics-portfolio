

select status_event_id
    , date(timestamp) as order_status_date
    , date_part('hour', timestamp) as order_status_hour
    , order_id
    , status
    , tracking_number
    , carrier
    , notes
    , now()::timestamp(0) as load_timestamp
    , timestamp::timestamp(0) as event_timestamp
from {{ source('raw', 'order_status_events')}}
