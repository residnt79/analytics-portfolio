

select event_id
    , date(timestamp) as session_date
    , date_part('hour', timestamp) as session_hour
    , user_id
    , session_id
    , event_type
    , parameters
    , now()::timestamp(0) as load_timestamp
    , timestamp::timestamp(0) as event_timestamp
from {{ source('raw', 'session_events')}}