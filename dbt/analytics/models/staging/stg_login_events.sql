

select event_id
    , date(timestamp) as event_date
    , date_part('hour', timestamp) as event_hour
    , user_id
    , session_id
    , status
    , ip_address
    , parameters ->> 'device_type' as device_type
    , parameters ->> 'app_version' as app_version
    , parameters ->> 'country' as country
    , parameters ->> 'login_method' as login_method
    , parameters ->> 'os' as operating_system
    , parameters ->> 'mac_address' as mac_address
    , now()::timestamp(0) as load_timestamp
    , timestamp::timestamp(0) as event_timestamp
from {{ source('raw', 'login_events')}}
