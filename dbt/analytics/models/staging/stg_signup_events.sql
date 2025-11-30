

select signup_id
    , user_id
    , date(timestamp) as signup_date
    , date_part('hour', timestamp) as signup_hour
    , signup_method
    , email
    , first_name
    , last_name
    , first_name || ' ' || last_name as full_name
    , address
    , country
    , city
    , state
    , postal_code
    , now()::timestamp(0) as load_timestamp
    , timestamp::timestamp(0) as event_timestamp
from {{ source('raw', 'signup_events')}}