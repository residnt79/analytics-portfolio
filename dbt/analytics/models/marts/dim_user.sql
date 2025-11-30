


with logins as (
    select user_id
        , min(event_date) as first_login_date
        , max(event_date) as last_login_date
        , count(event_date) as total_logins
        , count(distinct event_date) as login_days
    from {{ ref('stg_login_events')}}
    group by user_id
)
, purchases as (
    select user_id
        , min(order_date) as first_purchase_date
        , max(order_date) as last_purchase_date
        , count(order_id) as total_orders
    from {{ ref('stg_orders')}}
    group by user_id
)
select su.user_id
    , su.signup_date
    , l.first_login_date
    , l.first_login_date - su.signup_date as days_to_first_login
    , l.last_login_date
    , date(now()) - l.last_login_date as days_since_last_login
    , l.total_logins
    , l.login_days
    , su.signup_method
    , su.country
    , su.state
    , su.city
    , case when p.user_id is not null then TRUE else FALSE end as has_purchased
    , p.first_purchase_date
    , p.first_purchase_date - l.first_login_date as days_to_first_purchase
    , p.last_purchase_date
    , date(now())- p.last_purchase_date as days_since_last_purchase
from {{ ref('stg_signup_events')}} su
left join logins l
    on su.user_id = l.user_id
left join purchases p
    on su.user_id = p.user_id