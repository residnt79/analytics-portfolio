


select product_id
    , product_name
    , product_category
    , product_price
    , product_brand
    , now()::timestamp(0) as load_timestamp
    , created_at::timestamp(0) as created_timestamp
from {{ source('raw', 'products')}}
