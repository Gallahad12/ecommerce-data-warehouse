select
    order_id,
    cast(order_item_id as int) as order_item_id,
    product_id,
    seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_at,
    cast(price as numeric) as price,
    cast(freight_value as numeric) as freight_value
from {{ source('olist', 'order_items') }}
