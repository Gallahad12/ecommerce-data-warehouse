-- Flat table for the dashboard. Grain: one row per item in an order.
select
    items.order_item_key,
    items.order_id,
    items.order_date,
    products.category_name,
    sellers.state as seller_state,
    orders.customer_state,
    items.order_status,
    items.price,
    items.freight_value,
    items.item_total,
    orders.review_score,
    orders.delivery_status
from {{ ref('fct_order_items') }} as items
inner join {{ ref('dim_products') }} as products
    on items.product_key = products.product_key
inner join {{ ref('dim_sellers') }} as sellers
    on items.seller_key = sellers.seller_key
inner join {{ ref('rpt_orders') }} as orders
    on items.order_id = orders.order_id
