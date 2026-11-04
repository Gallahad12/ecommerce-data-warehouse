{# Partitioning and clustering only apply on BigQuery, not the DuckDB CI target. #}
{% if target.type == 'bigquery' %}
    {{ config(
        partition_by={'field': 'order_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['product_key', 'seller_key']
    ) }}
{% endif %}

-- Grain: one row per item in an order.
select
    {{ dbt_utils.generate_surrogate_key(['items.order_id', 'items.order_item_id']) }} as order_item_key,
    items.order_id,
    items.order_item_id,
    {{ dbt_utils.generate_surrogate_key(['customers.customer_unique_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['items.product_id']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['items.seller_id']) }} as seller_key,
    cast(orders.purchased_at as date) as order_date,
    orders.order_status,
    items.price,
    items.freight_value,
    items.price + items.freight_value as item_total
from {{ ref('stg_order_items') }} as items
inner join {{ ref('stg_orders') }} as orders
    on items.order_id = orders.order_id
inner join {{ ref('stg_customers') }} as customers
    on orders.customer_id = customers.customer_id
