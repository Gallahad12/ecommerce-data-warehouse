{# Partitioning and clustering only apply on BigQuery, not the DuckDB CI target. #}
{% if target.type == 'bigquery' %}
    {{ config(
        partition_by={'field': 'order_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['customer_key', 'order_status']
    ) }}
{% endif %}

-- Grain: one row per order.
with item_totals as (
    select
        order_id,
        count(*) as item_count,
        sum(price) as items_value,
        sum(freight_value) as freight_value
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    orders.order_id,
    {{ dbt_utils.generate_surrogate_key(['customers.customer_unique_id']) }} as customer_key,
    cast(orders.purchased_at as date) as order_date,
    orders.order_status,
    coalesce(item_totals.item_count, 0) as item_count,
    item_totals.items_value,
    item_totals.freight_value,
    payments.payment_value,
    payments.primary_payment_type,
    payments.max_installments,
    reviews.review_score,
    {{ dbt.datediff('orders.purchased_at', 'orders.delivered_at', 'hour') }} as delivery_hours,
    orders.delivered_at > orders.estimated_delivery_at as is_delivered_late
from {{ ref('stg_orders') }} as orders
inner join {{ ref('stg_customers') }} as customers
    on orders.customer_id = customers.customer_id
left join item_totals
    on orders.order_id = item_totals.order_id
left join {{ ref('int_order_payments') }} as payments
    on orders.order_id = payments.order_id
left join {{ ref('int_order_reviews') }} as reviews
    on orders.order_id = reviews.order_id
