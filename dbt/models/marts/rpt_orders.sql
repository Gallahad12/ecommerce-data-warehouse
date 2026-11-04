-- Flat table for the dashboard. Grain: one row per order.
select
    orders.order_id,
    orders.order_date,
    cast({{ dbt.date_trunc('month', 'orders.order_date') }} as date) as order_month,
    customers.state as customer_state,
    orders.order_status,
    orders.item_count,
    orders.items_value,
    orders.freight_value,
    orders.payment_value,
    orders.primary_payment_type,
    orders.review_score,
    orders.delivery_hours / 24.0 as delivery_days,
    case
        when orders.is_delivered_late is null then 'Not delivered'
        when orders.is_delivered_late then 'Late'
        else 'On time'
    end as delivery_status
from {{ ref('fct_orders') }} as orders
inner join {{ ref('dim_customers') }} as customers
    on orders.customer_key = customers.customer_key
