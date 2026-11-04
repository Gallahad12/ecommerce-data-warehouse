-- Payments should cover items plus freight. Warn only, because the source data
-- contains small rounding differences and vouchers.
{{ config(severity='warn') }}

select
    order_id,
    payment_value,
    items_value + freight_value as order_total
from {{ ref('fct_orders') }}
where order_status = 'delivered'
    and abs(payment_value - (items_value + freight_value)) > 1
