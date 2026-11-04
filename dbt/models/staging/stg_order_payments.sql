select
    order_id,
    cast(payment_sequential as int) as payment_sequential,
    payment_type,
    cast(payment_installments as int) as payment_installments,
    cast(payment_value as numeric) as payment_value
from {{ source('olist', 'order_payments') }}
