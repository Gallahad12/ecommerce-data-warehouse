-- One row per order, so joining payments to orders cannot fan out.
select
    order_id,
    sum(payment_value) as payment_value,
    count(*) as payment_count,
    max(payment_installments) as max_installments,
    max(case when payment_sequential = 1 then payment_type end) as primary_payment_type
from {{ ref('stg_order_payments') }}
group by order_id
