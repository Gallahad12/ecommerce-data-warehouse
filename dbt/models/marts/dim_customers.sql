-- One row per real customer. Their address comes from their most recent order.
with ranked as (
    select
        customers.customer_unique_id,
        customers.zip_code_prefix,
        customers.city,
        customers.state,
        row_number() over (
            partition by customers.customer_unique_id
            order by orders.purchased_at desc, customers.customer_id
        ) as recency_rank
    from {{ ref('stg_customers') }} as customers
    left join {{ ref('stg_orders') }} as orders
        on customers.customer_id = orders.customer_id
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_key,
    customer_unique_id,
    zip_code_prefix,
    city,
    state
from ranked
where recency_rank = 1
