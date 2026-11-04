-- Some orders have several reviews. Keep the most recently answered one.
with ranked as (
    select
        order_id,
        review_score,
        row_number() over (
            partition by order_id
            order by review_answered_at desc, review_id
        ) as review_rank
    from {{ ref('stg_order_reviews') }}
)

select
    order_id,
    review_score
from ranked
where review_rank = 1
