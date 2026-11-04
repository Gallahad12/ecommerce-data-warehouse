-- A delivery cannot happen before the purchase. Warn only, so a few bad source
-- rows are reported without stopping the pipeline.
{{ config(severity='warn') }}

select
    order_id,
    delivery_hours
from {{ ref('fct_orders') }}
where delivery_hours < 0
