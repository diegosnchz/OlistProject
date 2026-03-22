-- Test: fct_orders must have positive total_amount for delivered orders.
-- A delivered order always has a payment recorded; zero or negative amounts
-- indicate a data quality problem upstream.

select
    order_id,
    total_amount
from {{ ref('fct_orders') }}
where is_delivered = true
  and total_amount is not null
  and total_amount <= 0
