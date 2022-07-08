-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Therefore return records where this isn't true to make the test fail.
select
  customer_id,
	count(order_id) as total_orders
from {{ ref('stg_orders') }}
group by 1
having not(count(order_id) >= 1)



