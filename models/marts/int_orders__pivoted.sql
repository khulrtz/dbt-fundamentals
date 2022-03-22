with 
orders as  (
    select * from {{ ref('stg_orders' )}}
)
,orders_pivoted as (
SELECT 
{{ pivotar('status','orders','num_orders') }}
FROM orders
)
select * from orders_pivoted