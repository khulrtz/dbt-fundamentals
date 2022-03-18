with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

WITH customer_amount AS (
SELECT CUSTOMER_ID,SUM(AMOUNT) LIFETIME_VALUE FROM {{ ref('fct_orders') }} GROUP BY CUSTOMER_ID
)

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.LIFETIME_VALUE, 0) as LIFETIME_VALUE

    from customers

    left join customer_orders using (customer_id)
    left join customer_amount using (customer_id)

)

select * from final