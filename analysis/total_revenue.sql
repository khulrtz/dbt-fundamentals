with

total_revenue as (
    select SUM(amount) as IMP_TOTAL_REVENUE from {{ ref('stg_payments') }} where status = 'success'
)

select * from total_revenue 