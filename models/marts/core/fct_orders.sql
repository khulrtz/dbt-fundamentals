with datos as (
SELECT 
B.ORDER_ID,
A.CUSTOMER_ID,
C.AMOUNT
FROM {{ ref('stg_customers') }} A 
LEFT JOIN {{ ref('stg_orders') }} B ON A.CUSTOMER_ID = B.CUSTOMER_ID
LEFT JOIN {{ ref('stg_payments') }} C ON B.ORDER_ID = C.ORDER_ID
)

SELECT 
ORDER_ID,
CUSTOMER_ID,
AMOUNT
FROM datos