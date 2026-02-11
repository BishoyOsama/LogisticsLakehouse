
/* Testing if the price multiplied by the quantity equals the sales amount */

SELECT
    order_number,
    product_key,
    customer_key,
    price,
    quantity,
    sales_amount
FROM {{ ref('fact_orders') }}
WHERE abs(sales_amount - (price * quantity)) > 0.00001 
AND price IS NOT NULL
AND quantity IS NOT NULL
AND sales_amount IS NOT NULL