

/* Testing if there is a price less than zero */

SELECT
    order_number,
    product_key,
    customer_key,
    price
FROM {{ ref('fact_orders') }}
WHERE price < 0
