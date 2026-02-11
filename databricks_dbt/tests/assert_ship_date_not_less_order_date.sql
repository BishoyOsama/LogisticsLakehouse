/* Testing if the ship date is not less than the order date */

SELECT
    order_number,
    product_key,
    customer_key,
    order_date,
    ship_date
FROM {{ ref('fact_orders') }}
WHERE ship_date < order_date