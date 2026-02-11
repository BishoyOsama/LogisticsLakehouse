

{{
    config(
        materialized='incremental',
        unique_key=['order_number', 'product_number'],
        incremental_strategy='append',
        catalog='workspace'
    )
}}

WITH static_orders AS (
    SELECT
        *,
        NULL AS kafka_timestamp,
        FALSE AS is_synthetic
    FROM {{ source('source_silver', 'crm_sales_details') }}

    {% if is_incremental() %}
        --skip static table after first load
        WHERE 1=0
    {% endif %}
),

synthetic_orders AS (
    SELECT
        order_number,
        product_number,
        customer_id,
        order_date,
        ship_date,
        due_date,
        sales_amount,
        quantity,
        price,
        kafka_timestamp,
        TRUE AS is_synthetic
    FROM {{ source('source_silver', 'kafka_orders') }}

    {% if is_incremental()%}
        --only get new streaming data since last run
        WHERE kafka_timestamp > (SELECT MAX(kafka_timestamp) FROM {{ this }} WHERE is_synthetic = TRUE)

    {% endif %}
),

all_orders AS (
    SELECT * FROM static_orders
    UNION ALL
    SELECT * FROM synthetic_orders
)

SELECT
    ao.order_number AS order_number,
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,
    ao.order_date AS order_date,
    ao.ship_date AS ship_date,
    ao.due_date AS due_date,
    ao.sales_amount AS sales_amount,
    ao.quantity AS quantity,
    ao.price AS price,
    ao.kafka_timestamp AS kafka_timestamp,
    ao.is_synthetic AS is_synthetic
FROM all_orders ao
LEFT JOIN {{ ref('dim_products') }} pr
    ON ao.product_number = pr.product_number
LEFT JOIN {{ ref('dim_customers') }} cu
    ON ao.customer_id = cu.customer_id