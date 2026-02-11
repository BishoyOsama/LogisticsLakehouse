
WITH dim_customers AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY crm_c.customer_id) AS customer_key, -- Surrogate Key
        crm_c.customer_id AS customer_id,
        crm_c.customer_number AS customer_number,
        crm_c.first_name AS first_name,
        crm_c.last_name AS last_name,
        erp_loc.country AS country,
        crm_c.marital_status AS marital_status,
        CASE
            WHEN crm_c.gender <> "Unknown" THEN crm_c.gender
            ELSE COALESCE(erp_c.gender, "Unknown")
        END AS gender,
        erp_c.birth_date AS birth_date,
        crm_c.creation_date AS creation_date
    FROM {{ source('source_silver', 'crm_cust_info') }} crm_c
    LEFT JOIN {{ source('source_silver', 'erp_cust_az12') }} erp_c
        ON crm_c.customer_number = erp_c.customer_number
    LEFT JOIN {{ source('source_silver', 'erp_loc_a101') }} erp_loc
        ON erp_c.customer_number = erp_loc.customer_number
)

SELECT * FROM dim_customers