

WITH dim_products AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY crm_p.start_date, crm_p.product_number) AS product_key, -- Surrogate Key
        crm_p.product_id AS product_id,
        crm_p.product_number AS product_number,
        crm_p.product_name AS product_name,
        crm_p.category_id AS category_id,
        erp_pc.category AS category,
        erp_pc.subcategory AS subcategory,
        erp_pc.maintenance_flag AS maintenance_flag,
        crm_p.product_line AS product_line,
        crm_p.start_date AS start_date
    FROM {{ source('source_silver', 'crm_prd_info') }} crm_p
    LEFT JOIN {{ source('source_silver', 'erp_cat_g1v2') }} erp_pc
        ON crm_p.category_id = erp_pc.category_id
    WHERE crm_p.end_date IS NULL
)

SELECT * FROM dim_products