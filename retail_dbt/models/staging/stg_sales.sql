WITH source AS (
    SELECT * FROM {{ source('retail_raw', 'sales_raw') }}
),
cleaned AS (
    SELECT
        order_id,
        order_date,
        COALESCE(customer_id, 'ANONYMOUS')   AS customer_id,
        TRIM(product_name)                    AS product_name,
        TRIM(category)                        AS category,
        INITCAP(TRIM(region))                 AS region,
        quantity,
        unit_price,
        discount_pct,
        payment_mode,
        day_of_week,
        is_weekend,
        revenue_clean                         AS revenue,
        processed_at,
        pipeline_version
    FROM source
    WHERE order_id IS NOT NULL
      AND unit_price > 0
      AND quantity > 0
)
SELECT * FROM cleaned
