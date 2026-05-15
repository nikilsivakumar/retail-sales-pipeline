WITH base AS (
    SELECT * FROM {{ ref('stg_sales') }}
),
regional_monthly AS (
    SELECT
        DATE_TRUNC('month', order_date)       AS month,
        region,
        category,
        COUNT(DISTINCT order_id)              AS total_orders,
        COUNT(DISTINCT customer_id)           AS unique_customers,
        SUM(quantity)                         AS total_units_sold,
        ROUND(SUM(revenue), 2)                AS total_revenue,
        ROUND(AVG(revenue), 2)                AS avg_order_value,
        ROUND(SUM(SUM(revenue)) OVER (
            PARTITION BY region
            ORDER BY DATE_TRUNC('month', order_date)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2)                                 AS cumulative_revenue
    FROM base
    GROUP BY 1, 2, 3
)
SELECT * FROM regional_monthly
ORDER BY month, region