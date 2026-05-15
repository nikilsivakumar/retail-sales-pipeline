WITH base AS (
    SELECT * FROM {{ ref('stg_sales') }}
),
product_stats AS (
    SELECT
        product_name,
        category,
        COUNT(DISTINCT order_id)              AS total_orders,
        SUM(quantity)                         AS units_sold,
        ROUND(SUM(revenue), 2)                AS total_revenue,
        ROUND(AVG(revenue), 2)                AS avg_order_value,
        ROUND(AVG(discount_pct), 1)           AS avg_discount_pct,
        COUNT(DISTINCT customer_id)           AS unique_buyers,
        RANK() OVER (
            PARTITION BY category
            ORDER BY SUM(revenue) DESC
        )                                     AS revenue_rank_in_category
    FROM base
    GROUP BY product_name, category
)
SELECT * FROM product_stats
ORDER BY total_revenue DESC