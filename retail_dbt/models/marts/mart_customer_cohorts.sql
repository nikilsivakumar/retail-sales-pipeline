WITH base AS (
    SELECT * FROM {{ ref('stg_sales') }}
),
first_purchase AS (
    SELECT
        customer_id,
        MIN(DATE_TRUNC('month', order_date))  AS cohort_month
    FROM base
    WHERE customer_id != 'ANONYMOUS'
    GROUP BY customer_id
),
cohort_activity AS (
    SELECT
        f.cohort_month,
        DATE_TRUNC('month', b.order_date)     AS activity_month,
        DATEDIFF('month', f.cohort_month,
            DATE_TRUNC('month', b.order_date)) AS months_since_first,
        b.customer_id,
        b.revenue
    FROM base b
    JOIN first_purchase f ON b.customer_id = f.customer_id
)
SELECT
    cohort_month,
    months_since_first,
    COUNT(DISTINCT customer_id)               AS active_customers,
    ROUND(SUM(revenue), 2)                    AS cohort_revenue,
    ROUND(AVG(revenue), 2)                    AS avg_revenue_per_customer
FROM cohort_activity
GROUP BY cohort_month, months_since_first
ORDER BY cohort_month, months_since_first