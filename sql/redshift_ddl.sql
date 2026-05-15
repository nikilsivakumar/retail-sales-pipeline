-- Redshift table definitions
-- Run once to set up the warehouse schema

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS staging.sales_raw (
    order_id          VARCHAR(20),
    order_date        DATE,
    customer_id       VARCHAR(20),
    product_name      VARCHAR(100),
    category          VARCHAR(50),
    region            VARCHAR(50),
    quantity          INTEGER,
    unit_price        DOUBLE PRECISION,
    discount_pct      INTEGER,
    payment_mode      VARCHAR(50),
    day               INTEGER,
    day_of_week       VARCHAR(20),
    is_weekend        BOOLEAN,
    revenue_clean     DOUBLE PRECISION,
    revenue_mismatch  BOOLEAN,
    processed_at      TIMESTAMP,
    pipeline_version  VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    sale_key       BIGINT IDENTITY(1,1),
    order_id       VARCHAR(20)   NOT NULL,
    order_date     DATE          NOT NULL,
    customer_id    VARCHAR(20),
    product_name   VARCHAR(100),
    category       VARCHAR(50),
    region         VARCHAR(50),
    quantity       INTEGER,
    unit_price     DOUBLE PRECISION,
    discount_pct   INTEGER,
    payment_mode   VARCHAR(50),
    day_of_week    VARCHAR(20),
    is_weekend     BOOLEAN,
    revenue        DOUBLE PRECISION,
    processed_at   TIMESTAMP,
    PRIMARY KEY (sale_key)
)
DISTSTYLE KEY
DISTKEY (region)
SORTKEY (order_date);

CREATE TABLE IF NOT EXISTS analytics.daily_sales_summary (
    summary_date     DATE,
    region           VARCHAR(50),
    category         VARCHAR(50),
    total_orders     INTEGER,
    total_revenue    DOUBLE PRECISION,
    avg_order_value  DOUBLE PRECISION,
    unique_customers INTEGER,
    PRIMARY KEY (summary_date, region, category)
);