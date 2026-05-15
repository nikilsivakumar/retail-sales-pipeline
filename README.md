# Retail Sales Analytics Pipeline

End-to-end AWS data engineering pipeline — from raw CSV files
to a queryable data warehouse with automated runs.

## What it does

A new sales file lands in S3 → Lambda triggers automatically →
AWS Glue cleans and transforms it → stored as Parquet in S3 →
loaded into Redshift → dbt builds analytics models → ready for dashboards.

## Architecture
CSV Source
↓
S3 Raw bucket  ──→  Lambda (auto-trigger on upload)
↓
AWS Glue ETL (PySpark)
- Schema enforcement
- Quarantine bad records
- Parquet + partitioning
↓
S3 Processed bucket
(year=/month= partitions)
↓
┌───────────┴───────────┐
↓                       ↓
Glue Catalog            Redshift Serverless
+ Athena                staging.sales_raw
(ad-hoc SQL)                 ↓
dbt models
- stg_sales (view)
- mart_sales_by_region
- mart_product_performance
- mart_customer_cohorts


## Tech stack

| Layer | Service | Purpose |
|---|---|---|
| Ingestion | AWS Lambda | Auto-trigger on S3 file upload |
| Storage | Amazon S3 | Raw + processed data lake zones |
| Transform | AWS Glue (PySpark) | Schema enforcement, quarantine, Parquet |
| Catalog | Glue Data Catalog | Schema registry |
| Ad-hoc queries | Amazon Athena | SQL directly on S3 |
| Warehouse | Amazon Redshift Serverless | Production analytics |
| SQL models | dbt Core | Staging + mart layer with tests |
| Orchestration | Python orchestrator + Airflow DAG | Pipeline scheduling |

## Key engineering decisions

**Parquet over CSV** — columnar format, 5-10x compression.
Athena reads only the columns a query needs, not the whole file.

**Hive-style partitioning** (year=/month=) — queries filtered
by date scan only relevant partitions, not the full dataset.

**Quarantine pattern** — bad records written to a separate S3
path with a reason flag. Never silently dropped.

**Schema enforcement** — explicit StructType in the Glue job
catches upstream schema changes immediately instead of
silently corrupting downstream data.

**dbt staging/mart separation** — staging models are views
(lightweight, always current), mart models are tables
(pre-computed, fast for dashboards). Clean dependency chain.

**Idempotent Redshift loads** — DELETE then COPY pattern means
re-running the pipeline for the same date never creates duplicates.

## Project Structure

```text
retail-sales-pipeline/
├── data/
│   └── generate_data.py
├── glue_jobs/
│   └── transform_sales.py
├── scripts/
│   ├── upload_to_s3.py
│   ├── transform_local.py
│   ├── verify_glue_output.py
│   ├── fix_parquet_timestamps.py
│   └── run_pipeline.py
├── retail_dbt/
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   └── stg_sales.sql
│       └── marts/
│           ├── schema.yml
│           ├── mart_sales_by_region.sql
│           ├── mart_product_performance.sql
│           └── mart_customer_cohorts.sql
├── airflow/
│   └── dags/
│       └── sales_pipeline_dag.py
└── sql/
    └── redshift_ddl.sql
```
## How to run

**Full pipeline (one command):**
```bash
python scripts/run_pipeline.py
```

This generates 100 sales records, uploads to S3, triggers Glue,
waits for completion, loads Redshift, runs dbt models and tests.
Total runtime: ~140 seconds.

**Individual steps:**
```bash
# Generate mock data
python data/generate_data.py

# Upload to S3 (triggers Lambda → Glue automatically)
python scripts/upload_to_s3.py

# Run dbt only
cd retail_dbt && dbt run && dbt test
```

## Sample Athena queries

```sql
-- Revenue by region
SELECT region, SUM(revenue_clean) AS revenue
FROM retail_db.sales
GROUP BY region ORDER BY revenue DESC;

-- Partition pruning (scans only one month)
SELECT * FROM retail_db.sales
WHERE year = 2024 AND month = 1;

-- Monthly trend
SELECT year, month, COUNT(*) AS orders,
       ROUND(SUM(revenue_clean), 2) AS revenue
FROM retail_db.sales
GROUP BY year, month ORDER BY year, month;
```

## Pipeline output (Redshift)

- `staging.sales_raw` — raw load from S3
- `analytics.fact_sales` — cleaned fact table
- `analytics.daily_sales_summary` — pre-aggregated
- `analytics_staging.stg_sales` — dbt staging view
- `analytics_analytics.mart_sales_by_region` — monthly by region
- `analytics_analytics.mart_product_performance` — product rankings
- `analytics_analytics.mart_customer_cohorts` — cohort analysis
