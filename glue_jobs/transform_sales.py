# glue_jobs/transform_sales.py

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
import logging

# ── LOGGING SETUP ────────────────────────────────────────────
# These logs appear in CloudWatch — your window into what the job is doing
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── JOB INIT ─────────────────────────────────────────────────
# Glue passes arguments into your script via sys.argv
# getResolvedOptions extracts them by name
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_BUCKET',
    'TARGET_BUCKET',
    'SOURCE_KEY'
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_KEY']}"
TARGET_PATH = f"s3://{args['TARGET_BUCKET']}/sales/"

logger.info(f"Job started")
logger.info(f"Source : {SOURCE_PATH}")
logger.info(f"Target : {TARGET_PATH}")

# ── STEP 1: EXTRACT ──────────────────────────────────────────
# Read CSV from S3 raw bucket
# inferSchema=true lets Spark guess types — fine for dev, 
# we'll enforce schema explicitly right after
df_raw = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(SOURCE_PATH))

raw_count = df_raw.count()
logger.info(f"Raw record count: {raw_count}")

# ── STEP 2: ENFORCE SCHEMA ───────────────────────────────────
# Never rely on inferSchema in production.
# Source systems can change column types without warning.
# Explicit schema = your job fails loudly instead of silently corrupting data.
schema = StructType([
    StructField("order_id",     StringType(),  True),
    StructField("order_date",   StringType(),  True),
    StructField("customer_id",  StringType(),  True),
    StructField("product_name", StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("region",       StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("discount_pct", IntegerType(), True),
    StructField("revenue",      DoubleType(),  True),
    StructField("payment_mode", StringType(),  True),
])

df = (spark.read
    .schema(schema)
    .option("header", "true")
    .csv(SOURCE_PATH))

# ── STEP 3: DATA QUALITY SPLIT ───────────────────────────────
# Separate bad records into a quarantine zone.
# NEVER silently drop bad rows — always preserve them for investigation.
# In an interview: "We use a quarantine pattern — bad records are written
# to a separate S3 path with the reason flagged, so the data team can
# audit, fix, and reprocess them."

bad_records = df.filter(
    F.col("order_id").isNull() |
    F.col("unit_price").isNull() |
    (F.col("unit_price") < 0) |
    F.col("quantity").isNull() |
    (F.col("quantity") <= 0)
).withColumn("quarantine_reason",
    F.when(F.col("order_id").isNull(), "null_order_id")
     .when(F.col("unit_price").isNull(), "null_unit_price")
     .when(F.col("unit_price") < 0, "negative_unit_price")
     .when(F.col("quantity").isNull(), "null_quantity")
     .when(F.col("quantity") <= 0, "zero_or_negative_quantity")
     .otherwise("unknown")
)

bad_count = bad_records.count()
logger.info(f"Quarantine records: {bad_count}")

if bad_count > 0:
    (bad_records.write
        .mode("append")
        .parquet(f"s3://{args['TARGET_BUCKET']}/quarantine/"))
    logger.info(f"Bad records written to quarantine")

# Clean records only
df_clean = df.filter(
    F.col("order_id").isNotNull() &
    F.col("unit_price").isNotNull() &
    (F.col("unit_price") >= 0) &
    F.col("quantity").isNotNull() &
    (F.col("quantity") > 0)
)

logger.info(f"Clean records: {df_clean.count()}")

# ── STEP 4: TRANSFORM ────────────────────────────────────────

df_transformed = (df_clean

    # Parse order_date string → proper date type
    .withColumn("order_date",
        F.to_date(F.col("order_date"), "yyyy-MM-dd"))

    # Extract date parts for partitioning and query performance
    .withColumn("year",        F.year("order_date"))
    .withColumn("month",       F.month("order_date"))
    .withColumn("day",         F.dayofmonth("order_date"))
    .withColumn("day_of_week", F.date_format("order_date", "EEEE"))
    .withColumn("is_weekend",
        F.dayofweek("order_date").isin([1, 7]))

    # Standardise region casing (chennai / CHENNAI → Chennai)
    .withColumn("region",
        F.initcap(F.trim(F.col("region"))))

    # Strip whitespace from product names
    .withColumn("product_name",
        F.trim(F.col("product_name")))

    # Fill anonymous customers
    .withColumn("customer_id",
        F.when(F.col("customer_id").isNull(), "ANONYMOUS")
         .otherwise(F.col("customer_id")))

    # Recalculate revenue from source fields — don't trust source value
    .withColumn("revenue_clean",
        F.round(
            F.col("quantity") * F.col("unit_price") *
            (1 - F.col("discount_pct") / 100),
            2
        ))

    # Flag where source revenue differs from recalculated
    .withColumn("revenue_mismatch",
        F.abs(F.col("revenue") - F.col("revenue_clean")) > 0.01)

    # Drop the original revenue column — use revenue_clean going forward
    .drop("revenue")

    # Add pipeline audit columns
    .withColumn("processed_at",      F.current_timestamp())
    .withColumn("pipeline_version",  F.lit("v1.0"))
)

# ── STEP 5: LOAD ─────────────────────────────────────────────
# Write as Parquet, partitioned by year and month.
#
# Why Parquet?
#   Columnar format — Athena reads only the columns your query needs.
#   Built-in compression — typically 5-10x smaller than CSV.
#
# Why partition by year/month?
#   A query for "sales in January 2024" only scans the year=2024/month=01
#   folder. Without partitioning, it scans the entire dataset.
#   On 1TB of data, partitioning turns a 10-minute query into a 5-second one.

(df_transformed.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(TARGET_PATH))

final_count = df_transformed.count()
logger.info(f"Records written to processed zone: {final_count}")
logger.info(f"Output path: {TARGET_PATH}")
logger.info(f"Job complete")

job.commit()