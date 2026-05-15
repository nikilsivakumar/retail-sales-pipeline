# scripts/run_pipeline.py
# Manual pipeline orchestrator — simulates what Airflow schedules daily
# Run this to trigger the full pipeline end to end

import boto3
import subprocess
import time
import pandas as pd
import numpy as np
import random
import os
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────
RAW_BUCKET = 'retail-pipeline-nikil-raw'
PROC_BUCKET = 'retail-pipeline-nikil-processed'
GLUE_JOB   = 'transform-sales-job'
ROLE_ARN   = 'arn:aws:iam::811821010882:role/RedshiftS3AccessRole'
WORKGROUP  = 'retail-workgroup'
REGION     = 'ap-south-1'
DBT_DIR    = r'C:\Users\ASUS\Documents\AWS Data Eng\Study\Project\retail-sales-pipeline\retail_dbt'

def step1_generate_and_upload():
    print("\n[STEP 1] Generating and uploading daily sales file...")
    today = datetime.now()
    np.random.seed(int(today.timestamp()) % 10000)
    random.seed(int(today.timestamp()) % 10000)

    products = {
        'Laptop'    : ('Electronics', 45000, 120000),
        'Phone'     : ('Electronics', 15000, 80000),
        'Tablet'    : ('Electronics', 20000, 60000),
        'Monitor'   : ('Peripherals', 8000,  35000),
        'Keyboard'  : ('Peripherals', 500,   5000),
        'Mouse'     : ('Peripherals', 300,   3000),
        'Headphones': ('Audio',       1000,  20000),
        'Charger'   : ('Accessories', 400,   2000),
    }
    regions = ['Chennai', 'Mumbai', 'Delhi', 'Bangalore', 'Hyderabad']

    rows = []
    for i in range(1, 101):
        product          = random.choice(list(products.keys()))
        category, lo, hi = products[product]
        qty              = random.randint(1, 5)
        price            = round(random.uniform(lo, hi), 2)
        disc             = random.choice([0, 5, 10, 15, 20])
        rows.append({
            'order_id'    : f'ORD-{today.strftime("%Y%m%d")}-{i:03d}',
            'order_date'  : today.strftime('%Y-%m-%d'),
            'customer_id' : f'CUST-{random.randint(1, 200):04d}',
            'product_name': product,
            'category'    : category,
            'region'      : random.choice(regions),
            'quantity'    : qty,
            'unit_price'  : price,
            'discount_pct': disc,
            'revenue'     : round(qty * price * (1 - disc/100), 2),
            'payment_mode': random.choice(['UPI', 'Credit Card', 'COD']),
        })

    df       = pd.DataFrame(rows)
    filename = f'sales_{today.strftime("%Y%m%d")}.csv'
    df.to_csv(filename, index=False)

    s3_key = (f'sales/year={today.year}/'
              f'month={today.month:02d}/{filename}')

    s3 = boto3.client('s3', region_name=REGION)
    s3.upload_file(filename, RAW_BUCKET, s3_key)
    os.remove(filename)

    print(f"  ✅ Uploaded {len(df)} rows → s3://{RAW_BUCKET}/{s3_key}")
    return s3_key


def step2_trigger_and_wait_glue(s3_key):
    print("\n[STEP 2] Triggering Glue ETL job...")
    glue = boto3.client('glue', region_name=REGION)

    response = glue.start_job_run(
        JobName=GLUE_JOB,
        Arguments={
            '--SOURCE_BUCKET': RAW_BUCKET,
            '--SOURCE_KEY'   : s3_key,
            '--TARGET_BUCKET': PROC_BUCKET,
        }
    )
    run_id = response['JobRunId']
    print(f"  Glue job started: {run_id}")
    print("  Waiting for completion (this takes 2-4 minutes)...")

    while True:
        resp   = glue.get_job_run(JobName=GLUE_JOB, RunId=run_id)
        status = resp['JobRun']['JobRunState']
        print(f"  Status: {status}")

        if status == 'SUCCEEDED':
            print("  ✅ Glue job succeeded")
            return
        elif status in ['FAILED', 'ERROR', 'TIMEOUT', 'STOPPED']:
            error = resp['JobRun'].get('ErrorMessage', '')
            raise Exception(f"Glue job failed: {status} — {error}")

        time.sleep(30)


def step3_load_redshift():
    print("\n[STEP 3] Loading processed data into Redshift...")
    client = boto3.client('redshift-data', region_name=REGION)
    today  = datetime.now()

    statements = [
        f"DELETE FROM staging.sales_raw WHERE order_date = '{today.strftime('%Y-%m-%d')}'",
        f"""COPY staging.sales_raw
            FROM 's3://{PROC_BUCKET}/sales/year={today.year}/month={today.month}/'
            IAM_ROLE '{ROLE_ARN}'
            FORMAT AS PARQUET
            SERIALIZETOJSON"""
    ]

    for sql in statements:
        resp    = client.execute_statement(
            WorkgroupName=WORKGROUP, Database='dev', Sql=sql
        )
        stmt_id = resp['Id']
        print(f"  Running: {sql[:60]}...")

        while True:
            status = client.describe_statement(Id=stmt_id)['Status']
            if status == 'FINISHED':
                print(f"  ✅ Done")
                break
            elif status in ['FAILED', 'ABORTED']:
                err = client.describe_statement(Id=stmt_id).get('Error', '')
                raise Exception(f"Redshift SQL failed: {err}")
            time.sleep(5)


def step4_run_dbt():
    print("\n[STEP 4] Running dbt models and tests...")
    result = subprocess.run(
        'dbt run && dbt test',
        shell=True,
        cwd=DBT_DIR,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("dbt run failed")
    print("  ✅ dbt models and tests passed")


def run_pipeline():
    print("=" * 55)
    print("  RETAIL SALES PIPELINE — MANUAL RUN")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    start = time.time()

    s3_key = step1_generate_and_upload()
    step2_trigger_and_wait_glue(s3_key)
    step3_load_redshift()
    step4_run_dbt()

    elapsed = round(time.time() - start, 1)
    print(f"\n{'='*55}")
    print(f"  ✅ PIPELINE COMPLETE in {elapsed}s")
    print(f"{'='*55}")


if __name__ == "__main__":
    run_pipeline()