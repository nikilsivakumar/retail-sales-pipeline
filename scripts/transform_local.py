# scripts/transform_local.py
import pandas as pd
import os
from datetime import datetime

def transform_sales(input_path, output_dir):
    print("=" * 55)
    print("  RETAIL SALES TRANSFORMATION — LOCAL TEST RUN")
    print("=" * 55)

    # ── EXTRACT ──────────────────────────────────────────────
    df = pd.read_csv(input_path)
    print(f"\n[EXTRACT]")
    print(f"  File    : {input_path}")
    print(f"  Rows    : {len(df)}")
    print(f"  Columns : {list(df.columns)}")

    # ── AUDIT: what the raw data looks like ──────────────────
    print(f"\n[AUDIT — BEFORE CLEANING]")
    print(f"  Total rows         : {len(df)}")
    print(f"  Null customer_id   : {df['customer_id'].isna().sum()}")
    print(f"  Negative prices    : {(df['unit_price'] < 0).sum()}")
    print(f"  Zero quantity      : {(df['quantity'] == 0).sum()}")
    print(f"  Whitespace names   : {df['product_name'].str.startswith(' ').sum()}")

    # ── QUALITY SPLIT: good vs bad records ───────────────────
    # Bad rows go to quarantine — never silently dropped
    bad_mask = (
        df['order_id'].isna() |
        (df['unit_price'] < 0) |
        (df['quantity'] <= 0)
    )
    df_bad  = df[bad_mask].copy()
    df_good = df[~bad_mask].copy()

    print(f"\n[QUALITY SPLIT]")
    print(f"  Clean rows      : {len(df_good)}")
    print(f"  Quarantine rows : {len(df_bad)}")

    if len(df_bad) > 0:
        bad_path = os.path.join(output_dir, "quarantine", "bad_records.csv")
        os.makedirs(os.path.dirname(bad_path), exist_ok=True)
        df_bad.to_csv(bad_path, index=False)
        print(f"  Bad rows saved  : {bad_path}")

    # ── TRANSFORM ────────────────────────────────────────────
    df_clean = df_good.copy()

    # 1. Parse dates properly
    df_clean['order_date'] = pd.to_datetime(df_clean['order_date'])

    # 2. Extract date parts (used for partitioning + faster queries)
    df_clean['year']        = df_clean['order_date'].dt.year
    df_clean['month']       = df_clean['order_date'].dt.month
    df_clean['day']         = df_clean['order_date'].dt.day
    df_clean['day_of_week'] = df_clean['order_date'].dt.day_name()
    df_clean['is_weekend']  = df_clean['order_date'].dt.dayofweek >= 5

    # 3. Standardise region (fix CHENNAI / chennai / Chennai → Chennai)
    df_clean['region'] = df_clean['region'].str.strip().str.title()

    # 4. Strip whitespace from product names
    df_clean['product_name'] = df_clean['product_name'].str.strip()

    # 5. Fill anonymous customers
    df_clean['customer_id'] = df_clean['customer_id'].fillna('ANONYMOUS')

    # 6. Recalculate revenue from raw fields (don't trust source values)
    df_clean['revenue_clean'] = (
        df_clean['quantity'] *
        df_clean['unit_price'] *
        (1 - df_clean['discount_pct'] / 100)
    ).round(2)

    # 7. Flag mismatches between source revenue and recalculated
    df_clean['revenue_mismatch'] = (
        abs(df_clean['revenue'] - df_clean['revenue_clean']) > 0.01
    )

    # 8. Add audit columns
    df_clean['processed_at']     = datetime.now().isoformat()
    df_clean['pipeline_version'] = 'v1.0'

    # ── AUDIT: what the clean data looks like ────────────────
    print(f"\n[AUDIT — AFTER CLEANING]")
    print(f"  Final rows          : {len(df_clean)}")
    print(f"  Anonymous customers : {(df_clean['customer_id'] == 'ANONYMOUS').sum()}")
    print(f"  Revenue mismatches  : {df_clean['revenue_mismatch'].sum()}")
    print(f"\n  Region breakdown:")
    for region, count in df_clean['region'].value_counts().items():
        print(f"    {region:<15} : {count} orders")

    # ── LOAD: write Parquet, partitioned by year + month ─────
    print(f"\n[LOAD — Writing Parquet files]")
    for (year, month), group in df_clean.groupby(['year', 'month']):
        partition_path = os.path.join(output_dir, f"year={year}", f"month={month:02d}")
        os.makedirs(partition_path, exist_ok=True)
        out_file = os.path.join(partition_path, "sales.parquet")
        group.to_parquet(out_file, index=False)
        print(f"  year={year} month={month:02d} → {len(group)} rows → {out_file}")

    print(f"\n✅ Transformation complete\n")
    return df_clean


if __name__ == "__main__":
    df = transform_sales(
        input_path="data\\sample\\sales_raw.csv",
        output_dir="data\\processed"
    )

    print("--- Sample of final output ---")
    cols = ['order_id', 'order_date', 'region', 'product_name',
            'revenue_clean', 'year', 'month', 'processed_at']
    print(df[cols].head(8).to_string())