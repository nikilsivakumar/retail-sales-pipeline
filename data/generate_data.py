# data/generate_data.py
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

def generate_sales_data(num_records=1500, output_path="data\\sample\\sales_raw.csv"):
    np.random.seed(42)
    random.seed(42)

    products = {
        'Laptop':     ('Electronics', 45000, 120000),
        'Phone':      ('Electronics', 15000, 80000),
        'Tablet':     ('Electronics', 20000, 60000),
        'Monitor':    ('Peripherals', 8000,  35000),
        'Keyboard':   ('Peripherals', 500,   5000),
        'Mouse':      ('Peripherals', 300,   3000),
        'Headphones': ('Audio',       1000,  20000),
        'Charger':    ('Accessories', 400,   2000),
    }
    regions = ['Chennai', 'Mumbai', 'Delhi', 'Bangalore', 'Hyderabad']
    payment_modes = ['UPI', 'Credit Card', 'Debit Card', 'Net Banking', 'COD']

    start_date = datetime(2024, 1, 1)
    rows = []

    for i in range(1, num_records + 1):
        product_name = random.choice(list(products.keys()))
        category, min_price, max_price = products[product_name]
        unit_price  = round(random.uniform(min_price, max_price), 2)
        quantity    = random.randint(1, 5)
        discount_pct = random.choice([0, 5, 10, 15, 20])
        revenue     = round(quantity * unit_price * (1 - discount_pct / 100), 2)
        order_date  = start_date + timedelta(days=random.randint(0, 364))

        rows.append({
            'order_id':     f'ORD-{i:05d}',
            'order_date':   order_date.strftime('%Y-%m-%d'),
            'customer_id':  f'CUST-{random.randint(1, 200):04d}',
            'product_name': product_name,
            'category':     category,
            'region':       random.choice(regions),
            'quantity':     quantity,
            'unit_price':   unit_price,
            'discount_pct': discount_pct,
            'revenue':      revenue,
            'payment_mode': random.choice(payment_modes),
        })

    df = pd.DataFrame(rows)

    # --- Inject intentional bad data (your pipeline will catch these) ---
    df.loc[df.sample(frac=0.03).index, 'customer_id'] = None   # anonymous users
    df.loc[df.sample(frac=0.01).index, 'unit_price']  = -999   # bad price entries
    df.loc[df.sample(frac=0.01).index, 'quantity']    = 0      # cancelled orders
    df.loc[df.sample(frac=0.05).index, 'region'] = \
        df.loc[df.sample(frac=0.05).index, 'region'].str.lower()   # casing errors
    df.loc[df.sample(frac=0.03).index, 'product_name'] = \
        '  ' + df.loc[df.sample(frac=0.03).index, 'product_name'] + '  '  # whitespace

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"\n✅ Generated {len(df)} records → {output_path}")
    print(f"\n--- Bad data injected (your pipeline will handle these) ---")
    print(f"Null customer_id  : {df['customer_id'].isna().sum()} rows")
    print(f"Negative price    : {(df['unit_price'] < 0).sum()} rows")
    print(f"Zero quantity     : {(df['quantity'] == 0).sum()} rows")
    print(f"\n--- First 5 rows preview ---")
    print(df.head(5).to_string())

if __name__ == "__main__":
    generate_sales_data()