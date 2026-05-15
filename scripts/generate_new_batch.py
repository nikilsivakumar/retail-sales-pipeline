# scripts/generate_new_batch.py
# Run this to simulate a new daily file arriving
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

np.random.seed(99)   # different seed = different data
random.seed(99)

products = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse', 'Headphones', 'Charger']
regions  = ['Chennai', 'Mumbai', 'Delhi', 'Bangalore', 'Hyderabad']
categories = {
    'Laptop': 'Electronics', 'Phone': 'Electronics', 'Tablet': 'Electronics',
    'Monitor': 'Peripherals', 'Keyboard': 'Peripherals', 'Mouse': 'Peripherals',
    'Headphones': 'Audio', 'Charger': 'Accessories'
}

rows = []
start = datetime(2024, 6, 1)

for i in range(1, 201):
    product   = random.choice(products)
    qty       = random.randint(1, 5)
    price     = round(random.uniform(300, 120000), 2)
    disc      = random.choice([0, 5, 10, 15, 20])
    date      = start + timedelta(days=random.randint(0, 30))

    rows.append({
        'order_id':     f'ORD-JUNE-{i:04d}',
        'order_date':   date.strftime('%Y-%m-%d'),
        'customer_id':  f'CUST-{random.randint(1, 200):04d}',
        'product_name': product,
        'category':     categories[product],
        'region':       random.choice(regions),
        'quantity':     qty,
        'unit_price':   price,
        'discount_pct': disc,
        'revenue':      round(qty * price * (1 - disc/100), 2),
        'payment_mode': random.choice(['UPI', 'Credit Card', 'COD']),
    })

df = pd.DataFrame(rows)
df.to_csv('data\\sample\\sales_june_batch.csv', index=False)
print(f"Generated {len(df)} records → data\\sample\\sales_june_batch.csv")