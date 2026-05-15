import pandas as pd
df = pd.read_parquet("data\\processed\\year=2024\\month=01\\sales.parquet")
print(df.dtypes)
print(df.shape)
print(df.head(3).to_string())