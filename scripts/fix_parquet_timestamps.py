# scripts/fix_parquet_timestamps.py
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os

s3 = boto3.client('s3', region_name='ap-south-1')
PROCESSED_BUCKET = 'retail-pipeline-nikil-processed'

# List all parquet files in the sales folder
response = s3.list_objects_v2(
    Bucket=PROCESSED_BUCKET,
    Prefix='sales/'
)

if 'Contents' not in response:
    print("No files found")
    exit()

files = [obj['Key'] for obj in response['Contents'] 
         if obj['Key'].endswith('.parquet')]

print(f"Found {len(files)} parquet files to fix")

for key in files:
    print(f"\nProcessing: {key}")
    
    # Download
    obj = s3.get_object(Bucket=PROCESSED_BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    
    # Convert timestamp columns from ns to us (microseconds)
    # Redshift handles microseconds cleanly
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].astype('datetime64[us]')
    
    print(f"  Rows: {len(df)}")
    print(f"  processed_at dtype: {df['processed_at'].dtype}")
    
    # Write back to parquet buffer
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    
    # Upload back to same S3 path
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=key,
        Body=buffer.getvalue()
    )
    print(f"  Uploaded back to s3://{PROCESSED_BUCKET}/{key}")

print(f"\n✅ All {len(files)} files fixed and re-uploaded")