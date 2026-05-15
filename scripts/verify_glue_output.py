# scripts/verify_glue_output.py
import boto3
import pandas as pd
import io

s3 = boto3.client('s3', region_name='ap-south-1')
PROCESSED_BUCKET = 'retail-pipeline-nikil-processed'

# List all processed files
print("\n--- Files in processed bucket ---")
response = s3.list_objects_v2(Bucket=PROCESSED_BUCKET, Prefix='sales/')

if 'Contents' not in response:
    print("No files found. Check if the Glue job succeeded.")
else:
    for obj in response['Contents']:
        size_kb = obj['Size'] / 1024
        print(f"  {obj['Key']}  ({size_kb:.1f} KB)")

    # Read the first parquet file found
    first_key = response['Contents'][0]['Key']
    print(f"\n--- Reading sample: {first_key} ---")

    obj = s3.get_object(Bucket=PROCESSED_BUCKET, Key=first_key)
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

    print(f"Rows    : {len(df)}")
    print(f"Columns : {list(df.columns)}")
    print(f"\nData types:")
    print(df.dtypes.to_string())
    print(f"\nFirst 5 rows:")
    print(df.head(5).to_string())

# Check quarantine
print("\n--- Quarantine files ---")
response_q = s3.list_objects_v2(Bucket=PROCESSED_BUCKET, Prefix='quarantine/')
if 'Contents' not in response_q:
    print("  No quarantine files (all data was clean)")
else:
    for obj in response_q['Contents']:
        print(f"  {obj['Key']}  ({obj['Size']/1024:.1f} KB)")