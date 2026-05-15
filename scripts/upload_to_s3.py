# scripts/upload_to_s3.py
import boto3
import os
from datetime import datetime

def upload_to_s3(local_path, bucket_name, s3_key=None):
    """
    Upload a local file to S3 with date-partitioned path.
    
    Date partitioning means files are stored like:
    sales/year=2024/month=01/filename.csv
    
    This is called Hive-style partitioning.
    Athena and Glue understand this natively and only
    scan the partitions your query needs — much faster.
    """
    s3 = boto3.client('s3', region_name='ap-south-1')

    if s3_key is None:
        today = datetime.now()
        filename = os.path.basename(local_path)
        s3_key = f"sales/year={today.year}/month={today.month:02d}/{filename}"

    print(f"\nUploading...")
    print(f"  From : {local_path}")
    print(f"  To   : s3://{bucket_name}/{s3_key}")

    s3.upload_file(local_path, bucket_name, s3_key)

    # Confirm upload succeeded
    response = s3.head_object(Bucket=bucket_name, Key=s3_key)
    size_kb = response['ContentLength'] / 1024

    print(f"\n✅ Upload successful")
    print(f"   File size : {size_kb:.1f} KB")
    print(f"   Full path : s3://{bucket_name}/{s3_key}")

    return f"s3://{bucket_name}/{s3_key}"


if __name__ == "__main__":
    bucket = "retail-pipeline-nikil-raw"      # your raw bucket name
    local_file = "data\\sample\\sales_raw.csv"

    uri = upload_to_s3(local_file, bucket)
    print(f"\nYour file is now accessible from any AWS service at:")
    print(f"  {uri}")