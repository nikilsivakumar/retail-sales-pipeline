import pandas as pd

# Read one partition directly from S3
import boto3, io

s3 = boto3.client('s3', region_name='ap-south-1')

# List files in the processed bucket
response = s3.list_objects_v2(
    Bucket='retail-pipeline-nikil-processed',
    Prefix='sales/year=2024/'
)

for obj in response['Contents']:
    print(obj['Key'], '-', obj['Size'], 'bytes')