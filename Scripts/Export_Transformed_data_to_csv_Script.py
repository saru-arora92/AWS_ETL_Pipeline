import logging
import boto3
import pandas as pd
import json
from io import StringIO
from io import BytesIO
import io
from botocore.exceptions import ClientError
from datetime import date, datetime

client = boto3.client('s3')
bucket_name = 'dsp-data-lake-dev'

FileName = 'Unaligned6.csv'
csv_buffer = StringIO()
ppdata_df.to_csv(csv_buffer)
response = client.put_object(
        ACL = 'private',
     Body = csv_buffer.getvalue(),
     Bucket=bucket_name,
     Key=FileName
)