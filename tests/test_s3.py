import boto3
import json
import os
from dotenv import load_dotenv

load_dotenv()

session = boto3.Session(profile_name='payflow')
s3 = session.client('s3', region_name='ap-southeast-1')

bucket = os.getenv('S3_BUCKET_NAME')

response = s3.list_objects_v2(Bucket=bucket)

print(f"Bucket: {bucket}")
print(f"Folders found:")
for obj in response.get('Contents', []):
    print(f"  {obj['Key']}")

test_data = json.dumps({"project": "payflow", "status": "ok"})
s3.put_object(Bucket=bucket, Key='bronze/test/connection_test.json', Body=test_data)
print(f"\nWrite test: OK")

read = s3.get_object(Bucket=bucket, Key='bronze/test/connection_test.json')
content = read['Body'].read().decode()
print(f"Read test: {content}")

s3.delete_object(Bucket=bucket, Key='bronze/test/connection_test.json')
print(f"Cleanup: OK")

print(f"\nS3 FULLY WORKING - ready to build")
