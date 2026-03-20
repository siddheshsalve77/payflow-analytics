import pandas as pd
import boto3
import json
import uuid
import random
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')

BANKS = ['SBI','HDFC','ICICI','AXIS','KOTAK','PNB','BOB','CANARA',
         'UNION','IDBI','INDUSIND','YES','FEDERAL','IOBK','CENTRAL',
         'BOI','UCO','PAYTM','AIRTEL','JIOPMTS','FINO','NSDL',
         'IDFC','BANDHAN','RBL','KARNATAKA','SARASWAT','TJSB','ABHYUDAYA','APMC']

MERCHANTS = [
    ('5411','Grocery Stores'),('5812','Restaurants'),('5541','Petrol Stations'),
    ('5912','Pharmacies'),('5311','Department Stores'),('5732','Electronics'),
    ('7011','Hotels'),('4121','Taxi Services'),('5621','Clothing'),
    ('7941','Sports and Recreation'),('8099','Health Services'),
    ('5200','Hardware Stores'),('5944','Jewellery Stores'),
    ('4814','Telecom Services'),('7832','Movie Theatres'),
    ('5942','Book Stores'),('5651','Family Clothing'),
    ('4111','Local Transport'),('7299','Personal Services'),
    ('8011','Doctors and Physicians')
]

MERCHANT_TIERS = ['Tier 1','Tier 1','Tier 1','Tier 2','Tier 2','Tier 3']

CITIES = ['Mumbai','Delhi','Bangalore','Hyderabad','Chennai','Pune',
          'Ahmedabad','Jaipur','Lucknow','Surat','Nagpur','Indore',
          'Bhopal','Patna','Vadodara','Coimbatore','Kochi','Ranchi']

UPI_APPS = ['GPay','PhonePe','Paytm','BHIM','AmazonPay','WhatsAppPay']

DEVICE_TYPES = ['Android','iOS','Web']

BASE_DATE = datetime(2024, 1, 1)

def convert_row_to_upi(row, idx):
    bank = random.choice(BANKS)
    merchant_code, merchant_name = random.choice(MERCHANTS)
    city = random.choice(CITIES)
    tier = random.choice(MERCHANT_TIERS)
    upi_app = random.choice(UPI_APPS)
    device = random.choice(DEVICE_TYPES)

    seconds_offset = int(row['Time'])
    txn_time = BASE_DATE + timedelta(seconds=seconds_offset)

    amount = round(float(row['Amount']), 2)
    if amount == 0.0:
        amount = round(random.uniform(1.0, 50.0), 2)

    is_fraud = int(row['Class']) == 1
    status = 'failed' if is_fraud and random.random() > 0.3 else 'success'

    sender_id = f"user{random.randint(1000,9999)}@{bank.lower()}"
    merchant_id = f"merchant{random.randint(100,999)}@{bank.lower()}"

    return {
        'transaction_id': str(uuid.uuid4()),
        'original_index': idx,
        'timestamp': txn_time.isoformat(),
        'sender_bank_code': bank,
        'sender_upi_id': sender_id,
        'merchant_upi_id': merchant_id,
        'merchant_category_code': merchant_code,
        'merchant_category_name': merchant_name,
        'merchant_tier': tier,
        'amount': amount,
        'currency': 'INR',
        'upi_app': upi_app,
        'status': status,
        'city': city,
        'device_type': device,
        'is_fraud': is_fraud,
        'fraud_score': round(abs(float(row['V1'])) / 10, 4),
        'data_source': 'creditcard_kaggle'
    }

def upload_chunk_to_s3(records, chunk_num, s3_client):
    date_str = f"2024/{chunk_num % 12 + 1:02d}/01"
    year = 2024
    month = chunk_num % 12 + 1
    day = 1

    key = f"bronze/year={year}/month={month:02d}/day={day:02d}/bulk_chunk_{chunk_num:04d}.json"

    json_lines = '\n'.join([json.dumps(r) for r in records])

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json_lines.encode('utf-8'),
        ContentType='application/json'
    )
    return key

def run_bulk_upload(chunk_size=10000):
    print(f"Starting bulk upload - {datetime.utcnow().isoformat()}")
    print(f"Reading creditcard.csv...")

    df = pd.read_csv('data/raw/kaggle/creditcard.csv')
    total_rows = len(df)
    fraud_count = int(df['Class'].sum())

    print(f"Total records: {total_rows:,}")
    print(f"Fraud records: {fraud_count:,} ({round(fraud_count/total_rows*100,3)}%)")
    print(f"Chunk size: {chunk_size:,}")
    print(f"Total chunks: {total_rows // chunk_size + 1}")
    print(f"Uploading to s3://{S3_BUCKET}/bronze/...")
    print()

    session = boto3.Session(profile_name='payflow')
    s3 = session.client('s3', region_name=AWS_REGION)

    total_uploaded = 0
    total_fraud_uploaded = 0
    chunk_num = 0

    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk_df = df.iloc[start:end]

        records = []
        for idx, row in chunk_df.iterrows():
            record = convert_row_to_upi(row, idx)
            records.append(record)

        fraud_in_chunk = sum(1 for r in records if r['is_fraud'])

        key = upload_chunk_to_s3(records, chunk_num, s3)

        total_uploaded += len(records)
        total_fraud_uploaded += fraud_in_chunk
        chunk_num += 1

        print(f"Chunk {chunk_num:02d} | Rows {start:,}-{end:,} | "
              f"Fraud: {fraud_in_chunk} | "
              f"Key: {key} | "
              f"Progress: {round(total_uploaded/total_rows*100,1)}%")

    print()
    print(f"Bulk upload complete - {datetime.utcnow().isoformat()}")
    print(f"Total records uploaded: {total_uploaded:,}")
    print(f"Total fraud records: {total_fraud_uploaded:,}")
    print(f"S3 Bronze now contains {chunk_num} chunk files")
    print(f"Ready for Glue Crawler")

if __name__ == "__main__":
    run_bulk_upload(chunk_size=10000)
