import boto3
import psycopg2
import json
import uuid
import random
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
print("Simulator module loaded successfully")
DB_CONFIG = {
    'host': os.getenv('RDS_HOST'),
    'port': int(os.getenv('RDS_PORT')),
    'database': os.getenv('RDS_DB'),
    'user': os.getenv('RDS_USER'),
    'password': os.getenv('RDS_PASSWORD'),
    'sslmode': 'require',
    'connect_timeout': 10
}

SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')

UPI_APPS = ['GPay', 'PhonePe', 'Paytm', 'BHIM', 'AmazonPay', 'WhatsAppPay']
CITIES = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai', 'Pune',
          'Ahmedabad', 'Jaipur', 'Lucknow', 'Surat', 'Nagpur', 'Indore']
DEVICE_TYPES = ['Android', 'iOS', 'Web']

AMOUNT_RANGES = {
    '5411': (50, 2000),
    '5812': (100, 1500),
    '5541': (500, 5000),
    '5912': (50, 800),
    '5311': (200, 10000),
    '5732': (500, 50000),
    '7011': (1000, 20000),
    '4121': (50, 500),
    '5813': (200, 3000),
    '5621': (300, 5000),
}

FAILURE_REASONS = [
    'insufficient_funds',
    'bank_server_timeout',
    'invalid_upi_pin',
    'daily_limit_exceeded',
    'account_blocked',
    'network_error'
]

def get_reference_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT bank_code, bank_name, upi_handle FROM dim_bank;")
    banks = cursor.fetchall()

    cursor.execute("SELECT mcc_code, category_name, merchant_tier FROM dim_merchant;")
    merchants = cursor.fetchall()

    cursor.close()
    conn.close()

    return banks, merchants

def generate_transaction(banks, merchants, is_fraud=False):
    bank = random.choice(banks)
    merchant = random.choice(merchants)
    mcc = merchant[0]

    amount_range = AMOUNT_RANGES.get(mcc, (10, 5000))
    amount = round(random.uniform(amount_range[0], amount_range[1]), 2)

    if is_fraud:
        amount = round(random.uniform(1, 50), 2)

    is_success = random.random() > 0.08
    status = 'success' if is_success else 'failed'
    failure_reason = None if is_success else random.choice(FAILURE_REASONS)

    sender_id = f"user{random.randint(1000, 9999)}@{bank[2].replace('@', '')}"
    merchant_id = f"merchant{random.randint(100, 999)}@{bank[2].replace('@', '')}"

    transaction = {
        'transaction_id': str(uuid.uuid4()),
        'timestamp': datetime.utcnow().isoformat(),
        'sender_bank_code': bank[0],
        'sender_bank_name': bank[1],
        'sender_upi_id': sender_id,
        'merchant_upi_id': merchant_id,
        'merchant_category_code': mcc,
        'merchant_category_name': merchant[1],
        'merchant_tier': merchant[2],
        'amount': amount,
        'currency': 'INR',
        'upi_app': random.choice(UPI_APPS),
        'status': status,
        'failure_reason': failure_reason,
        'city': random.choice(CITIES),
        'device_type': random.choice(DEVICE_TYPES),
        'is_fraud': is_fraud
    }

    return transaction

def send_to_sqs(transactions):
    session = boto3.Session(profile_name='payflow')
    sqs = session.client('sqs', region_name=AWS_REGION)

    success_count = 0
    fail_count = 0

    for txn in transactions:
        try:
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(txn),
                MessageAttributes={
                    'transaction_type': {
                        'StringValue': 'fraud' if txn['is_fraud'] else 'normal',
                        'DataType': 'String'
                    }
                }
            )
            success_count += 1
        except Exception as e:
            print(f"Failed to send transaction {txn['transaction_id']}: {e}")
            fail_count += 1

    return success_count, fail_count

def run_simulator(batch_size=50):
    print(f"Starting PayFlow simulator - {datetime.utcnow().isoformat()}")
    print(f"Loading reference data from RDS...")

    banks, merchants = get_reference_data()
    print(f"Loaded {len(banks)} banks and {len(merchants)} merchants")

    transactions = []

    fraud_indices = random.sample(range(batch_size), 1)

    for i in range(batch_size):
        is_fraud = i in fraud_indices
        txn = generate_transaction(banks, merchants, is_fraud)
        transactions.append(txn)

    fraud_count = sum(1 for t in transactions if t['is_fraud'])
    print(f"Generated {len(transactions)} transactions ({fraud_count} fraud)")

    print(f"Sending to SQS queue...")
    success, failed = send_to_sqs(transactions)

    print(f"Sent: {success} success, {failed} failed")
    print(f"Simulator run complete - {datetime.utcnow().isoformat()}")
    print("")

    return transactions

if __name__ == "__main__":
    transactions = run_simulator(batch_size=50)

    print("Sample transactions:")
    for txn in transactions[:3]:
        print(f"  {txn['transaction_id'][:8]}... | {txn['sender_bank_code']} | "
              f"Rs {txn['amount']} | {txn['merchant_category_name']} | "
              f"{txn['status']} | fraud={txn['is_fraud']}")
