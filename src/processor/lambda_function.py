import boto3
import json
import os
from datetime import datetime, timezone

S3_BUCKET = os.environ.get("S3_BUCKET_NAME")
SNS_FRAUD_ARN = os.environ.get("SNS_FRAUD_TOPIC_ARN")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")

REQUIRED_FIELDS = ["transaction_id","timestamp","sender_bank_code","sender_upi_id","merchant_upi_id","merchant_category_code","amount","currency","upi_app","status"]

VALID_BANK_CODES = ["SBI","HDFC","ICICI","AXIS","KOTAK","PNB","BOB","CANARA","UNION","IDBI","INDUSIND","YES","FEDERAL","IOBK","CENTRAL","BOI","UCO","PAYTM","AIRTEL","JIOPMTS","FINO","NSDL","IDFC","BANDHAN","RBL","KARNATAKA","SARASWAT","TJSB","ABHYUDAYA","APMC"]

fraud_velocity_tracker = {}

def validate_event(t):
    for f in REQUIRED_FIELDS:
        if f not in t or t[f] is None:
            return False, f"Missing field: {f}"
    if not isinstance(t["amount"], (int, float)):
        return False, "Amount not a number"
    if t["amount"] <= 0 or t["amount"] > 200000:
        return False, f"Invalid amount: {t['amount']}"
    if t["sender_bank_code"] not in VALID_BANK_CODES:
        return False, f"Unknown bank: {t['sender_bank_code']}"
    if t["currency"] != "INR":
        return False, f"Invalid currency: {t['currency']}"
    return True, None

def check_fraud(t):
    sender = t["sender_upi_id"]
    now = datetime.now(timezone.utc)
    if sender not in fraud_velocity_tracker:
        fraud_velocity_tracker[sender] = []
    fraud_velocity_tracker[sender].append(now)
    cutoff = now.timestamp() - 300
    fraud_velocity_tracker[sender] = [x for x in fraud_velocity_tracker[sender] if x.timestamp() > cutoff]
    if len(fraud_velocity_tracker[sender]) >= 5:
        return True, f"Velocity fraud: {len(fraud_velocity_tracker[sender])} in 5 mins"
    if t.get("is_fraud", False):
        return True, "Fraud flag set by simulator"
    return False, None

def write_bronze(t, s3):
    key = f"bronze/year={datetime.utcnow().year}/month={datetime.utcnow().strftime('%m')}/day={datetime.utcnow().strftime('%d')}/{t['transaction_id']}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(t), ContentType="application/json")
    return key

def write_fraud(t, s3):
    key = f"fraud-alerts/{datetime.utcnow().strftime('%Y/%m/%d')}/{t['transaction_id']}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(t), ContentType="application/json")
    return key

def send_alert(t, reason, sns):
    msg = f"FRAUD ALERT\nTransaction: {t['transaction_id']}\nAmount: Rs {t['amount']}\nSender: {t['sender_upi_id']}\nBank: {t['sender_bank_code']}\nReason: {reason}"
    sns.publish(TopicArn=SNS_FRAUD_ARN, Subject=f"FRAUD ALERT Rs {t['amount']} {t['sender_bank_code']}", Message=msg)

def lambda_handler(event, context):
    print(f"Triggered with {len(event.get('Records',[]))} records")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    sns = boto3.client("sns", region_name=AWS_REGION)
    results = {"processed":0,"valid":0,"fraud":0,"invalid":0,"errors":0}
    for record in event.get("Records", []):
        try:
            t = json.loads(record["body"])
            results["processed"] += 1
            valid, reason = validate_event(t)
            if not valid:
                results["invalid"] += 1
                print(f"INVALID: {reason}")
                continue
            is_fraud, fraud_reason = check_fraud(t)
            if is_fraud:
                write_fraud(t, s3)
                send_alert(t, fraud_reason, sns)
                results["fraud"] += 1
                print(f"FRAUD: {t['transaction_id'][:8]}")
            else:
                write_bronze(t, s3)
                results["valid"] += 1
                print(f"OK: {t['transaction_id'][:8]}")
        except Exception as e:
            results["errors"] += 1
            print(f"Error: {e}")
    print(f"Results: {results}")
    return results
