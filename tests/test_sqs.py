import boto3
import os
import json
from dotenv import load_dotenv

load_dotenv()

session = boto3.Session(profile_name='payflow')
sqs = session.client('sqs', region_name='ap-southeast-1')

queue_url = os.getenv('SQS_QUEUE_URL')

response = sqs.send_message(
    QueueUrl=queue_url,
    MessageBody=json.dumps({"test": "payflow", "status": "ok"})
)
print(f"Message sent. ID: {response['MessageId']}")

msgs = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=1,
    WaitTimeSeconds=2
)

if 'Messages' in msgs:
    msg = msgs['Messages'][0]
    print(f"Message received: {msg['Body']}")
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=msg['ReceiptHandle']
    )
    print("Message deleted from queue")
    print("")
    print("SQS FULLY WORKING")
else:
    print("No messages received - check queue URL")

