import boto3
import os
from dotenv import load_dotenv

load_dotenv()

session = boto3.Session(profile_name='payflow')
sns = session.client('sns', region_name='ap-southeast-1')

fraud_topic = os.getenv('SNS_FRAUD_TOPIC_ARN')
ops_topic = os.getenv('SNS_OPS_TOPIC_ARN')

response1 = sns.publish(
    TopicArn=fraud_topic,
    Subject='PayFlow Test - Fraud Alert',
    Message='This is a test fraud alert from PayFlow Analytics pipeline. If you received this email SNS is working correctly.'
)
print(f"Fraud alert sent. MessageId: {response1['MessageId']}")

response2 = sns.publish(
    TopicArn=ops_topic,
    Subject='PayFlow Test - Pipeline Ops',
    Message='This is a test pipeline ops notification from PayFlow Analytics. If you received this email SNS is working correctly.'
)
print(f"Ops notification sent. MessageId: {response2['MessageId']}")

print("")
print("SNS FULLY WORKING")
print("Check your email - you should receive 2 test emails")

