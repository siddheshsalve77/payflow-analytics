import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('RDS_HOST'),
    port=int(os.getenv('RDS_PORT')),
    database='postgres',
    user=os.getenv('RDS_USER'),
    password=os.getenv('RDS_PASSWORD'),
    connect_timeout=10,
    sslmode='require'
)

conn.autocommit = True
cursor = conn.cursor()
cursor.execute("CREATE DATABASE payflow;")
print("Database 'payflow' created successfully")
cursor.close()
conn.close()