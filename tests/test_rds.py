import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('RDS_HOST'),
        port=int(os.getenv('RDS_PORT')),
        database=os.getenv('RDS_DB'),
        user=os.getenv('RDS_USER'),
        password=os.getenv('RDS_PASSWORD'),
        connect_timeout=10,
        sslmode='require'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"Connected to: {version[0][:60]}")
    cursor.execute("SELECT current_database();")
    db = cursor.fetchone()
    print(f"Database: {db[0]}")
    cursor.close()
    conn.close()
    print("")
    print("RDS CONNECTION FULLY WORKING")

except Exception as e:
    print(f"Connection failed: {e}")
    print("Check these:")
    print("1. RDS status is Available in AWS Console")
    print("2. RDS_HOST in .env has the correct endpoint")
    print("3. Security group allows your IP on port 5432")
    print("4. RDS_PASSWORD in .env matches what you set")