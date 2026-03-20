# PayFlow Analytics — UPI Payments Intelligence Platform

> End-to-end AWS data engineering pipeline for real-time UPI payment fraud detection and analytics — built entirely on AWS free tier using Lambda, Glue, Athena, S3, RDS, and SQS.

[![AWS](https://img.shields.io/badge/AWS-Cloud-orange)](https://aws.amazon.com)
[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.0-red)](https://spark.apache.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## What is PayFlow Analytics

PayFlow Analytics is a production-grade, cloud-native data platform that simulates a UPI payments intelligence system. It ingests live transaction events, detects fraud in real time, processes 284,807 real anonymised transactions through a Medallion Architecture, and serves analytics via Amazon Athena — all running on AWS free tier with zero manual intervention after deployment.

This project demonstrates the full data engineering lifecycle — streaming ingestion, event-driven processing, serverless transformation, data lake management, schema governance, and analytical querying — using AWS-native services exclusively.

---

## Architecture Overview
```
Data Sources (RDS + Kaggle Credit Card Dataset)
              ↓
    Python Transaction Simulator
              ↓
    Amazon SQS (main queue + DLQ)
              ↓
    AWS Lambda Processor (fraud detection + routing)
              ↓
    ┌─────────────────────────────────┐
    │         Amazon S3 Data Lake     │
    │  Bronze → Silver → Gold         │
    │  (JSON)   (Parquet) (Aggregated)│
    └─────────────────────────────────┘
              ↓
    AWS Glue ETL (PySpark transformation)
              ↓
    AWS Glue Data Catalog (schema registry)
              ↓
    Amazon Athena (serverless SQL analytics)
              ↓
    Amazon SNS (batch fraud alerts via email)
```

---

## Tech Stack

| Category | Technology | Purpose |
|---|---|---|
| Language | Python 3.10 | Simulator, Lambda functions, ETL scripts |
| Processing | PySpark (AWS Glue) | Bronze → Silver → Gold transformation |
| Storage | Amazon S3 | Data lake — all three Medallion layers |
| Database | Amazon RDS (PostgreSQL) | Reference data + audit logs |
| Messaging | Amazon SQS | Event buffer with dead letter queue |
| Compute | AWS Lambda | Serverless fraud detection + routing |
| Catalog | AWS Glue Data Catalog | Unified schema registry |
| Query | Amazon Athena | Serverless SQL on S3 |
| Alerts | Amazon SNS | Batch fraud summary emails |
| Security | AWS IAM | Role-based access, zero hardcoded credentials |
| Monitoring | AWS CloudWatch | Lambda logs + pipeline metrics |

---

## Dataset

| Source | Records | Description |
|---|---|---|
| Kaggle Credit Card Fraud Detection | 284,807 | Real anonymised transactions with fraud labels |
| RBI Payment System Reports | Reference | Indian bank names, MCC codes, city tiers |
| Python Simulator | Live stream | Realistic UPI events generated from RDS reference data |

**Key statistics:**
- Total transactions: 284,807
- Fraud cases: 492 (0.173% — matching real-world UPI fraud rate)
- Total transaction value: Rs 2.56 crore
- Banks covered: 30 Indian banks including SBI, HDFC, ICICI, PhonePe, Paytm
- Merchant categories: 20 MCC codes across Tier 1, 2, and 3 cities

---

## Project Structure
```
payflow-analytics/
├── src/
│   ├── simulator/
│   │   ├── simulate.py          # Live UPI transaction generator
│   │   └── bulk_upload.py       # Historical data bulk loader (284K records)
│   ├── processor/
│   │   └── lambda_function.py   # Lambda fraud detection + S3 routing
│   ├── glue_jobs/
│   │   ├── bronze_to_silver.py  # PySpark Bronze → Silver ETL
│   │   └── silver_to_gold.py    # PySpark Silver → Gold aggregations
│   └── orchestrator/
│       └── lambda_function.py   # Batch pipeline orchestrator
├── tests/
│   ├── test_s3.py               # S3 connectivity verification
│   ├── test_rds.py              # RDS connectivity verification
│   ├── test_sqs.py              # SQS connectivity verification
│   └── test_sns.py              # SNS connectivity verification
├── sql/
│   ├── rds_schema.sql           # RDS reference table definitions
│   └── redshift_schema.sql      # Star schema DDL
├── docs/
│   └── screenshots/             # Portfolio evidence screenshots
├── .env.example                 # Environment variable template
└── requirements.txt             # Python dependencies
```

---

## Medallion Architecture

### Bronze Layer — Raw zone
- Format: JSON (newline-delimited)
- Partitioning: `year=YYYY/month=MM/day=DD/`
- Content: Exactly as received — no transformation, no filtering
- Purpose: Replay source, audit trail, disaster recovery

### Silver Layer — Cleaned zone
- Format: Apache Parquet (Snappy compressed)
- Partitioning: `partition_year/partition_month/partition_bank/`
- Content: Deduplicated, enriched with bank and merchant names, validated, typed
- New columns: `amount_bucket`, `data_quality_flag`, `fraud_score`, `is_high_value`
- Purpose: Primary analytical layer for ad-hoc Athena queries

### Gold Layer — Business metrics zone
- Format: Apache Parquet (Snappy compressed)
- Five metric tables: `daily_gmv`, `bank_performance`, `fraud_summary`, `upi_market_share`, `amount_distribution`
- Purpose: Pre-computed aggregations for dashboards and reporting

---

## Pipeline Phases

### Phase 1 — Foundation and Streaming Ingestion

**What was built:** Complete AWS infrastructure and the real-time transaction simulator.

**Services:** S3, RDS PostgreSQL, SQS, SNS, IAM, AWS Budgets

**Key components:**
- S3 bucket with 6 folders (bronze, silver, gold, fraud-alerts, dead-letters, scripts)
- RDS PostgreSQL with dim_bank (30 banks), dim_merchant (30 categories), audit_log, pipeline_status tables
- SQS main queue with dead letter queue (messages retry 3 times before DLQ)
- SNS topics for fraud alerts and pipeline ops notifications
- Python simulator generating 50 realistic UPI transactions per run with 1 fraud injection

**Data flow:** `Simulator → SQS → (waiting for Phase 2)`

---

### Phase 2 — Lambda Processor and Fraud Detection

**What was built:** Serverless event-driven processing pipeline triggered automatically by SQS.

**Services:** AWS Lambda, SQS trigger, S3, SNS

**Key components:**
- Lambda Processor validates each transaction (schema, amount range, bank code)
- Fraud velocity check — flags senders with 5+ transactions in 5 minutes
- Clean events routed to S3 Bronze, fraud events to S3 fraud-alerts
- Batch fraud summary email via SNS — one email per batch, not one per event
- Dead letter queue monitoring — silent failures become loud alerts

**Data flow:** `Simulator → SQS → Lambda → S3 Bronze + S3 fraud-alerts + SNS email`

**Result:** 130+ files in S3 Bronze, fraud emails arriving in Gmail, fully automated

---

### Phase 3 — Data Catalog and Ad-hoc Querying

**What was built:** Schema governance layer and serverless SQL querying on the data lake.

**Services:** AWS Glue Crawler, AWS Glue Data Catalog, Amazon Athena

**Key components:**
- Bulk loaded 284,807 real credit card transactions into S3 Bronze (29 chunks, partitioned by month)
- Glue Crawler auto-detected schema and registered bronze table in Glue Catalog
- Three Glue databases: payflow_bronze, payflow_silver, payflow_gold
- Five Athena business queries on 284K records:
  - Fraud rate by bank
  - Top merchant categories by GMV
  - UPI app market share
  - Transaction volume by city
  - Monthly transaction trend

**Key insight:** PhonePe leads with 47,690 transactions — matching real NPCI market share data

---

### Phase 4 — Glue ETL Transformation Engine

**What was built:** Serverless PySpark transformation pipeline — Bronze to Silver to Gold.

**Services:** AWS Glue ETL, PySpark, S3

**Glue ETL Job 1 — Bronze to Silver:**
- Reads all Bronze JSON files
- Deduplicates on transaction_id
- Casts types (timestamp, boolean, double)
- Adds derived columns: amount_bucket, data_quality_flag, fraud_score, is_high_value
- Validates data quality (drops nulls, invalid amounts, unknown banks)
- Writes Parquet to Silver partitioned by year, month, and bank code
- Result: 1,210 Parquet files

**Glue ETL Job 2 — Silver to Gold:**
- Reads Silver Parquet
- Computes 5 aggregations: daily GMV, bank performance, fraud summary, UPI market share, amount distribution
- Writes Snappy-compressed Parquet to Gold metric folders
- Result: 5 metric tables ready for Athena queries

---

## AWS Services Used

| Service | Role | Free Tier |
|---|---|---|
| Amazon S3 | Data lake — Bronze/Silver/Gold | 5GB forever |
| Amazon RDS | Reference data + audit logs | 750hrs/month 12 months |
| Amazon SQS | Message buffer + DLQ | 1M requests forever |
| AWS Lambda | Fraud detection + routing | 1M invocations forever |
| AWS Glue Crawler | Schema auto-detection | 1M objects free |
| AWS Glue ETL | PySpark transformation | 10 DPU-hours/month |
| AWS Glue Catalog | Schema registry | 1M objects free |
| Amazon Athena | Serverless SQL | 1TB queries/month |
| Amazon SNS | Fraud + ops notifications | 1M publishes forever |
| AWS IAM | Security + access control | Always free |
| Amazon CloudWatch | Monitoring + logs | Basic free forever |
| AWS Budgets | Billing protection | 2 budgets free |

**Total monthly cost: $0**

---

## Key Technical Concepts Demonstrated

**Streaming Architecture**
- Event-driven processing using SQS + Lambda
- At-least-once delivery with idempotency handling
- Dead letter queue for silent failure prevention
- Batch fraud alerting vs individual event alerting

**Medallion Architecture**
- Bronze as immutable replay source
- Silver as deduplicated, enriched analytical layer
- Gold as pre-computed business metrics
- Partition pruning for query cost optimisation

**Data Quality**
- Schema validation at ingestion (Lambda)
- Deduplication at transformation (Glue ETL)
- Type enforcement and range validation
- Fraud scoring derived from statistical patterns

**Schema Governance**
- Unified Glue Data Catalog across all three layers
- Automatic schema detection via Glue Crawlers
- Single source of truth for all query engines
- Partition key management (avoiding duplicate columns)

**Security**
- IAM role-based authentication throughout
- Zero hardcoded credentials in any script
- Least privilege principle per service
- Separate IAM roles for Lambda, Glue, and Redshift

---

## Athena Query Results
```sql
-- Total platform statistics
SELECT COUNT(*) as total_records,          -- 284,942
       SUM(CASE WHEN is_fraud = true 
           THEN 1 ELSE 0 END) as fraud,    -- 492
       ROUND(SUM(amount), 2) as total_gmv  -- Rs 2.56 crore
FROM bronze
```
```sql
-- UPI app market share (Gold layer)
SELECT upi_app, total_transactions, 
       total_value_inr, fraud_count
FROM metric_upi_market_share
ORDER BY total_transactions DESC
-- PhonePe: 47,690 | GPay: 47,657 | BHIM: 47,601
```

---

## Fraud Detection Logic

Lambda Processor applies two fraud detection rules:

**Rule 1 — Velocity fraud:** Same sender UPI ID appearing 5 or more times within a 5-minute window is flagged as velocity fraud. This matches real-world card testing attack patterns.

**Rule 2 — Statistical fraud:** Transactions flagged by the Kaggle credit card fraud model (Class = 1) are routed to fraud-alerts with a fraud score derived from the V1 PCA component.

**Batch alerting:** Instead of one email per fraud event, Lambda collects all fraud events in a batch and sends one summary email containing transaction IDs, amounts, banks, categories, and total fraud value at risk.

---

## What I Would Do Differently in Production

- Add Terraform for infrastructure as code instead of manual console setup
- Use Kafka or Kinesis for higher-throughput streaming at millions of events per second
- Add Great Expectations for data quality validation at each Medallion layer
- Configure VPC peering so Lambda can reach RDS without public internet access
- Add Schema Registry to enforce Avro schemas on the producer side
- Use Redshift for warehouse querying instead of Athena for complex multi-table joins
- Add dbt models on top of the Gold layer for analytics engineering
- Implement EventBridge rules to fully automate the pipeline end-to-end
- Add CloudWatch dashboards and alarms for operational observability

---

## Screenshots

| Component | Screenshot |
|---|---|
| S3 Data Lake Structure | docs/screenshots/AWS S3 bucket.png |
| Lambda Function + SQS Trigger | docs/screenshots/lambda function.png |
| SQS Queues | docs/screenshots/AWS SQS.png |
| Glue Databases | docs/screenshots/GLUE databases.png |
| Glue Tables (7 tables) | docs/screenshots/GLUE tables.png |
| Glue ETL Jobs Succeeded | docs/screenshots/ETL jobs.png |
| ETL Job Run Status | docs/screenshots/ETL JOB status.png |
| Athena Query Results | docs/screenshots/athena query.png |
| S3 Bronze Day 20 Files | docs/screenshots/S3 bronze day 20.png |
| Fraud Batch Email | docs/screenshots/aws SNS mail.png |

---

## How to Run

### Prerequisites
- AWS account with free tier
- Python 3.10
- AWS CLI configured
- pgAdmin 4

### Setup
```bash
# Clone the repository
git clone https://github.com/siddheshsalve77/payflow-analytics.git
cd payflow-analytics

# Create virtual environment
python -m venv venv310
.\venv310\Scripts\Activate.ps1  # Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env
# Fill in your AWS credentials and resource ARNs in .env
```

### Run the simulator
```bash
# Generate 50 live UPI transactions → SQS → Lambda → S3 Bronze
python tests/simulate.py
```

### Bulk load historical data
```bash
# Load 284,807 records from Kaggle dataset into S3 Bronze
python src/simulator/bulk_upload.py
```

### Run Glue ETL manually
```bash
# Trigger via AWS Console:
# Glue → ETL jobs → payflow-bronze-to-silver → Run
# Glue → ETL jobs → payflow-silver-to-gold → Run
```

---

## Author

**Siddhesh Salve**
- GitHub: [@siddheshsalve77](https://github.com/siddheshsalve77)
- LinkedIn: [siddhesh-salve](https://linkedin.com/in/siddhesh-salve)

---

