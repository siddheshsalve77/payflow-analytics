import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BUCKET = "payflow-analytics"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/"

logger.info("Reading Silver layer...")
df = spark.read.parquet(SILVER_PATH)
total = df.count()
logger.info(f"Silver records: {total}")

logger.info("Aggregation 1 - Daily GMV by merchant category...")
daily_gmv = df.groupBy('merchant_category_name', 'partition_year', 'partition_month') \
    .agg(
        F.count('transaction_id').alias('total_transactions'),
        F.round(F.sum('amount'), 2).alias('total_gmv_inr'),
        F.round(F.avg('amount'), 2).alias('avg_transaction_amount'),
        F.sum(F.when(F.col('is_fraud') == True, 1).otherwise(0)).alias('fraud_count'),
        F.round(F.sum(F.when(F.col('is_fraud') == True, F.col('amount')).otherwise(0)), 2).alias('fraud_amount_inr')
    ) \
    .withColumn('metric_type', F.lit('daily_gmv')) \
    .withColumn('created_at', F.current_timestamp())

daily_gmv.write.mode('overwrite').parquet(f"{GOLD_PATH}metric=daily_gmv/")
logger.info(f"Daily GMV written: {daily_gmv.count()} rows")

logger.info("Aggregation 2 - Bank performance metrics...")
bank_metrics = df.groupBy('sender_bank_code', 'partition_year', 'partition_month') \
    .agg(
        F.count('transaction_id').alias('total_transactions'),
        F.round(F.sum('amount'), 2).alias('total_volume_inr'),
        F.round(F.avg('amount'), 2).alias('avg_amount'),
        F.sum(F.when(F.col('status') == 'failed', 1).otherwise(0)).alias('failed_count'),
        F.round(
            F.sum(F.when(F.col('status') == 'failed', 1).otherwise(0)) * 100.0 / F.count('transaction_id'),
            3
        ).alias('failure_rate_pct'),
        F.sum(F.when(F.col('is_fraud') == True, 1).otherwise(0)).alias('fraud_count'),
        F.round(
            F.sum(F.when(F.col('is_fraud') == True, 1).otherwise(0)) * 100.0 / F.count('transaction_id'),
            3
        ).alias('fraud_rate_pct')
    ) \
    .withColumn('metric_type', F.lit('bank_performance')) \
    .withColumn('created_at', F.current_timestamp())

bank_metrics.write.mode('overwrite').parquet(f"{GOLD_PATH}metric=bank_performance/")
logger.info(f"Bank metrics written: {bank_metrics.count()} rows")

logger.info("Aggregation 3 - Fraud summary by city...")
fraud_summary = df.filter(F.col('is_fraud') == True) \
    .groupBy('city', 'merchant_category_name', 'upi_app') \
    .agg(
        F.count('transaction_id').alias('fraud_count'),
        F.round(F.sum('amount'), 2).alias('total_fraud_amount_inr'),
        F.round(F.avg('amount'), 2).alias('avg_fraud_amount'),
        F.round(F.avg('fraud_score'), 4).alias('avg_fraud_score')
    ) \
    .withColumn('metric_type', F.lit('fraud_summary')) \
    .withColumn('created_at', F.current_timestamp())

fraud_summary.write.mode('overwrite').parquet(f"{GOLD_PATH}metric=fraud_summary/")
logger.info(f"Fraud summary written: {fraud_summary.count()} rows")

logger.info("Aggregation 4 - UPI app market share...")
upi_share = df.groupBy('upi_app', 'partition_year', 'partition_month') \
    .agg(
        F.count('transaction_id').alias('total_transactions'),
        F.round(F.sum('amount'), 2).alias('total_value_inr'),
        F.sum(F.when(F.col('is_fraud') == True, 1).otherwise(0)).alias('fraud_count'),
        F.round(F.avg('fraud_score'), 4).alias('avg_fraud_score'),
        F.countDistinct('sender_upi_id').alias('unique_senders')
    ) \
    .withColumn('metric_type', F.lit('upi_market_share')) \
    .withColumn('created_at', F.current_timestamp())

upi_share.write.mode('overwrite').parquet(f"{GOLD_PATH}metric=upi_market_share/")
logger.info(f"UPI market share written: {upi_share.count()} rows")

logger.info("Aggregation 5 - Amount bucket distribution...")
amount_dist = df.groupBy('amount_bucket', 'merchant_category_name', 'partition_month') \
    .agg(
        F.count('transaction_id').alias('transaction_count'),
        F.round(F.sum('amount'), 2).alias('total_amount_inr'),
        F.round(F.avg('amount'), 2).alias('avg_amount'),
        F.sum(F.when(F.col('is_fraud') == True, 1).otherwise(0)).alias('fraud_count'),
        F.round(F.avg('fraud_score'), 4).alias('avg_fraud_score')
    ) \
    .withColumn('metric_type', F.lit('amount_distribution')) \
    .withColumn('created_at', F.current_timestamp())

amount_dist.write.mode('overwrite').parquet(f"{GOLD_PATH}metric=amount_distribution/")
logger.info(f"Amount distribution written: {amount_dist.count()} rows")

logger.info("All Gold aggregations complete")
logger.info(f"Gold metrics written to {GOLD_PATH}")

job.commit()

