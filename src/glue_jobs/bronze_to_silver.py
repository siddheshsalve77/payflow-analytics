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
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/"

logger.info("Reading Bronze layer from S3...")

df = spark.read.option("multiline", "false").json(BRONZE_PATH)

logger.info(f"Bronze records read: {df.count()}")

logger.info("Step 1 - Deduplication...")
df = df.dropDuplicates(['transaction_id'])

logger.info("Step 2 - Schema validation and cleaning...")
df = df.filter(
    F.col('transaction_id').isNotNull() &
    F.col('amount').isNotNull() &
    F.col('amount').cast(DoubleType()).isNotNull() &
    (F.col('amount') > 0) &
    (F.col('amount') <= 200000) &
    F.col('sender_bank_code').isNotNull() &
    F.col('timestamp').isNotNull()
)

logger.info("Step 3 - Type casting and enrichment...")
df = df.withColumn('amount', F.col('amount').cast(DoubleType())) \
       .withColumn('is_fraud', F.col('is_fraud').cast(BooleanType())) \
       .withColumn('fraud_score', F.col('fraud_score').cast(DoubleType())) \
       .withColumn('processed_timestamp', F.current_timestamp()) \
       .withColumn('transaction_date', F.to_date(F.col('timestamp'))) \
       .withColumn('transaction_hour',
           F.when(F.col('timestamp').contains('T'),
               F.hour(F.to_timestamp(F.col('timestamp'))))
           .otherwise(F.lit(12))) \
       .withColumn('is_high_value', F.when(F.col('amount') > 10000, True).otherwise(False)) \
       .withColumn('amount_bucket',
           F.when(F.col('amount') <= 100, 'micro')
           .when(F.col('amount') <= 500, 'small')
           .when(F.col('amount') <= 2000, 'medium')
           .when(F.col('amount') <= 10000, 'large')
           .otherwise('high_value')) \
       .withColumn('data_quality_flag',
           F.when(F.col('fraud_score') > 5.0, 'high_risk')
           .when(F.col('fraud_score') > 2.0, 'medium_risk')
           .otherwise('low_risk'))

logger.info("Step 4 - Adding partition columns...")
df = df.withColumn('partition_year', F.year(F.col('transaction_date'))) \
       .withColumn('partition_month', F.month(F.col('transaction_date'))) \
       .withColumn('partition_bank', F.col('sender_bank_code'))

silver_count = df.count()
fraud_count = df.filter(F.col('is_fraud') == True).count()
logger.info(f"Silver records after cleaning: {silver_count}")
logger.info(f"Fraud records: {fraud_count}")

logger.info("Step 5 - Writing Silver layer as Parquet...")
df.write \
  .mode('append') \
  .partitionBy('partition_year', 'partition_month', 'partition_bank') \
  .parquet(SILVER_PATH)

logger.info(f"Silver layer written successfully to {SILVER_PATH}")
logger.info(f"Total records: {silver_count}")
logger.info(f"Fraud records: {fraud_count}")

job.commit()
