# import modules required
import os
import pyspark
from pyspark.sql import SparkSession
import logging
from google.cloud import storage

# set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## ------------------------------------------------------------------------------##

# create a function to initilize the spark session 
def create_spark_session(app_name: str):
    logger.info("✅creating spark session ...")
    return (SparkSession
                    .builder
                    .master("local")
                    .appName(app_name)
                    .getOrCreate())

## ------------------------------------------------------------------------------##

# Function to read from BigQuery
def read_from_bigquery(spark, table_id):
    logger.info(f"🎯Attempting to load data from BigQuery table: {table_id}")
    
    try:
        df = (spark
                  .read
                  .format("bigquery")
                  .option("table", table_id)
                  .load())
        logger.info(f"✅Successfully loaded data from table: {table_id}")
        return df
    
    except Exception as e:
        logger.error(f"❌Failed to load data from BigQuery table {table_id}. Error: {str(e)}")
        raise

## ------------------------------------------------------------------------------##

# Function to write data to GCS bucket-1
def write_to_gcs(df,bucket_path,num_partitions=5):
    logger.info(f"✅Writing data to GCS at {bucket_path} with {num_partitions} partitions.")
    
    try:
        (df
            .repartition(num_partitions)
            .write
            .options(codec="org.apache.hadoop.io.compress.GzipCodec")
            .csv(bucket_path))
        logger.info(f"✅Data successfully written to {bucket_path}")
    
    except Exception as e:
        logger.error(f"❌Error writing data to GCS: {str(e)}")
        raise

## ------------------------------------------------------------------------------##      

# Function to copy blobs from one GCS bucket to another
def copy_blobs(source_bucket_name, dest_bucket_name, file_type=".csv.gz"):
    
    client = storage.Client()
    source_bucket = client.get_bucket(source_bucket_name)
    dest_bucket = client.get_bucket(dest_bucket_name)
    blobs = list(source_bucket.list_blobs())

    if not blobs:
        logger.warning(f"❌No blobs found in source bucket {source_bucket_name}")
        return

    logger.info(f"✅Found {len(blobs)} blobs in source bucket {source_bucket_name}. Initiating copy to {dest_bucket_name}.")

    for blob in blobs:
        if file_type in blob.name:
            try:
                source_bucket.copy_blob(blob, dest_bucket)
                logger.info(f"✅Copied {blob.name} to {dest_bucket_name}")
            except Exception as e:
                logger.error(f"❌Failed to copy {blob.name}. Error: {str(e)}")

## ------------------------------------------------------------------------------##                   

# variable/configuration section
TABLE_ID = "gcp-morning-batch-740.json_example_ds.EmpTravelRecords"
FILTER = "Dept_02"
BUCKET_1 = "bucket-1-1672024"
BUCKET_2 = "bucket-2-1672024"
BUCKET_1_PATH = f"gs://{BUCKET_1}/travel/"
BUCKET_2_PATH = f"gs://{BUCKET_2}/"

## ------------------------------------------------------------------------------##

logger.info(f"✅Pipeline starting with TABLE_ID={TABLE_ID}, FILTER={FILTER}, BUCKET_1={BUCKET_1}, BUCKET_2={BUCKET_2}")

# Step 1: Create Spark session
spark = create_spark_session("case_study_3")

# Step 2: Read data from BigQuery
df = read_from_bigquery(spark, TABLE_ID)

# Step-3: transform - filter the data
df = df.filter(df.Department == FILTER)

# step-4 : transform - remove duplicates
df = df.dropDuplicates()

# Step-5: Write df to GCS Bucket-1 (csv, zip)
write_to_gcs(df, BUCKET_1_PATH)

# Step-6: Copy files from Bucket-1 to Bucket-2
copy_blobs(BUCKET_1, BUCKET_2)

logger.info("✅Pipeline completed successfully.")