from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("raw ingestion")\
        .getOrCreate()

    print("orange's demo: s3 to bronze")

    # Defining the schema based on your CSV structure
    schema_data = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("session_id", StringType(), True),
        StructField("msisdn", StringType(), True),
        StructField("apn", StringType(), True),
        StructField("session_duration_seconds", IntegerType(), True),
        StructField("bytes_uploaded", IntegerType(), True),
        StructField("bytes_downloaded", IntegerType(), True),
        StructField("cell_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("session_end_reason", StringType(), True),
        StructField("charging_amount", DecimalType(7,2), True),
        StructField("currency", StringType(), True),
    ])

    schema_sms = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("sms_id", StringType(), True),
        StructField("sender_msisdn", StringType(), True),
        StructField("receiver_msisdn", StringType(), True),
        StructField("sms_type", StringType(), True),
        StructField("message_length", IntegerType(), True),
        StructField("cell_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("delivery_status", StringType(), True),
        StructField("charging_amount", DecimalType(7,2), True),
        StructField("currency", StringType(), True),
    ])

    # read raw source (multiple files)
    source_df_data = spark.read.load("/tmp/extra_vol_4w0nd1totl/", format="csv", pathGlobFilter="data_cdr_mali_*.csv", header=True, schema=schema_data)

    source_df_sms = spark.read.load("/tmp/extra_vol_4w0nd1totl/", format="csv", pathGlobFilter="sms_cdr_mali_*.csv", header=True, schema=schema_sms)
    
    source_df_data.createOrReplaceTempView("source_data")
    source_df_sms.createOrReplaceTempView("source_sms")

    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.orange_cdr_bronze")

    # Create Target Tables if they don't exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.orange_cdr_bronze.data_cdr_mali (
            timestamp TIMESTAMP,
            session_id STRING,
            msisdn STRING,
            apn STRING,
            session_duration_seconds INT,
            bytes_uploaded INT,
            bytes_downloaded INT,
            cell_id STRING,
            region STRING,
            session_end_reason STRING,
            charging_amount DECIMAL(7,2),
            currency STRING
        )
        USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.orange_cdr_bronze.sms_cdr_mali (
            timestamp        TIMESTAMP,
            sms_id           STRING,
            sender_msisdn    STRING,
            receiver_msisdn  STRING,
            sms_type         STRING,
            message_length   INT,
            cell_id          STRING,
            region           STRING,
            delivery_status  STRING,
            charging_amount  DECIMAL(7,2),
            currency         STRING
        )
        USING iceberg
    """)

    # upsert destination table with incoming batch
    # add a row ONLY if the conditions are met: different session_id and different cell_id

    # spark.sql("""
    #     MERGE INTO iceberg_catalog.orange_cdr_silver.normalized_data AS target
    #     USING incoming_batch AS source
    #     ON target.sms_id = source.sms_id 
    #     AND target.cell_id = source.cell_id
    #     WHEN NOT MATCHED THEN
    #         INSERT *
    # """)

    # spark.sql("""
    #     MERGE INTO iceberg_catalog.silver_layer.sms_data AS target
    #     USING incoming_batch AS source
    #     ON target.sms_id = source.sms_id
    #     AND target.cell_id = source.cell_id
    #     WHEN NOT MATCHED THEN INSERT *
    # """)

    source_df_data.write.format("iceberg").mode(saveMode="overwrite").saveAsTable("iceberg_catalog.orange_cdr_bronze.data_cdr_mali")
    source_df_sms.write.format("iceberg").mode(saveMode="overwrite").saveAsTable("iceberg_catalog.orange_cdr_bronze.sms_cdr_mali")

    # read table from catalog to test
    spark.sql("select * from iceberg_catalog.orange_cdr_bronze.sms_cdr_mali limit 10").show()

    spark.stop()