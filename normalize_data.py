#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("normalize_data")\
        .getOrCreate()

    print("orange's demo: normalize_data")

    # Defining the schema based on your CSV structure
    schema = StructType([
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

    # read raw source (multiple files)
    source_df = spark.read.load("s3a://raw/", format="csv", pathGlobFilter="data_cdr_mali_*.csv", header=True, schema=schema)

    source_df.show()

    source_df \
        .dropDuplicates(["session_id"]) \
        .orderBy("timestamp")
    
    # 1. overwrite NULL currency cells as XOF
    # 2. normalize the amount to USD
    # 3. replace USD value to currency column
    normalized_currency_df = source_df.withColumn(
        "currency", 
        F.when((F.col("currency") == "") | (F.col("currency").isNull()), "XOF")
        .otherwise(F.col("currency"))
    ).withColumn(
        "charging_amount",
        F.when(F.col("currency") == "EUR", F.round(F.col("charging_amount") / 0.92, 2))
        .when(F.col("currency") == "XOF", F.round(F.col("charging_amount") / 610.0, 2))
        .otherwise(F.col("charging_amount"))
    ).withColumn(
        "currency", 
        F.lit("USD")
    )
    
    # Safety Check: Cast back to Decimal(7,2) to match your schema
    normalized_currency_df = normalized_currency_df.withColumn(
        "charging_amount", 
        F.col("charging_amount").cast("decimal(7,2)")
    )
    
    normalized_currency_df.createOrReplaceTempView("incoming_batch")

    # Create Target Table if it doesn't exist
    # This is where we define the Daily Partitioning strategy
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.orange_cdr_silver.normalized_data (
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
        PARTITIONED BY (cell_id)
    """)

    # upsert destination table with incoming batch
    # add a row ONLY if the conditions are met: different session_id and different cell_id

    spark.sql("""
        MERGE INTO iceberg_catalog.orange_cdr_silver.normalized_data AS target
        USING incoming_batch AS source
        ON target.sms_id = source.sms_id 
        AND target.cell_id = source.cell_id
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    # read table from catalog to test
    spark.sql("select * from iceberg_catalog.orange_cdr_silver.normalized_data limit 10").show()

    spark.stop()
