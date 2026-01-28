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
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("normalize_sms")\
        .getOrCreate()

    print("orange's demo: normalize_sms")

    # Defining the schema based on your CSV structure
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("sms_id", StringType(), True),
        StructField("sender_msisdn", StringType(), True),
        StructField("receiver_msisdn", StringType(), True),
        StructField("sms_type", StringType(), True),
        StructField("message_length", IntegerType(), True),
        StructField("cell_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("delivery_status", StringType(), True),
        StructField("charging_amount", DecimalType(7,2), True)
    ])

    # read raw source
    source_df = spark.read.csv("s3a://raw/sms_cdr_mali.csv", header=True, schema=schema)
    source_df.show()

    source_df \
        .dropDuplicates(["sms_id"]) \
        .orderBy("timestamp") \
        .createOrReplaceTempView("incoming_batch")

    # Create Target Table if it doesn't exist
    # This is where we define the Daily Partitioning strategy
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.silver_layer.sms_data (
            timestamp TIMESTAMP,
            sms_id STRING,
            sender_msisdn STRING,
            receiver_msisdn STRING,
            sms_type STRING,
            message_length INT,
            cell_id STRING,
            region STRING,
            delivery_status STRING,
            charging_amount DECIMAL(7,2)
        )
        USING iceberg
        PARTITIONED BY (cell_id)
    """)

    # load towers table
    # towers_df = spark.table("iceberg_catalog.silver_layer.towers")

    spark.sql("""
        MERGE INTO iceberg_catalog.silver_layer.sms_data AS target
        USING incoming_batch AS source
        ON target.sms_id = source.sms_id 
        AND target.cell_id = source.cell_id
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    # save results on icerberg catalog (data lakehouse)

    # normalized_regions.write.format("iceberg").mode(saveMode="overwrite").saveAsTable("iceberg_catalog.silver_layer.regions")
    # towers.write.format("iceberg").mode(saveMode="overwrite").saveAsTable("iceberg_catalog.silver_layer.towers")

    # read table from catalog
    spark.sql("select * from iceberg_catalog.silver_layer.sms_data limit 10").show()

    spark.stop()
