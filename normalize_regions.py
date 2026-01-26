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
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("normalize_regions")\
        .getOrCreate()

    print("orange's demo: normalize_regions")

    # Defining the schema based on your CSV structure
    schema = StructType([
        StructField("cell_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("province", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("technology", StringType(), True),
        StructField("capacity_erlang", IntegerType(), True)
    ])

    # read raw source
    source_df = spark.read.csv("s3a://raw/cell_towers.csv", header=True, schema=schema)
    source_df.show()

    # add incremental row id
    windowSpec = Window.partitionBy("cell_id").orderBy("region")
    source_with_id = source_df.withColumn("id", row_number().over(windowSpec))

    # create regions df
    normalized_regions = source_with_id.select("id", "region", "province", "latitude", "longitude")
    print("normalized regions")
    normalized_regions.show()

    # create towers df
    towers = source_with_id.select("cell_id", "technology", "capacity_erlang", source_with_id.id.alias("region_id"))
    print("normalized towers")
    towers.show()

    # save results on icerberg catalog (data lakehouse)

    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.silver_layer")
    normalized_regions.write.format("iceberg").mode(saveMode="overwrite").saveAsTable("iceberg_catalog.silver_layer.regions")
    towers.write.format("iceberg").mode(saveMode="overwrite").saveAsTable("iceberg_catalog.silver_layer.towers")

    # read table from catalog
    spark.sql("select * from iceberg_catalog.silver_layer.towers").show()

    spark.stop()
