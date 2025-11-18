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

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.types import *


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("test_s3_iceberg")\
        .getOrCreate()

    print("darionic's example: test_s3_iceberg")

    print("available catalogs")
    spark.sql("show catalogs").show()
    print("available databases")
    spark.sql("show databases").show()
    
    # create dataframe and store it in the catalog
    data = [("Robert", "Baratheon", "Baratheon", "Storms End", 48, None, None, None),
        ("Eddard", "Stark", "Stark", "Winterfell", 46, None, None, None),
        ("Jamie", "Lannister", "Lannister", "Casterly Rock", 29, None, None, None)
    ]

    schema = StructType([
        StructField("firstname_2", StringType(), True),
        StructField("lastname_2", StringType(), True),
        StructField("house_2", StringType(), True),
        StructField("location_2", StringType(), True),
        StructField("age_2", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("ts", TimestampType(), True),
        StructField("tsntz", TimestampNTZType(), True),
    ])

    sample_dataframe = spark.createDataFrame(data=data, schema=schema)

    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.darionic_test_database")
    sample_dataframe.write.format("iceberg").mode(saveMode="overwrite").saveAsTable("iceberg_catalog.darionic_test_database.darionic_iceberg_tab")

    # read table from catalog
    spark.sql("select * from iceberg_catalog.darionic_test_database.darionic_iceberg_tab").show()

    spark.stop()
