from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr
import time

def demonstrate_dynamic_allocation():
    spark = SparkSession\
        .builder \
        .appName("K8s-Dynamic-Allocation-Demo") \
        .getOrCreate()
    
    print(f"Starting with {spark.sparkContext.defaultParallelism} parallelism")

    # Phase 1: Light load (should scale down)
    print("=== Phase 1: Light workload ===")
    df1 = spark.range(1000).selectExpr("id", "id * 2 as doubled")
    df1.count()
    time.sleep(90)  # Wait to see scale-down
    
    # Phase 2: Heavy load (should scale up)
    print("=== Phase 2: Heavy workload ===")
    df2 = spark.range(10000000).repartition(200)
    df2 = df2.selectExpr("id", "id * id as squared", "id % 100 as bucket")
    df2.groupBy("bucket").count().collect()

    # Phase 3: Back to light load
    print("=== Phase 3: Back to light workload ===")
    time.sleep(90)  # Observe scale-down again
    
    spark.stop()

if __name__ == "__main__":
    demonstrate_dynamic_allocation()