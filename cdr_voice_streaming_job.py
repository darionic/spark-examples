"""
CDR Kafka → S3 Iceberg Streaming Job
=====================================
Reads Call Detail Records (CDR) from a Kafka topic, applies cleaning and
currency normalisation, then sinks the result into an Apache Iceberg table
stored on Amazon S3.

Transformations applied
-----------------------
1. Drop the "region" column.
2. Cast "charging_amount" to DECIMAL(7, 2).
3. Normalise "charging_amount" to USD using the rates:
       EUR → USD : × 1.16
       USD → USD : × 1.00
       XOF → USD : × 0.002
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Configuration — override via environment variables or a config file in prod
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "48.32.91.42:9092"          # e.g. "broker1:9092,broker2:9092"
KAFKA_TOPIC             = "voice_cdr_mali"
KAFKA_STARTING_OFFSETS  = "latest"                  # "earliest" for backfill

S3_WAREHOUSE_PATH       = "s3://testlake/warehouse/"
ICEBERG_CATALOG         = "iceberg_catalog"                    # or "hadoop" / "rest"
ICEBERG_DATABASE        = "orange_cdr_silver"
ICEBERG_TABLE           = "voice_cdr_normalized"
ICEBERG_TABLE_FQN       = f"{ICEBERG_DATABASE}.{ICEBERG_TABLE}"

CHECKPOINT_LOCATION     = f"{S3_WAREHOUSE_PATH}/_checkpoints/{ICEBERG_TABLE}"

TRIGGER_INTERVAL        = "10 seconds"              # micro-batch cadence
OUTPUT_MODE             = "append"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("CDRStreamingJob")

# ---------------------------------------------------------------------------
# Source schema (one JSON object per Kafka message value)
# ---------------------------------------------------------------------------
SOURCE_SCHEMA = StructType([
    StructField("timestamp",          TimestampType(), nullable=True),
    StructField("call_id",            StringType(),    nullable=False),
    StructField("caller_msisdn",      StringType(),    nullable=True),
    StructField("callee_msisdn",      StringType(),    nullable=True),
    StructField("call_type",          StringType(),    nullable=True),
    StructField("duration_seconds",   IntegerType(),   nullable=True),
    StructField("cell_id",            StringType(),    nullable=True),
    StructField("region",             StringType(),    nullable=True),   # will be dropped
    StructField("termination_reason", StringType(),    nullable=True),
    StructField("charging_amount",    DecimalType(12, 6), nullable=True), # raw precision
    StructField("currency",           StringType(),    nullable=True),
])

# ---------------------------------------------------------------------------
# Currency → USD conversion map
# ---------------------------------------------------------------------------
CURRENCY_TO_USD = {
    "EUR": 1.16,
    "USD": 1.00,
    "XOF": 0.002,
}


def create_iceberg_table_if_not_exists(spark: SparkSession) -> None:
    """
    Idempotently create the destination Iceberg table.
    The table is partitioned by call_type and by the date part of the
    event timestamp to enable efficient time-range queries.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DATABASE}")

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_TABLE_FQN} (
            timestamp           TIMESTAMP,
            call_id             STRING        NOT NULL,
            caller_msisdn       STRING,
            callee_msisdn       STRING,
            call_type           STRING,
            duration_seconds    INT,
            cell_id             STRING,
            termination_reason  STRING,
            charging_amount_usd DECIMAL(7, 2),
            currency            STRING
        )
        USING iceberg
        PARTITIONED BY (call_type, days(timestamp))
        LOCATION '{S3_WAREHOUSE_PATH}/{ICEBERG_DATABASE}/{ICEBERG_TABLE}'
        TBLPROPERTIES (
            'write.format.default'          = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max'       = '10'
        )
    """
    logger.info("Ensuring Iceberg table exists: %s.%s", ICEBERG_CATALOG, ICEBERG_TABLE_FQN)
    spark.sql(ddl)
    logger.info("Table ready.")


def build_currency_udf():
    """
    Return a Spark UDF that multiplies a decimal amount by the USD rate for
    the given currency code, returning a Python float that will later be
    cast to DECIMAL(7, 2).
    """
    rates = CURRENCY_TO_USD          # captured in closure — no broadcast needed for 3 entries

    def normalise(amount, currency: str):
        if amount is None or currency is None:
            return None
        rate = rates.get(currency.upper())
        if rate is None:
            # Unknown currency: keep the original amount unchanged and log later
            return float(amount)
        return float(amount) * rate

    return F.udf(normalise, DecimalType(15, 6))


def transform(raw_df):
    """
    Apply all cleaning and enrichment rules to the parsed DataFrame.

    Steps
    -----
    1. Drop 'region'.
    2. Normalise charging_amount to USD.
    3. Cast normalised amount to DECIMAL(7, 2) and rename column.
    4. Drop intermediate columns.
    5. Deduplicate on call_id within the micro-batch (best-effort; use
       watermark + stateful ops for cross-batch exactness if required).
    """
    normalise_udf = build_currency_udf()

    return (
        raw_df
        # ── 1. Drop region ────────────────────────────────────────────────
        .drop("region")

        # ── 2. Compute raw USD amount (full precision) ────────────────────
        .withColumn(
            "_usd_raw",
            normalise_udf(F.col("charging_amount"), F.col("currency"))
        )

        # ── 3. Enforce DECIMAL(7, 2) and rename ───────────────────────────
        .withColumn(
            "charging_amount_usd",
            F.col("_usd_raw").cast(DecimalType(7, 2))
        )

        # ── 4. Drop source monetary columns and temp column ───────────────
        .drop("charging_amount", "_usd_raw")

        # ── 5. Light deduplication within the batch ───────────────────────
        .dropDuplicates(["call_id"])

        # ── 6. Enforce non-null call_id (primary key) ─────────────────────
        .filter(F.col("call_id").isNotNull())

        # ── 7. Canonical column order ─────────────────────────────────────
        .select(
            "timestamp",
            "call_id",
            "caller_msisdn",
            "callee_msisdn",
            "call_type",
            "duration_seconds",
            "cell_id",
            "termination_reason",
            "charging_amount_usd",
            "currency",
        )
    )


def foreach_batch_sink(batch_df, batch_id: int) -> None:
    """
    foreachBatch sink — enables Iceberg MERGE / upsert semantics and gives
    full control over write options per micro-batch.
    """
    count = batch_df.count()
    if count == 0:
        logger.info("Batch %d — empty, skipping write.", batch_id)
        return

    logger.info("Batch %d — writing %d records to Iceberg.", batch_id, count)

    (
        batch_df.writeTo(f"{ICEBERG_CATALOG}.{ICEBERG_TABLE_FQN}")
        .option("fanout-enabled", "true")   # allow out-of-order partition writes
        .append()
    )
    logger.info("Batch %d — committed.", batch_id)


def run() -> None:
    spark = SparkSession\
        .builder\
        .appName("CDR_voice_Kafka_to_Iceberg_Streaming")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Create destination table ──────────────────────────────────────────
    create_iceberg_table_if_not_exists(spark)

    # ── 1. Read from Kafka ────────────────────────────────────────────────
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Deserialise JSON payload ───────────────────────────────────────
    parsed_df = (
        kafka_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                SOURCE_SCHEMA
            ).alias("data"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*")                   # flatten nested struct
        # Watermark for stateful late-data handling (30-minute tolerance)
        .withWatermark("timestamp", "30 minutes")
    )

    # ── 3. Transform ──────────────────────────────────────────────────────
    clean_df = transform(parsed_df)

    # ── 4. Sink ───────────────────────────────────────────────────────────
    query = (
        clean_df.writeStream
        .format("iceberg")
        .outputMode(OUTPUT_MODE)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("path", f"{ICEBERG_CATALOG}.{ICEBERG_TABLE_FQN}")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .foreachBatch(foreach_batch_sink)
        .start()
    )

    logger.info(
        "Streaming query started — id=%s, runId=%s",
        query.id, query.runId,
    )
    query.awaitTermination()


if __name__ == "__main__":
    run()
