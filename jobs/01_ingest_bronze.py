#!/usr/bin/env python3
"""
BRONZE: land raw events untouched (plus lineage metadata) as Parquet on HDFS.

  Kafka topic  ─┐
                ├─> Structured Streaming (trigger=availableNow, checkpointed) -> bronze.clickstream_raw
  landing files ┘

Properties: append-only, no parsing / no filtering, incremental (checkpoint tracks offsets/files),
re-runnable (nothing new -> no-op), partitioned by ingest_date.
"""
import argparse
from pyspark.sql import functions as F
from common import BRONZE_PATH, CHECKPOINT_ROOT, LAKE_ROOT, get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="clickstream.raw")
    ap.add_argument("--landing", default=f"{LAKE_ROOT}/landing")
    ap.add_argument("--max-offsets-per-trigger", type=int, default=5_000_000)
    ap.add_argument("--max-files-per-trigger", type=int, default=200)
    args = ap.parse_args()

    spark = get_spark("01_ingest_bronze")

    if args.source == "kafka":
        stream = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", args.bootstrap)
                  .option("subscribe", args.topic)
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
                  .option("failOnDataLoss", "false")
                  .load()
                  .select(F.col("value").cast("string").alias("raw_payload"),
                          F.lit("kafka").alias("source"),
                          F.concat_ws("/", F.col("topic"), F.col("partition").cast("string")).alias("source_ref"),
                          F.col("partition").cast("int").alias("kafka_partition"),
                          F.col("offset").cast("long").alias("kafka_offset"),
                          F.col("timestamp").alias("source_ts")))
    else:
        stream = (spark.readStream.format("text")
                  .option("maxFilesPerTrigger", args.max_files_per_trigger)
                  .load(args.landing)
                  .select(F.col("value").alias("raw_payload"),
                          F.lit("files").alias("source"),
                          F.input_file_name().alias("source_ref"),
                          F.lit(None).cast("int").alias("kafka_partition"),
                          F.lit(None).cast("long").alias("kafka_offset"),
                          F.lit(None).cast("timestamp").alias("source_ts")))

    stream = (stream.withColumn("ingest_ts", F.current_timestamp())
                    .withColumn("ingest_date", F.to_date("ingest_ts")))

    q = (stream.writeStream.format("parquet")
         .option("path", BRONZE_PATH)
         .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_clickstream_raw_{args.source}")
         .partitionBy("ingest_date")
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS bronze.clickstream_raw (
            raw_payload STRING, source STRING, source_ref STRING,
            kafka_partition INT, kafka_offset BIGINT, source_ts TIMESTAMP, ingest_ts TIMESTAMP)
        PARTITIONED BY (ingest_date DATE)
        STORED AS PARQUET
        LOCATION '{BRONZE_PATH}'""")
    spark.sql("MSCK REPAIR TABLE bronze.clickstream_raw")
    print("bronze rows:", spark.table("bronze.clickstream_raw").count())


if __name__ == "__main__":
    main()
