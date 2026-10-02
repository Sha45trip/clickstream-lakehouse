#!/usr/bin/env python3
"""
CONTINUOUS BRONZE: Kafka -> bronze.rees46_raw, running all the time (micro-batches every N seconds).

Differences from 01_ingest_bronze.py
  * trigger = processingTime (runs forever) instead of availableNow (drain and stop)
  * writes through foreachBatch + insertInto, so every micro-batch registers its new partitions in the
    Hive metastore immediately (no MSCK REPAIR step needed while the stream is running)
  * one bronze table per source schema: bronze.rees46_raw  (the synthetic data keeps bronze.clickstream_raw)

Delivery guarantee: foreachBatch is AT-LEAST-ONCE. If the job dies after writing a batch but before the
checkpoint is committed, that batch is written again on restart. That is fine for bronze: silver
de-duplicates on event_id. (Phase 3 / Iceberg makes this write transactional.)

Stop it with Ctrl+C (or use --run-seconds for a time-boxed run). On restart it continues from the
checkpoint, i.e. from the last committed Kafka offsets.
"""
import argparse
from pyspark.sql import functions as F
from common import CHECKPOINT_ROOT, LAKE_ROOT, get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--bootstrap", default="kafka:9092", help="Kafka address as seen from the containers")
    ap.add_argument("--topic", default="rees46.events.raw")
    ap.add_argument("--landing", default=f"{LAKE_ROOT}/landing_rees46", help="used only with --source files")
    ap.add_argument("--table", default="bronze.rees46_raw")
    ap.add_argument("--trigger-seconds", type=int, default=10, help="micro-batch interval; 0 = drain and stop")
    ap.add_argument("--max-offsets-per-trigger", type=int, default=500_000,
                    help="cap per micro-batch, protects the cluster when catching up")
    ap.add_argument("--starting-offsets", default="earliest", help="only used on the very first run")
    ap.add_argument("--run-seconds", type=int, default=0, help="stop after N seconds (0 = run until Ctrl+C)")
    args = ap.parse_args()

    db, name = args.table.split(".")
    path = f"{LAKE_ROOT}/bronze/{name}"
    checkpoint = f"{CHECKPOINT_ROOT}/{name}_{args.source}"

    spark = get_spark("10_stream_bronze")

    # Table first, so foreachBatch can insertInto it from the first batch.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {args.table} (
            raw_payload STRING, source STRING, source_ref STRING,
            kafka_partition INT, kafka_offset BIGINT, source_ts TIMESTAMP, ingest_ts TIMESTAMP)
        PARTITIONED BY (ingest_date DATE)
        STORED AS PARQUET
        LOCATION '{path}'""")

    if args.source == "kafka":
        stream = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", args.bootstrap)
                  .option("subscribe", args.topic)
                  .option("startingOffsets", args.starting_offsets)
                  .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
                  .option("failOnDataLoss", "false")
                  .load()
                  .select(F.col("value").cast("string").alias("raw_payload"),
                          F.lit("kafka").alias("source"),
                          F.concat_ws("/", F.col("topic"), F.col("partition").cast("string")).alias("source_ref"),
                          F.col("partition").cast("int").alias("kafka_partition"),
                          F.col("offset").cast("long").alias("kafka_offset"),
                          F.col("timestamp").alias("source_ts")))
    else:  # files: lets you test the job without Kafka (NDJSON files, one event per line)
        stream = (spark.readStream.format("text")
                  .option("maxFilesPerTrigger", 100)
                  .load(args.landing)
                  .select(F.col("value").alias("raw_payload"),
                          F.lit("files").alias("source"),
                          F.input_file_name().alias("source_ref"),
                          F.lit(None).cast("int").alias("kafka_partition"),
                          F.lit(None).cast("long").alias("kafka_offset"),
                          F.lit(None).cast("timestamp").alias("source_ts")))

    stream = (stream.withColumn("ingest_ts", F.current_timestamp())
                    .withColumn("ingest_date", F.to_date("ingest_ts")))

    def write_batch(batch_df, batch_id):
        # persist: count() and write() would otherwise each re-read the batch from Kafka (2x the work).
        batch_df.persist()
        try:
            n = batch_df.count()
            if n == 0:
                return
            # insertInto matches columns BY POSITION: the order above equals the table DDL, partition column last.
            batch_df.write.mode("append").insertInto(args.table)
            print(f"[bronze] batch {batch_id}: {n:,} rows -> {args.table}", flush=True)
        finally:
            batch_df.unpersist()

    writer = (stream.writeStream
              .foreachBatch(write_batch)
              .option("checkpointLocation", checkpoint))
    writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds") if args.trigger_seconds > 0 \
        else writer.trigger(availableNow=True)

    q = writer.start()
    print(f"[bronze] streaming {args.source} -> {args.table} (checkpoint: {checkpoint})", flush=True)

    try:
        if args.run_seconds:
            q.awaitTermination(args.run_seconds)
            q.stop()
        else:
            q.awaitTermination()
    except KeyboardInterrupt:
        print("[bronze] stopping...", flush=True)
        q.stop()

    print("[bronze] rows in table:", f"{spark.table(args.table).count():,}", flush=True)


if __name__ == "__main__":
    main()