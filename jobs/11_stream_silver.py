#!/usr/bin/env python3
"""
CONTINUOUS SILVER: bronze.rees46_raw  ->  silver.rees46_events  (+ silver.rees46_quarantine)

One Spark application, TWO streaming queries reading the same bronze files:

  query 1  VALID    parse -> validate -> map names -> watermark(24h) -> dropDuplicatesWithinWatermark(event_id)
                    -> silver.rees46_events        (ORC, partitioned by event_date)
  query 2  REJECTS  rows that fail validation (+ reason)
                    -> silver.rees46_quarantine    (ORC, partitioned by ingest_date)

Why two queries: the de-duplication operator keeps state and must only see valid rows; rejects must never
enter that state. Spark has no "side output", so each branch is its own query with its own checkpoint.

De-duplication
  * key = event_id (deterministic, created by the replayer). Duplicates come from three places:
    Kafka/replay re-delivery, a re-written bronze batch after a crash, and the source itself.
  * state is kept for the watermark delay (24 h of EVENT time behind the newest event seen), in RocksDB
    (off-heap, spills to disk) so it does not eat the executor heap.
  * GUARANTEE AND ITS LIMIT: a duplicate is removed only if its first copy is still inside the window,
    i.e. the first copy's event_time is within 24 h of the newest event_time seen so far. A replay of events
    older than that passes through as "new" rows. (Verified: first-time events that arrive late are KEPT,
    not dropped.) In production, pair this with a periodic batch job that removes older duplicates.
"""
import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType, LongType, IntegerType
from common import CHECKPOINT_ROOT, LAKE_ROOT, get_spark

VALID_SOURCE_TYPES = ["view", "cart", "remove_from_cart", "purchase"]

EVENT_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("event_id", StringType()), ("event_time", StringType()), ("event_type", StringType()),
    ("product_id", StringType()), ("category_id", StringType()), ("category_code", StringType()),
    ("brand", StringType()), ("price", DoubleType()), ("user_id", StringType()),
    ("user_session", StringType()),
    # from_json puts the raw text of an unparseable record into this column (PERMISSIVE mode);
    # without it, broken JSON silently becomes a row of NULLs and cannot be told apart from missing fields
    ("_corrupt_record", StringType())]])

# bronze file schema (partition column ingest_date comes from the directory name)
BRONZE_SCHEMA = StructType([
    StructField("raw_payload", StringType()), StructField("source", StringType()),
    StructField("source_ref", StringType()), StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()), StructField("source_ts", TimestampType()),
    StructField("ingest_ts", TimestampType())])

SILVER_COLS = ["event_id", "event_ts", "event_type", "product_id", "category_id", "category_code",
               "category_l1", "brand", "price", "user_id", "source_session_id", "ingest_ts", "event_date"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-table", default="bronze.rees46_raw")
    ap.add_argument("--silver-table", default="silver.rees46_events")
    ap.add_argument("--quarantine-table", default="silver.rees46_quarantine")
    ap.add_argument("--watermark", default="24 hours", help="dedup window, in event time")
    ap.add_argument("--min-event-date", default="2015-01-01",
                    help="plausibility floor for event_time (a wrong floor rejects everything: see job 02 lesson)")
    ap.add_argument("--trigger-seconds", type=int, default=30)
    ap.add_argument("--max-files-per-trigger", type=int, default=30)
    ap.add_argument("--state-partitions", type=int, default=4,
                    help="shuffle partitions for the dedup state. FIXED forever once the checkpoint exists.")
    ap.add_argument("--run-seconds", type=int, default=0, help="stop after N seconds (0 = until Ctrl+C)")
    args = ap.parse_args()

    spark = get_spark("11_stream_silver")
    # RocksDB state store: state lives off-heap and on local disk instead of the JVM heap.
    spark.conf.set("spark.sql.streaming.stateStore.providerClass",
                   "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.shuffle.partitions", str(args.state_partitions))

    bronze_path = f"{LAKE_ROOT}/bronze/{args.bronze_table.split('.')[1]}"
    silver_path = f"{LAKE_ROOT}/silver/{args.silver_table.split('.')[1]}"
    quar_path = f"{LAKE_ROOT}/silver/{args.quarantine_table.split('.')[1]}"

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {args.silver_table} (
            event_id STRING, event_ts TIMESTAMP, event_type STRING, product_id STRING, category_id STRING,
            category_code STRING, category_l1 STRING, brand STRING, price DOUBLE, user_id STRING,
            source_session_id STRING, ingest_ts TIMESTAMP)
        PARTITIONED BY (event_date DATE)
        STORED AS ORC LOCATION '{silver_path}'""")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {args.quarantine_table} (
            raw_payload STRING, source_ref STRING, ingest_ts TIMESTAMP, reject_reasons STRING)
        PARTITIONED BY (ingest_date DATE)
        STORED AS ORC LOCATION '{quar_path}'""")

    # ------------------------------------------------------------------ source: new bronze files, as a stream
    bronze = (spark.readStream.schema(BRONZE_SCHEMA)
              .option("maxFilesPerTrigger", args.max_files_per_trigger)
              .parquet(bronze_path))

    parsed = (bronze.withColumn("j", F.from_json("raw_payload", EVENT_SCHEMA))
                    .withColumn("_malformed", F.col("j._corrupt_record").isNotNull())
                    .select("raw_payload", "source_ref", "ingest_ts", "_malformed", "j.*")
                    .drop("_corrupt_record")
                    .withColumn("event_ts", F.to_timestamp("event_time"))      # honours the literal "UTC" suffix
                    .withColumn("ingest_date", F.to_date("ingest_ts")))

    ts = F.col("event_ts")
    rules = [
        ("malformed_json", F.col("_malformed")),
        ("null_event_id", F.col("event_id").isNull()),
        ("null_user_id", F.col("user_id").isNull()),
        ("bad_event_ts", ts.isNull() | (ts < F.lit(args.min_event_date).cast("timestamp"))
                         | (ts > F.current_timestamp() + F.expr("INTERVAL 1 DAY"))),
        ("bad_event_type", F.coalesce(~F.col("event_type").isin(*VALID_SOURCE_TYPES), F.lit(True))),
        ("negative_price", F.coalesce(F.col("price") < 0, F.lit(False))),     # price 0 is legal (free items)
    ]
    checked = parsed.withColumn("reject_reasons", F.concat_ws(",", *[F.when(c, F.lit(n)) for n, c in rules]))

    # ------------------------------------------------------------------ branch 1: valid -> silver
    mapped_type = (F.when(F.col("event_type") == "view", "product_view")
                    .when(F.col("event_type") == "cart", "add_to_cart")
                    .otherwise(F.col("event_type")))                 # purchase, remove_from_cart keep their names
    valid = (checked.where(F.col("reject_reasons") == "")
             .select("event_id", "event_ts", mapped_type.alias("event_type"), "product_id", "category_id",
                     "category_code", F.split("category_code", r"\.").getItem(0).alias("category_l1"),
                     "brand", "price", "user_id", F.col("user_session").alias("source_session_id"),
                     "ingest_ts", F.to_date("event_ts").alias("event_date"))
             # watermark AFTER validation, so a garbage timestamp can never push the watermark forward
             .withWatermark("event_ts", args.watermark)
             .dropDuplicatesWithinWatermark(["event_id"]))

    def write_valid(batch_df, batch_id):
        batch_df.persist()
        try:
            n = batch_df.count()
            if n == 0:
                return
            batch_df.select(*SILVER_COLS).write.mode("append").insertInto(args.silver_table)
            print(f"[silver] batch {batch_id}: {n:,} new events -> {args.silver_table}", flush=True)
        finally:
            batch_df.unpersist()

    # ------------------------------------------------------------------ branch 2: rejects -> quarantine
    rejects = checked.where(F.col("reject_reasons") != "").select(
        "raw_payload", "source_ref", "ingest_ts", "reject_reasons", "ingest_date")

    def write_rejects(batch_df, batch_id):
        batch_df.persist()
        try:
            n = batch_df.count()
            if n == 0:
                return
            batch_df.write.mode("append").insertInto(args.quarantine_table)
            print(f"[silver] batch {batch_id}: {n:,} REJECTED -> {args.quarantine_table}", flush=True)
        finally:
            batch_df.unpersist()

    trig = f"{args.trigger_seconds} seconds"
    q1 = (valid.writeStream.foreachBatch(write_valid)
          .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_rees46_events")
          .trigger(processingTime=trig).queryName("silver_valid").start())
    q2 = (rejects.writeStream.foreachBatch(write_rejects)
          .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_rees46_quarantine")
          .trigger(processingTime=trig).queryName("silver_rejects").start())
    print(f"[silver] streaming {bronze_path} -> {args.silver_table} (dedup window {args.watermark})", flush=True)

    try:
        if args.run_seconds:
            spark.streams.awaitAnyTermination(args.run_seconds)
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("[silver] stopping...", flush=True)
    finally:
        for q in spark.streams.active:
            q.stop()

    print("[silver] rows in silver:", f"{spark.table(args.silver_table).count():,}",
          "| in quarantine:", f"{spark.table(args.quarantine_table).count():,}", flush=True)


if __name__ == "__main__":
    main()