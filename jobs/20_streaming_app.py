#!/usr/bin/env python3
"""
STREAMING APPLICATION: Kafka -> bronze -> silver (+ quarantine) -> live aggregates, in ONE Spark application.

  query "bronze"         Kafka topic            -> bronze.rees46_raw          (raw payload + lineage, Parquet)
  query "silver_valid"   new bronze files       -> silver.rees46_events       (parse, validate, 24 h dedup)
                                                -> serving.live_minute        (events + revenue per minute)
  query "silver_rejects" new bronze files       -> silver.rees46_quarantine   (rejected rows + reasons)

Why one application instead of jobs 10 and 11 side by side: every Spark application costs a driver, an
ApplicationMaster and at least one executor. On this laptop (5 GB for YARN) two applications use all of it and
leave nothing for anything else; one application with three queries needs about 3 GB.
Jobs 10 and 11 stay as standalone variants for development; this is the one to run normally.

Checkpoints are the same ones jobs 10 and 11 use, so a run continues where they stopped.

LIVE AGGREGATES (best effort, by design)
  For every micro-batch of NEW (de-duplicated) silver events, events and revenue are counted per minute of event
  time and ADDED to serving.live_minute. The ledger table live_batches records every applied batch id, so a batch that
  Spark re-runs after a crash is not counted twice. If Postgres is unreachable the pipeline keeps going and only logs
  a warning: the live tiles may then miss that batch, and the exact numbers come from the gold layer anyway.
  (Timestamps are written as UTC; the driver container runs in UTC.)
"""
import argparse
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)
from common import CHECKPOINT_ROOT, LAKE_ROOT, get_spark

VALID_SOURCE_TYPES = ["view", "cart", "remove_from_cart", "purchase"]

EVENT_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("event_id", StringType()), ("event_time", StringType()), ("event_type", StringType()),
    ("product_id", StringType()), ("category_id", StringType()), ("category_code", StringType()),
    ("brand", StringType()), ("price", DoubleType()), ("user_id", StringType()),
    ("user_session", StringType()),
    # filled by from_json for unparseable JSON, so broken records are labelled "malformed_json"
    ("_corrupt_record", StringType())]])

BRONZE_SCHEMA = StructType([
    StructField("raw_payload", StringType()), StructField("source", StringType()),
    StructField("source_ref", StringType()), StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()), StructField("source_ts", TimestampType()),
    StructField("ingest_ts", TimestampType()),
    # partition column (from the directory name). It MUST be declared: if the stream starts while the bronze folder
    # is still empty, Spark cannot infer it, and the first batch then fails with "Invalid batch" on a schema mismatch.
    StructField("ingest_date", DateType())])

SILVER_COLS = ["event_id", "event_ts", "event_type", "product_id", "category_id", "category_code",
               "category_l1", "brand", "price", "user_id", "source_session_id", "ingest_ts", "event_date"]


# ------------------------------------------------------------------------------------------------ live writer
def apply_live(pg, batch_id, minute_rows, max_event_ts, n_rows):
    """Add one micro-batch to serving.live_minute exactly once. Returns True if applied, False if already applied."""
    conn = psycopg2.connect(host=pg["host"], dbname=pg["db"], user=pg["user"], password=pg["password"],
                            connect_timeout=5, options="-c timezone=UTC")
    try:
        with conn:                                   # one transaction
            with conn.cursor() as cur:
                cur.execute("INSERT INTO live_batches (query, batch_id) VALUES ('silver_valid', %s) "
                            "ON CONFLICT DO NOTHING", (batch_id,))
                if cur.rowcount != 1:
                    return False
                if minute_rows:
                    execute_values(cur, """
                        INSERT INTO live_minute (minute_ts, event_type, events, revenue) VALUES %s
                        ON CONFLICT (minute_ts, event_type) DO UPDATE SET
                            events = live_minute.events + EXCLUDED.events,
                            revenue = live_minute.revenue + EXCLUDED.revenue,
                            refreshed_at = now()""", minute_rows)
                cur.execute("""
                    INSERT INTO live_status (id, max_event_ts, rows_in_batch, updated_at) VALUES (1, %s, %s, now())
                    ON CONFLICT (id) DO UPDATE SET
                        max_event_ts = GREATEST(live_status.max_event_ts, EXCLUDED.max_event_ts),
                        rows_in_batch = EXCLUDED.rows_in_batch, updated_at = now()""", (max_event_ts, n_rows))
        return True
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="rees46.events.raw")
    ap.add_argument("--landing", default=f"{LAKE_ROOT}/landing_rees46", help="only with --source files (testing)")
    ap.add_argument("--bronze-table", default="bronze.rees46_raw")
    ap.add_argument("--silver-table", default="silver.rees46_events")
    ap.add_argument("--quarantine-table", default="silver.rees46_quarantine")
    ap.add_argument("--watermark", default="24 hours", help="dedup window in event time")
    ap.add_argument("--min-event-date", default="2015-01-01")
    ap.add_argument("--bronze-trigger-seconds", type=int, default=10)
    ap.add_argument("--silver-trigger-seconds", type=int, default=30)
    ap.add_argument("--max-offsets-per-trigger", type=int, default=200_000)
    ap.add_argument("--max-files-per-trigger", type=int, default=30)
    ap.add_argument("--state-partitions", type=int, default=4,
                    help="dedup state partitions. FIXED forever once the checkpoint exists.")
    ap.add_argument("--no-live", action="store_true", help="do not write live aggregates to Postgres")
    ap.add_argument("--pg-host", default=os.environ.get("SERVING_PG_HOST", "airflow-db"))
    ap.add_argument("--pg-db", default=os.environ.get("SERVING_PG_DB", "serving"))
    ap.add_argument("--pg-user", default=os.environ.get("SERVING_PG_USER", "airflow"))
    ap.add_argument("--pg-password", default=os.environ.get("SERVING_PG_PASSWORD", "airflow"))
    ap.add_argument("--run-seconds", type=int, default=0, help="stop after N seconds (0 = until Ctrl+C)")
    args = ap.parse_args()
    pg = {"host": args.pg_host, "db": args.pg_db, "user": args.pg_user, "password": args.pg_password}
    live = not args.no_live

    spark = get_spark("20_streaming_app")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass",
                   "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.shuffle.partitions", str(args.state_partitions))

    b_db, b_name = args.bronze_table.split(".")
    bronze_path = f"{LAKE_ROOT}/bronze/{b_name}"
    silver_path = f"{LAKE_ROOT}/silver/{args.silver_table.split('.')[1]}"
    quar_path = f"{LAKE_ROOT}/silver/{args.quarantine_table.split('.')[1]}"

    # ---- tables
    for db in {b_db, "silver"}:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {args.bronze_table} (
            raw_payload STRING, source STRING, source_ref STRING,
            kafka_partition INT, kafka_offset BIGINT, source_ts TIMESTAMP, ingest_ts TIMESTAMP)
        PARTITIONED BY (ingest_date DATE) STORED AS PARQUET LOCATION '{bronze_path}'""")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {args.silver_table} (
            event_id STRING, event_ts TIMESTAMP, event_type STRING, product_id STRING, category_id STRING,
            category_code STRING, category_l1 STRING, brand STRING, price DOUBLE, user_id STRING,
            source_session_id STRING, ingest_ts TIMESTAMP)
        PARTITIONED BY (event_date DATE) STORED AS ORC LOCATION '{silver_path}'""")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {args.quarantine_table} (
            raw_payload STRING, source_ref STRING, ingest_ts TIMESTAMP, reject_reasons STRING)
        PARTITIONED BY (ingest_date DATE) STORED AS ORC LOCATION '{quar_path}'""")
    # the silver queries read this directory as a stream; on a first-ever run it must exist before bronze writes
    jpath = spark._jvm.org.apache.hadoop.fs.Path(bronze_path)
    jpath.getFileSystem(spark._jsc.hadoopConfiguration()).mkdirs(jpath)

    # ================================================================================== query 1: BRONZE
    if args.source == "kafka":
        src = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap).option("subscribe", args.topic)
               .option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
               .option("failOnDataLoss", "false").load()
               .select(F.col("value").cast("string").alias("raw_payload"),
                       F.lit("kafka").alias("source"),
                       F.concat_ws("/", F.col("topic"), F.col("partition").cast("string")).alias("source_ref"),
                       F.col("partition").cast("int").alias("kafka_partition"),
                       F.col("offset").cast("long").alias("kafka_offset"),
                       F.col("timestamp").alias("source_ts")))
    else:
        src = (spark.readStream.format("text").option("maxFilesPerTrigger", 100).load(args.landing)
               .select(F.col("value").alias("raw_payload"), F.lit("files").alias("source"),
                       F.input_file_name().alias("source_ref"),
                       F.lit(None).cast("int").alias("kafka_partition"),
                       F.lit(None).cast("long").alias("kafka_offset"),
                       F.lit(None).cast("timestamp").alias("source_ts")))
    src = (src.withColumn("ingest_ts", F.current_timestamp()).withColumn("ingest_date", F.to_date("ingest_ts")))

    def write_bronze(batch_df, batch_id):
        batch_df.persist()
        try:
            n = batch_df.count()
            if n == 0:
                return
            batch_df.write.mode("append").insertInto(args.bronze_table)     # columns by position = table DDL
            print(f"[bronze] batch {batch_id}: {n:,} rows", flush=True)
        finally:
            batch_df.unpersist()

    # ================================================================================== silver (two queries)
    bronze_stream = (spark.readStream.schema(BRONZE_SCHEMA)
                     .option("maxFilesPerTrigger", args.max_files_per_trigger).parquet(bronze_path))
    parsed = (bronze_stream.withColumn("j", F.from_json("raw_payload", EVENT_SCHEMA))
              .withColumn("_malformed", F.col("j._corrupt_record").isNotNull())
              .select("raw_payload", "source_ref", "ingest_ts", "_malformed", "j.*").drop("_corrupt_record")
              .withColumn("event_ts", F.to_timestamp("event_time"))
              .withColumn("ingest_date", F.to_date("ingest_ts")))
    ts = F.col("event_ts")
    rules = [
        ("malformed_json", F.col("_malformed")),
        ("null_event_id", F.col("event_id").isNull()),
        ("null_user_id", F.col("user_id").isNull()),
        ("bad_event_ts", ts.isNull() | (ts < F.lit(args.min_event_date).cast("timestamp"))
                         | (ts > F.current_timestamp() + F.expr("INTERVAL 1 DAY"))),
        ("bad_event_type", F.coalesce(~F.col("event_type").isin(*VALID_SOURCE_TYPES), F.lit(True))),
        ("negative_price", F.coalesce(F.col("price") < 0, F.lit(False))),
    ]
    checked = parsed.withColumn("reject_reasons", F.concat_ws(",", *[F.when(c, F.lit(n)) for n, c in rules]))

    mapped_type = (F.when(F.col("event_type") == "view", "product_view")
                    .when(F.col("event_type") == "cart", "add_to_cart")
                    .otherwise(F.col("event_type")))
    valid = (checked.where(F.col("reject_reasons") == "")
             .select("event_id", "event_ts", mapped_type.alias("event_type"), "product_id", "category_id",
                     "category_code", F.split("category_code", r"\.").getItem(0).alias("category_l1"),
                     "brand", "price", "user_id", F.col("user_session").alias("source_session_id"),
                     "ingest_ts", F.to_date("event_ts").alias("event_date"))
             .withWatermark("event_ts", args.watermark)
             .dropDuplicatesWithinWatermark(["event_id"]))

    def write_valid(batch_df, batch_id):
        batch_df.persist()
        try:
            n = batch_df.count()
            if n == 0:
                return
            batch_df.select(*SILVER_COLS).write.mode("append").insertInto(args.silver_table)
            note = ""
            if live:                                  # AFTER silver is written, so live is never ahead of silver
                try:
                    agg = (batch_df.groupBy(F.date_trunc("minute", "event_ts").alias("minute_ts"), "event_type")
                           .agg(F.count("*").alias("events"),
                                F.sum(F.when(F.col("event_type") == "purchase", F.coalesce("price", F.lit(0.0)))
                                      .otherwise(0.0)).alias("revenue")).collect())
                    rows = [(r["minute_ts"], r["event_type"], r["events"], round(r["revenue"] or 0.0, 2)) for r in agg]
                    max_ts = batch_df.agg(F.max("event_ts")).first()[0]
                    applied = apply_live(pg, batch_id, rows, max_ts, n)
                    note = f" | live: {len(rows)} minute-rows" if applied else " | live: batch already applied"
                except Exception as e:                # best effort: never stall the pipeline for a dashboard
                    note = f" | LIVE WRITE FAILED ({type(e).__name__}: {str(e)[:80]})"
            print(f"[silver] batch {batch_id}: {n:,} new events{note}", flush=True)
        finally:
            batch_df.unpersist()

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

    # ================================================================================== start everything
    s_trig = f"{args.silver_trigger_seconds} seconds"
    (src.writeStream.foreachBatch(write_bronze)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{b_name}_{args.source}")
        .trigger(processingTime=f"{args.bronze_trigger_seconds} seconds").queryName("bronze").start())
    (valid.writeStream.foreachBatch(write_valid)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_rees46_events")
        .trigger(processingTime=s_trig).queryName("silver_valid").start())
    (rejects.writeStream.foreachBatch(write_rejects)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_rees46_quarantine")
        .trigger(processingTime=s_trig).queryName("silver_rejects").start())
    print(f"[app] streaming {args.source} -> {args.bronze_table} -> {args.silver_table} "
          f"(dedup {args.watermark}, live aggregates {'ON' if live else 'OFF'})", flush=True)

    try:
        if args.run_seconds:
            spark.streams.awaitAnyTermination(args.run_seconds)
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("[app] stopping...", flush=True)
    finally:
        for q in spark.streams.active:
            q.stop()

    print("[app] rows: bronze", f"{spark.table(args.bronze_table).count():,}",
          "| silver", f"{spark.table(args.silver_table).count():,}",
          "| quarantine", f"{spark.table(args.quarantine_table).count():,}", flush=True)


if __name__ == "__main__":
    main()
