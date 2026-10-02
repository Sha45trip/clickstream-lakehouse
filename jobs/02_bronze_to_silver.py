#!/usr/bin/env python3
"""
SILVER: parse, validate, de-duplicate, quarantine.

  bronze.clickstream_raw --(parse JSON, apply DQ rules)--> silver.events            (ORC, partitioned by event_date)
                                                      \--> silver.events_quarantine (rejects + reasons)

Incremental by ingest_date. Late/duplicate events land in *older* event_date partitions, so every
affected event_date partition is rebuilt as  (existing rows UNION new rows) -> dedup on event_id
and written with dynamic partition overwrite. Idempotent: re-running a window gives the same result.
(Phase 3 replaces this rewrite pattern with Iceberg MERGE INTO.)
"""
import argparse
from datetime import date
from pyspark.sql import Window, functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField, StructType)
from common import (CHECKPOINT_ROOT, SILVER_EVENTS_PATH, SILVER_QUARANTINE_PATH, VALID_EVENT_TYPES, get_spark)

EVENT_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("event_id", StringType()), ("event_ts", StringType()), ("user_id", StringType()),
    ("event_type", StringType()), ("page_url", StringType()), ("product_id", StringType()),
    ("category", StringType()), ("price", DoubleType()), ("quantity", IntegerType()),
    ("order_id", StringType()), ("order_value", DoubleType()), ("device_type", StringType()),
    ("os", StringType()), ("browser", StringType()), ("country", StringType()),
    ("referrer", StringType()), ("utm_source", StringType()), ("utm_campaign", StringType())]])

EVENT_COLS = ["event_id", "event_ts", "user_id", "event_type", "page_url", "product_id", "category",
              "price", "quantity", "order_id", "order_value", "device_type", "os", "browser",
              "country", "referrer", "utm_source", "utm_campaign", "ingest_ts", "event_date"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingest-from", default=date.today().isoformat(), help="bronze ingest_date, inclusive")
    ap.add_argument("--ingest-to", default=date.today().isoformat(), help="bronze ingest_date, inclusive")
    ap.add_argument("--all", action="store_true", help="process every bronze partition")
    args = ap.parse_args()

    spark = get_spark("02_bronze_to_silver")
    spark.sparkContext.setCheckpointDir(f"{CHECKPOINT_ROOT}/tmp")

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver.events (
            event_id STRING, event_ts TIMESTAMP, user_id STRING, event_type STRING, page_url STRING,
            product_id STRING, category STRING, price DOUBLE, quantity INT, order_id STRING,
            order_value DOUBLE, device_type STRING, os STRING, browser STRING, country STRING,
            referrer STRING, utm_source STRING, utm_campaign STRING, ingest_ts TIMESTAMP)
        PARTITIONED BY (event_date DATE)
        STORED AS ORC
        LOCATION '{SILVER_EVENTS_PATH}'""")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver.events_quarantine (
            raw_payload STRING, source_ref STRING, ingest_ts TIMESTAMP, reject_reasons STRING)
        PARTITIONED BY (ingest_date DATE)
        STORED AS ORC
        LOCATION '{SILVER_QUARANTINE_PATH}'""")

    bronze = spark.table("bronze.clickstream_raw")
    if not args.all:
        bronze = bronze.where(F.col("ingest_date").between(F.lit(args.ingest_from).cast("date"),
                                                            F.lit(args.ingest_to).cast("date")))

    # ---- parse
    p = (bronze.withColumn("j", F.from_json("raw_payload", EVENT_SCHEMA))
               .withColumn("_malformed", F.col("j").isNull())
               .select("raw_payload", "source_ref", "ingest_ts", "ingest_date", "_malformed", "j.*")
               .withColumn("event_ts_raw", F.col("event_ts"))
               .withColumn("event_ts", F.to_timestamp("event_ts_raw")))

    # ---- data-quality rules: each yields its rule name when violated
    ts = F.col("event_ts")
    rules = [
        ("malformed_json", F.col("_malformed")),
        ("null_event_id", F.col("event_id").isNull()),
        ("null_user_id", F.col("user_id").isNull()),
        ("bad_event_ts", ts.isNull() | (ts < F.lit("2020-01-01").cast("timestamp"))
                         | (ts > F.current_timestamp() + F.expr("INTERVAL 1 DAY"))),
        ("bad_event_type", F.coalesce(~F.col("event_type").isin(*VALID_EVENT_TYPES), F.lit(True))),
        ("negative_price", F.coalesce(F.col("price") < 0, F.lit(False))),
        ("bad_quantity", F.coalesce(F.col("quantity") <= 0, F.lit(False))),
    ]
    p = p.withColumn("reject_reasons", F.concat_ws(",", *[F.when(c, F.lit(n)) for n, c in rules])).cache()

    # ---- quarantine (rewritten per ingest_date partition -> idempotent)
    (p.where(F.col("reject_reasons") != "")
       .select("raw_payload", "source_ref", "ingest_ts", "reject_reasons", "ingest_date")
       .write.insertInto("silver.events_quarantine", overwrite=True))

    # ---- valid events
    valid = (p.where(F.col("reject_reasons") == "")
              .withColumn("event_date", F.to_date("event_ts"))
              .select(*EVENT_COLS))

    affected = [r[0] for r in valid.select("event_date").distinct().collect()]
    if affected:
        existing = spark.table("silver.events").where(F.col("event_date").isin(affected)).select(*EVENT_COLS)
        w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").asc())
        merged = (valid.unionByName(existing)
                       .withColumn("_rn", F.row_number().over(w))
                       .where("_rn = 1").drop("_rn")
                       .checkpoint(eager=True))          # cut lineage: we overwrite a table we just read
        merged.write.insertInto("silver.events", overwrite=True)

    n_in, n_bad = p.count(), p.where(F.col("reject_reasons") != "").count()
    print(f"bronze rows read: {n_in:,} | rejected -> quarantine: {n_bad:,} | "
          f"event_date partitions rewritten: {len(affected)}")
    print("silver.events total:", f"{spark.table('silver.events').count():,}")


if __name__ == "__main__":
    main()
