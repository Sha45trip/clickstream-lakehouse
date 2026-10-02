#!/usr/bin/env python3
"""
GOLD: sessionization (30-min inactivity gap) + daily conversion funnel.

  silver.events -> gold.sessions      (one row per session, partitioned by session_date)
                -> gold.daily_funnel  (per day x device: sessions at each funnel stage, conversion, revenue)

Window: sessions that START within [--from-date, --to-date]. Events from one day before / after are read so
sessions crossing midnight are stitched correctly. Re-runnable (dynamic partition overwrite).
"""
import argparse
from datetime import date, timedelta
from pyspark.sql import Window, functions as F
from common import GOLD_FUNNEL_PATH, GOLD_SESSIONS_PATH, get_spark

SESSION_GAP_SEC = 30 * 60


def flag(event_type):
    return F.sum((F.col("event_type") == event_type).cast("int"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", required=True, help="session start date, inclusive (YYYY-MM-DD)")
    ap.add_argument("--to-date", required=True, help="session start date, inclusive (YYYY-MM-DD)")
    ap.add_argument("--gap-seconds", type=int, default=SESSION_GAP_SEC)
    args = ap.parse_args()
    lo, hi = date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)

    spark = get_spark("03_silver_to_gold")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS gold.sessions (
            session_id STRING, user_id STRING, session_start TIMESTAMP, session_end TIMESTAMP,
            duration_sec BIGINT, n_events BIGINT, n_page_views BIGINT, n_searches BIGINT,
            n_product_views BIGINT, n_add_to_cart BIGINT, n_begin_checkout BIGINT, n_purchases BIGINT,
            revenue DOUBLE, converted BOOLEAN, device_type STRING, country STRING, utm_source STRING)
        PARTITIONED BY (session_date DATE)
        STORED AS ORC LOCATION '{GOLD_SESSIONS_PATH}'""")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS gold.daily_funnel (
            device_type STRING, sessions BIGINT, users BIGINT, sessions_product_view BIGINT,
            sessions_add_to_cart BIGINT, sessions_checkout BIGINT, sessions_purchase BIGINT,
            revenue DOUBLE, view_to_cart_rate DOUBLE, cart_to_purchase_rate DOUBLE, session_conversion_rate DOUBLE)
        PARTITIONED BY (session_date DATE)
        STORED AS ORC LOCATION '{GOLD_FUNNEL_PATH}'""")

    ev = (spark.table("silver.events")
               .where(F.col("event_date").between(F.lit(lo - timedelta(days=1)).cast("date"),
                                                   F.lit(hi + timedelta(days=1)).cast("date"))))

    # ---- sessionize: new session when gap to previous event of the same user > threshold
    w = Window.partitionBy("user_id").orderBy("event_ts", "event_id")
    ev = (ev.withColumn("prev_ts", F.lag("event_ts").over(w))
            .withColumn("new_session",
                        F.when(F.col("prev_ts").isNull() |
                               ((F.unix_timestamp("event_ts") - F.unix_timestamp("prev_ts")) > args.gap_seconds), 1)
                        .otherwise(0))
            .withColumn("seq", F.sum("new_session").over(w)))

    sessions = (ev.groupBy("user_id", "seq").agg(
                    F.min("event_ts").alias("session_start"),
                    F.max("event_ts").alias("session_end"),
                    F.count("*").alias("n_events"),
                    flag("page_view").alias("n_page_views"),
                    flag("search").alias("n_searches"),
                    flag("product_view").alias("n_product_views"),
                    flag("add_to_cart").alias("n_add_to_cart"),
                    flag("begin_checkout").alias("n_begin_checkout"),
                    flag("purchase").alias("n_purchases"),
                    F.sum(F.coalesce("order_value", F.lit(0.0))).alias("revenue"),
                    F.expr("min_by(device_type, event_ts)").alias("device_type"),
                    F.expr("min_by(country, event_ts)").alias("country"),
                    F.expr("min_by(utm_source, event_ts)").alias("utm_source"))
                .withColumn("session_id", F.md5(F.concat_ws("|", "user_id", F.col("session_start").cast("string"))))
                .withColumn("duration_sec", F.unix_timestamp("session_end") - F.unix_timestamp("session_start"))
                .withColumn("converted", F.col("n_purchases") > 0)
                .withColumn("session_date", F.to_date("session_start"))
                .where(F.col("session_date").between(F.lit(lo).cast("date"), F.lit(hi).cast("date")))
                .select("session_id", "user_id", "session_start", "session_end", "duration_sec", "n_events",
                        "n_page_views", "n_searches", "n_product_views", "n_add_to_cart", "n_begin_checkout",
                        "n_purchases", "revenue", "converted", "device_type", "country", "utm_source",
                        "session_date")
                .cache())
    sessions.write.insertInto("gold.sessions", overwrite=True)

    # ---- daily funnel
    def reached(c):
        return F.sum((F.col(c) > 0).cast("int"))

    funnel = (sessions.groupBy("session_date", "device_type").agg(
                  F.count("*").alias("sessions"),
                  F.countDistinct("user_id").alias("users"),
                  reached("n_product_views").alias("sessions_product_view"),
                  reached("n_add_to_cart").alias("sessions_add_to_cart"),
                  reached("n_begin_checkout").alias("sessions_checkout"),
                  reached("n_purchases").alias("sessions_purchase"),
                  F.sum("revenue").alias("revenue"))
              .withColumn("view_to_cart_rate", F.round(F.col("sessions_add_to_cart") / F.col("sessions_product_view"), 4))
              .withColumn("cart_to_purchase_rate", F.round(F.col("sessions_purchase") / F.col("sessions_add_to_cart"), 4))
              .withColumn("session_conversion_rate", F.round(F.col("sessions_purchase") / F.col("sessions"), 4))
              .select("device_type", "sessions", "users", "sessions_product_view", "sessions_add_to_cart",
                      "sessions_checkout", "sessions_purchase", "revenue", "view_to_cart_rate",
                      "cart_to_purchase_rate", "session_conversion_rate", "session_date"))
    funnel.write.insertInto("gold.daily_funnel", overwrite=True)

    print("sessions written:", f"{sessions.count():,}")
    spark.table("gold.daily_funnel").where(F.col("session_date").between(F.lit(lo).cast("date"), F.lit(hi).cast("date"))) \
         .orderBy("session_date", "device_type").show(50, truncate=False)


if __name__ == "__main__":
    main()
