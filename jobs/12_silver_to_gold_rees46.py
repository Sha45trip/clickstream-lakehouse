#!/usr/bin/env python3
"""
GOLD for the REES46 events (re-runnable per date range; schedule it every few minutes or nightly).

  silver.rees46_events  ->  gold.rees46_sessions        one row per session (our own 30-min-gap sessionization)
                        ->  gold.rees46_daily_funnel    per session_date: view -> cart -> purchase
                        ->  gold.rees46_category_daily  per event_date x category_l1: views, carts, purchases, revenue

Also prints a VALIDATION REPORT: how well our sessions agree with the dataset's own user_session ids.

Window: sessions that START inside [--from-date, --to-date]. One day of events before and after is read so that
sessions crossing midnight are stitched correctly. Writes use dynamic partition overwrite -> idempotent.

NOTE the funnel is NOT strictly nested. In this dataset some sessions purchase without a cart event
(cart tracking gaps), so each stage is counted independently and the gap is reported explicitly.
"""
import argparse
from datetime import date, timedelta
from pyspark.sql import Window, functions as F
from common import LAKE_ROOT, get_spark


def flag(event_type):
    return F.sum((F.col("event_type") == event_type).cast("int"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", required=True, help="session start date, inclusive (YYYY-MM-DD)")
    ap.add_argument("--to-date", required=True, help="session start date, inclusive (YYYY-MM-DD)")
    ap.add_argument("--gap-seconds", type=int, default=1800, help="inactivity that ends a session (default 30 min)")
    ap.add_argument("--silver-table", default="silver.rees46_events")
    args = ap.parse_args()
    lo, hi = date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)

    spark = get_spark("12_gold_rees46")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS gold.rees46_sessions (
            session_id STRING, user_id STRING, session_start TIMESTAMP, session_end TIMESTAMP,
            duration_sec BIGINT, n_events BIGINT, n_views BIGINT, n_carts BIGINT, n_removes BIGINT,
            n_purchases BIGINT, n_products BIGINT, revenue DOUBLE, converted BOOLEAN,
            first_category STRING, n_source_sessions BIGINT)
        PARTITIONED BY (session_date DATE)
        STORED AS ORC LOCATION '{LAKE_ROOT}/gold/rees46_sessions'""")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS gold.rees46_daily_funnel (
            sessions BIGINT, users BIGINT, sessions_view BIGINT, sessions_cart BIGINT, sessions_purchase BIGINT,
            purchase_without_cart BIGINT, buyers BIGINT, revenue DOUBLE,
            view_to_cart_rate DOUBLE, session_conversion_rate DOUBLE)
        PARTITIONED BY (session_date DATE)
        STORED AS ORC LOCATION '{LAKE_ROOT}/gold/rees46_daily_funnel'""")
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS gold.rees46_category_daily (
            category_l1 STRING, views BIGINT, carts BIGINT, purchases BIGINT, revenue DOUBLE, users BIGINT)
        PARTITIONED BY (event_date DATE)
        STORED AS ORC LOCATION '{LAKE_ROOT}/gold/rees46_category_daily'""")

    ev = (spark.table(args.silver_table)
               .where(F.col("event_date").between(F.lit(lo - timedelta(days=1)).cast("date"),
                                                   F.lit(hi + timedelta(days=1)).cast("date"))))

    # ---- sessionize: a new session starts when the gap to the user's previous event exceeds the threshold
    w = Window.partitionBy("user_id").orderBy("event_ts", "event_id")
    ev = (ev.withColumn("prev_ts", F.lag("event_ts").over(w))
            .withColumn("new_session",
                        F.when(F.col("prev_ts").isNull() |
                               ((F.unix_timestamp("event_ts") - F.unix_timestamp("prev_ts")) > args.gap_seconds), 1)
                        .otherwise(0))
            .withColumn("seq", F.sum("new_session").over(w))
            .persist())

    sessions = (ev.groupBy("user_id", "seq").agg(
                    F.min("event_ts").alias("session_start"),
                    F.max("event_ts").alias("session_end"),
                    F.count("*").alias("n_events"),
                    flag("product_view").alias("n_views"),
                    flag("add_to_cart").alias("n_carts"),
                    flag("remove_from_cart").alias("n_removes"),
                    flag("purchase").alias("n_purchases"),
                    F.countDistinct("product_id").alias("n_products"),
                    F.sum(F.when(F.col("event_type") == "purchase", F.coalesce("price", F.lit(0.0)))
                          .otherwise(0.0)).alias("revenue"),
                    F.expr("min_by(category_l1, event_ts)").alias("first_category"),
                    F.countDistinct("source_session_id").alias("n_source_sessions"))
                .withColumn("session_id", F.md5(F.concat_ws("|", "user_id", F.col("session_start").cast("string"))))
                .withColumn("duration_sec", F.unix_timestamp("session_end") - F.unix_timestamp("session_start"))
                .withColumn("converted", F.col("n_purchases") > 0)
                .withColumn("session_date", F.to_date("session_start"))
                .where(F.col("session_date").between(F.lit(lo).cast("date"), F.lit(hi).cast("date")))
                .select("session_id", "user_id", "session_start", "session_end", "duration_sec", "n_events",
                        "n_views", "n_carts", "n_removes", "n_purchases", "n_products", "revenue", "converted",
                        "first_category", "n_source_sessions", "session_date")
                .persist())
    sessions.write.insertInto("gold.rees46_sessions", overwrite=True)

    # ---- daily funnel (stages counted independently, see module docstring)
    def reached(c):
        return F.sum((F.col(c) > 0).cast("int"))

    funnel = (sessions.groupBy("session_date").agg(
                  F.count("*").alias("sessions"), F.countDistinct("user_id").alias("users"),
                  reached("n_views").alias("sessions_view"), reached("n_carts").alias("sessions_cart"),
                  reached("n_purchases").alias("sessions_purchase"),
                  F.sum(((F.col("n_purchases") > 0) & (F.col("n_carts") == 0)).cast("int")).alias("purchase_without_cart"),
                  F.countDistinct(F.when(F.col("n_purchases") > 0, F.col("user_id"))).alias("buyers"),
                  F.sum("revenue").alias("revenue"))
              .withColumn("view_to_cart_rate", F.round(F.col("sessions_cart") / F.col("sessions_view"), 4))
              .withColumn("session_conversion_rate", F.round(F.col("sessions_purchase") / F.col("sessions"), 4))
              .select("sessions", "users", "sessions_view", "sessions_cart", "sessions_purchase",
                      "purchase_without_cart", "buyers", "revenue", "view_to_cart_rate",
                      "session_conversion_rate", "session_date"))
    funnel.write.insertInto("gold.rees46_daily_funnel", overwrite=True)

    # ---- category x day (from events directly)
    cat = (spark.table(args.silver_table)
                .where(F.col("event_date").between(F.lit(lo).cast("date"), F.lit(hi).cast("date")))
                .groupBy(F.coalesce("category_l1", F.lit("unknown")).alias("category_l1"), "event_date").agg(
                    flag("product_view").alias("views"), flag("add_to_cart").alias("carts"),
                    flag("purchase").alias("purchases"),
                    F.sum(F.when(F.col("event_type") == "purchase", F.coalesce("price", F.lit(0.0)))
                          .otherwise(0.0)).alias("revenue"),
                    F.countDistinct("user_id").alias("users"))
                .select("category_l1", "views", "carts", "purchases", "revenue", "users", "event_date"))
    cat.write.insertInto("gold.rees46_category_daily", overwrite=True)

    # ---- validation: do our sessions agree with the dataset's own session ids?
    n = sessions.count()
    ours = sessions.groupBy(F.when(F.col("n_source_sessions") == 1, "exactly 1 source session")
                             .when(F.col("n_source_sessions") > 1, "merges several source sessions")
                             .otherwise("no source session id")).count().collect()
    src = (ev.where(F.col("source_session_id").isNotNull())
             .groupBy("source_session_id").agg(F.countDistinct("user_id", "seq").alias("n_ours"))
             .groupBy(F.when(F.col("n_ours") == 1, "kept whole")
                       .otherwise("split into several of ours")).count().collect())
    print(f"\n[gold] sessions written: {n:,}  (gap = {args.gap_seconds}s)")
    print("[gold] OUR sessions vs source session ids:")
    for k, c in sorted(ours, key=lambda r: -r[1]):
        print(f"         {k:34s} {c:>12,}  {100.0 * c / n:5.1f}%")
    t = sum(c for _, c in src) or 1
    print("[gold] SOURCE sessions vs our sessions:")
    for k, c in sorted(src, key=lambda r: -r[1]):
        print(f"         {k:34s} {c:>12,}  {100.0 * c / t:5.1f}%")
    spark.table("gold.rees46_daily_funnel").where(
        F.col("session_date").between(F.lit(lo).cast("date"), F.lit(hi).cast("date"))
    ).orderBy("session_date").show(truncate=False)
    ev.unpersist()
    sessions.unpersist()


if __name__ == "__main__":
    main()