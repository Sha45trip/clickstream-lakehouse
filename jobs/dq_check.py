#!/usr/bin/env python3
"""
DATA-QUALITY GATES for the REES46 pipeline. Run by Airflow between pipeline stages.

  --stage silver --date D   is silver for day D complete and sane?   (run BEFORE building gold)
  --stage gold   --date D   does gold for day D reconcile to silver? (run AFTER building gold)

Exit code 0 = all hard checks passed, 1 = at least one failed. Airflow marks the task failed and does NOT run
downstream tasks, so bad data never reaches the next layer. WARN lines never fail the run.
"""
import argparse
import sys
from pyspark.sql import functions as F
from common import get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=["silver", "gold"], required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--min-rows", type=int, default=1000, help="silver: minimum events expected for the day")
    ap.add_argument("--silver-table", default="silver.rees46_events")
    args = ap.parse_args()

    spark = get_spark(f"dq_check_{args.stage}")
    d = F.lit(args.date).cast("date")
    results = []                                  # (name, ok, detail, hard)

    def check(name, ok, detail, hard=True):
        results.append((name, bool(ok), detail, hard))

    silver = spark.table(args.silver_table).where(F.col("event_date") == d)

    if args.stage == "silver":
        r = silver.agg(
            F.count("*").alias("n"),
            F.countDistinct("event_id").alias("uniq"),
            F.sum((F.col("user_id").isNull() | F.col("event_ts").isNull() | F.col("event_type").isNull())
                  .cast("int")).alias("nulls"),
            F.sum((F.col("price") < 0).cast("int")).alias("neg_price")).first()
        n = r["n"] or 0
        check("rows >= min_rows", n >= args.min_rows, f"{n:,} rows (min {args.min_rows:,})")
        check("event_id unique", (r["uniq"] or 0) == n, f"{n:,} rows, {(r['uniq'] or 0):,} distinct ids")
        check("no null user/time/type", (r["nulls"] or 0) == 0, f"{r['nulls'] or 0} rows with nulls")
        check("no negative price", (r["neg_price"] or 0) == 0, f"{r['neg_price'] or 0} rows")

    else:
        s = silver.agg(
            F.sum((F.col("event_type") == "purchase").cast("int")).alias("p"),
            F.sum(F.when(F.col("event_type") == "purchase", F.coalesce("price", F.lit(0.0))).otherwise(0.0)).alias("rev")
        ).first()
        g = (spark.table("gold.rees46_category_daily").where(F.col("event_date") == d)
             .agg(F.sum("purchases").alias("p"), F.sum("revenue").alias("rev")).first())
        sp, srev, gp, grev = s["p"] or 0, s["rev"] or 0.0, g["p"] or 0, g["rev"] or 0.0
        check("gold purchases == silver purchases", sp == gp, f"silver {sp:,} vs gold {gp:,}")
        check("gold revenue == silver revenue (±0.01)", abs(srev - grev) <= 0.01,
              f"silver {srev:,.2f} vs gold {grev:,.2f}")
        f = spark.table("gold.rees46_daily_funnel").where(F.col("session_date") == d).first()
        check("funnel row exists", f is not None, "found" if f else "no row for this date")
        if f is not None:
            check("sessions > 0", f["sessions"] > 0, f"{f['sessions']:,} sessions")
            check("purchase sessions <= sessions", f["sessions_purchase"] <= f["sessions"],
                  f"{f['sessions_purchase']:,} <= {f['sessions']:,}")
            check("conversion within 0..1", 0 <= (f["session_conversion_rate"] or 0) <= 1,
                  f"{f['session_conversion_rate']}")
            cov = 1 - (f["purchase_without_cart"] / f["sessions_purchase"]) if f["sessions_purchase"] else None
            # soft check: cart tracking coverage on purchase sessions was ~36-38% in the first two days of data
            check("cart coverage on purchase sessions >= 20%", cov is not None and cov >= 0.20,
                  "n/a" if cov is None else f"{cov:.1%}", hard=False)

    print(f"\n[dq] stage={args.stage} date={args.date}")
    failed = 0
    for name, ok, detail, hard in results:
        tag = "PASS" if ok else ("FAIL" if hard else "WARN")
        failed += (not ok) and hard
        print(f"[dq]  {tag:4s}  {name:45s} {detail}")
    if failed:
        print(f"[dq] {failed} hard check(s) FAILED")
        sys.exit(1)
    print("[dq] all hard checks passed")


if __name__ == "__main__":
    main()