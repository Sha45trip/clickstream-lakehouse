#!/usr/bin/env python3
"""
PUBLISH gold -> serving Postgres (what the dashboards read).

  gold.rees46_daily_funnel   ->  serving.daily_funnel
  gold.rees46_category_daily ->  serving.category_daily

Why copy instead of querying Spark: dashboards need sub-second answers; a Spark query over small ORC files
takes minutes and a Thrift server would hold YARN memory permanently. The gold aggregates are tiny
(about 1 row per day + ~20 rows per day), so copying them is cheap.

Idempotent and atomic: for the date range, DELETE + INSERT run in ONE transaction, so a dashboard never sees a
half-written day, and re-running a day gives the same result. After the commit the job reads the data back and
compares it with what Spark had; any mismatch exits with code 1 (Airflow marks the task failed).

Needs psycopg2 in the Python that runs this driver (installed in the airflow image).
"""
import argparse
import os
import sys
from datetime import date

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import functions as F
from common import get_spark

FUNNEL_COLS = ["session_date", "sessions", "users", "sessions_view", "sessions_cart", "sessions_purchase",
               "purchase_without_cart", "buyers", "revenue", "view_to_cart_rate", "session_conversion_rate"]
CAT_COLS = ["event_date", "category_l1", "views", "carts", "purchases", "revenue", "users"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", required=True)
    ap.add_argument("--to-date", required=True)
    ap.add_argument("--pg-host", default=os.environ.get("SERVING_PG_HOST", "airflow-db"))
    ap.add_argument("--pg-db", default=os.environ.get("SERVING_PG_DB", "serving"))
    ap.add_argument("--pg-user", default=os.environ.get("SERVING_PG_USER", "airflow"))
    ap.add_argument("--pg-password", default=os.environ.get("SERVING_PG_PASSWORD", "airflow"))
    args = ap.parse_args()
    lo, hi = date.fromisoformat(args.from_date), date.fromisoformat(args.to_date)

    spark = get_spark("13_publish_serving")
    rng = lambda c: F.col(c).between(F.lit(lo).cast("date"), F.lit(hi).cast("date"))

    funnel = [tuple(r[c] for c in FUNNEL_COLS) for r in
              spark.table("gold.rees46_daily_funnel").where(rng("session_date")).select(*FUNNEL_COLS).collect()]
    cats = [tuple(r[c] for c in CAT_COLS) for r in
            spark.table("gold.rees46_category_daily").where(rng("event_date")).select(*CAT_COLS).collect()]
    # NUMERIC(18,2): round the Spark doubles to cents here, once
    funnel = [r[:8] + (round(r[8] or 0.0, 2),) + r[9:] for r in funnel]
    cats = [r[:5] + (round(r[5] or 0.0, 2),) + r[6:] for r in cats]
    if not funnel:
        sys.exit(f"[publish] no gold funnel rows for {lo}..{hi}: refusing to publish an empty range")

    conn = psycopg2.connect(host=args.pg_host, dbname=args.pg_db, user=args.pg_user, password=args.pg_password)
    try:
        with conn:                                    # one transaction: commit on success, rollback on any error
            with conn.cursor() as cur:
                cur.execute("DELETE FROM daily_funnel WHERE session_date BETWEEN %s AND %s", (lo, hi))
                cur.execute("DELETE FROM category_daily WHERE event_date BETWEEN %s AND %s", (lo, hi))
                execute_values(cur, f"INSERT INTO daily_funnel ({','.join(FUNNEL_COLS)}) VALUES %s", funnel)
                if cats:
                    execute_values(cur, f"INSERT INTO category_daily ({','.join(CAT_COLS)}) VALUES %s", cats)

        # read back and compare with what Spark had
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), coalesce(sum(sessions),0), coalesce(sum(revenue),0) "
                        "FROM daily_funnel WHERE session_date BETWEEN %s AND %s", (lo, hi))
            n_f, s_f, r_f = cur.fetchone()
            cur.execute("SELECT count(*), coalesce(sum(purchases),0), coalesce(sum(revenue),0) "
                        "FROM category_daily WHERE event_date BETWEEN %s AND %s", (lo, hi))
            n_c, p_c, r_c = cur.fetchone()
    finally:
        conn.close()

    exp = {"funnel rows": (n_f, len(funnel)), "funnel sessions": (s_f, sum(r[1] for r in funnel)),
           "category rows": (n_c, len(cats)), "category purchases": (p_c, sum(r[4] for r in cats))}
    ok = all(a == b for a, b in exp.values()) and abs(float(r_c) - sum(r[5] for r in cats)) < 0.01
    print(f"\n[publish] range {lo}..{hi}")
    for k, (got, want) in exp.items():
        print(f"[publish]  {'OK ' if got == want else 'BAD'}  {k:20s} serving={got:,} spark={want:,}")
    print(f"[publish]  category revenue serving={float(r_c):,.2f}")
    if not ok:
        sys.exit("[publish] read-back does not match Spark output")
    print("[publish] done")


if __name__ == "__main__":
    main()