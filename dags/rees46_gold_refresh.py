"""
rees46_gold_refresh: the SPEED LAYER. Every 10 minutes, rebuild and publish gold for the day that is being replayed.

    find_day  ->  build_gold  ->  publish_serving

  * find_day       reads serving.live_status (written by the streaming app) to learn which event day is "now".
                   If no new data arrived for 15 minutes (stream stopped or finished) it SKIPS the run: no compute is
                   wasted re-publishing an unchanged day.
  * build_gold     job 12 for that single day, in Spark LOCAL mode inside this container (2 cores, 2 GB). Local mode
                   keeps the 5 GB YARN node free for the streaming application.
  * publish_serving job 13: atomic copy of that day's gold rows into the serving Postgres.

Provisional by design: there is NO reconciliation gate here, because silver keeps growing while gold is being
built, so gold can legitimately trail silver by a few minutes of data. The daily DAG (rees46_daily_gold) re-builds
every settled day with check_silver / check_gold and overwrites these numbers with verified ones.
Sessions that started the day before and run past midnight are corrected by that daily run as well.

The pool "local_spark" has ONE slot (create it once: airflow pools set local_spark 1 "one local Spark job at a time"),
so refresh runs never overlap with each other or pile up memory.
"""
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

JOBS = os.environ.get("LAKEHOUSE_JOBS", "/opt/jobs")
SPARK_GOLD = os.environ.get(
    "LAKEHOUSE_SPARK_GOLD_LOCAL",
    "spark-submit --master local[2] --driver-memory 2g --conf spark.driver.host=airflow")
SPARK_LOCAL = os.environ.get(
    "LAKEHOUSE_SPARK_LOCAL",
    "spark-submit --master local[1] --driver-memory 1g --conf spark.driver.host=airflow")
STALE_AFTER_SECONDS = int(os.environ.get("LAKEHOUSE_REFRESH_STALE_SECONDS", "900"))

DAY = "{{ ti.xcom_pull(task_ids='find_day') }}"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=25),
}

with DAG(
    dag_id="rees46_gold_refresh",
    description="Every 10 min: rebuild + publish gold for the day currently being replayed (provisional numbers)",
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    is_paused_upon_creation=True,
    default_args=default_args,
    tags=["rees46", "speed-layer"],
) as dag:

    @task
    def find_day() -> str:
        import psycopg2
        from airflow.sdk.exceptions import AirflowSkipException

        conn = psycopg2.connect(
            host=os.environ.get("SERVING_PG_HOST", "airflow-db"), dbname=os.environ.get("SERVING_PG_DB", "serving"),
            user=os.environ.get("SERVING_PG_USER", "airflow"), password=os.environ.get("SERVING_PG_PASSWORD", "airflow"),
            connect_timeout=5)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT (max_event_ts AT TIME ZONE 'UTC')::date, "
                            "EXTRACT(EPOCH FROM (now() - updated_at)) FROM live_status WHERE id = 1")
                row = cur.fetchone()
        finally:
            conn.close()
        if row is None or row[0] is None:
            raise AirflowSkipException("live_status is empty: the streaming app has not written anything yet")
        day, age = row
        if age > STALE_AFTER_SECONDS:
            raise AirflowSkipException(f"no new data for {age / 60:.0f} min (limit {STALE_AFTER_SECONDS // 60} min): nothing to refresh")
        return day.isoformat()

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=SPARK_GOLD + " " + JOBS + "/12_silver_to_gold_rees46.py --from-date " + DAY + " --to-date " + DAY,
        pool="local_spark",
    )

    publish_serving = BashOperator(
        task_id="publish_serving",
        bash_command=SPARK_LOCAL + " " + JOBS + "/13_publish_serving.py --from-date " + DAY + " --to-date " + DAY,
        pool="local_spark",
    )

    find_day() >> build_gold >> publish_serving
