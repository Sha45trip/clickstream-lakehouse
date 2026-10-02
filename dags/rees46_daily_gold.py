"""
rees46_daily_gold: build and verify the gold layer for one day.

    check_silver  ->  build_gold  ->  check_gold

  * check_silver : data-quality gate on the day's silver data. If it fails, gold is NOT built.
  * build_gold   : Spark-on-YARN job 12 for that day (idempotent: safe to retry or backfill).
  * check_gold   : reconciliation of gold against silver (purchases and revenue must match to the cent).

Operations
  * schedule @daily; each run processes the logical date {{ ds }}  (run for D happens after D ends)
  * retries with exponential backoff; execution_timeout kills hung jobs
  * pool "yarn" has ONE slot: only one Spark application runs at a time (the cluster has 5 GB)
  * unpausing runs the historical days 2019-10-01 and 2019-10-02 (catchup); re-run any day from the UI, or:
    airflow backfill create --dag-id rees46_daily_gold --from-date 2019-10-01 --to-date 2019-10-02
"""
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

JOBS = os.environ.get("LAKEHOUSE_JOBS", "/opt/jobs")
# Heavy job on the YARN cluster (driver in this container, hence spark.driver.host=airflow)
SPARK_YARN = os.environ.get(
    "LAKEHOUSE_SPARK_YARN",
    "spark-submit --master yarn --deploy-mode client --conf spark.driver.host=airflow --num-executors 1")
# Small checks run in local mode: no YARN containers needed
SPARK_LOCAL = os.environ.get(
    "LAKEHOUSE_SPARK_LOCAL",
    "spark-submit --master local[1] --driver-memory 1g --conf spark.driver.host=airflow")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=60),
}

with DAG(
    dag_id="rees46_daily_gold",
    description="Gold layer for one day, with data-quality gates before and after",
    start_date=datetime(2019, 10, 1),
    # The dataset is HISTORICAL (1-2 Oct 2019). catchup=True + end_date makes the scheduler run exactly those days
    # when you unpause (one at a time). For live data: remove end_date and set catchup=False.
    end_date=datetime(2019, 10, 2),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["rees46", "gold"],
) as dag:

    check_silver = BashOperator(
        task_id="check_silver",
        bash_command=SPARK_LOCAL + " " + JOBS + "/dq_check.py --stage silver --date {{ ds }}",
        pool="yarn",
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=SPARK_YARN + " " + JOBS + "/12_silver_to_gold_rees46.py --from-date {{ ds }} --to-date {{ ds }}",
        pool="yarn",
    )

    check_gold = BashOperator(
        task_id="check_gold",
        bash_command=SPARK_LOCAL + " " + JOBS + "/dq_check.py --stage gold --date {{ ds }}",
        pool="yarn",
    )

    check_silver >> build_gold >> check_gold