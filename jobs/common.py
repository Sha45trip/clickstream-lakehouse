"""Shared helpers for the pipeline jobs."""
import os
from pyspark.sql import SparkSession

# Root of the lake. On the cluster this is HDFS; override LAKE_ROOT to run locally (file:///tmp/lake).
LAKE_ROOT = os.environ.get("LAKE_ROOT", "hdfs://namenode:8020/lakehouse").rstrip("/")

BRONZE_PATH = f"{LAKE_ROOT}/bronze/clickstream_raw"
SILVER_EVENTS_PATH = f"{LAKE_ROOT}/silver/events"
SILVER_QUARANTINE_PATH = f"{LAKE_ROOT}/silver/events_quarantine"
GOLD_SESSIONS_PATH = f"{LAKE_ROOT}/gold/sessions"
GOLD_FUNNEL_PATH = f"{LAKE_ROOT}/gold/daily_funnel"
CHECKPOINT_ROOT = f"{LAKE_ROOT}/_checkpoints"

VALID_EVENT_TYPES = ["page_view", "search", "product_view", "add_to_cart",
                     "remove_from_cart", "begin_checkout", "purchase"]


def get_spark(app_name: str) -> SparkSession:
    """Cluster-wide settings come from conf/spark/spark-defaults.conf; the two below are
    repeated so the jobs also work when run locally without that file."""
    return (SparkSession.builder.appName(app_name)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
            .enableHiveSupport()
            .getOrCreate())
