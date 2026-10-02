#!/usr/bin/env bash
# Starts the three Airflow components in one container (lean setup for a laptop).
set -uo pipefail

echo ">> waiting for Postgres and creating the 'airflow' database if needed"
/opt/af-venv/bin/python - <<'PY'
import sys, time
import psycopg2
conn = None
for _ in range(40):
    try:
        conn = psycopg2.connect(host="postgres", user="hive", password="hivepass", dbname="metastore")
        break
    except Exception as e:
        time.sleep(3)
if conn is None:
    sys.exit("postgres not reachable")
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
if cur.fetchone() is None:
    cur.execute("CREATE DATABASE airflow")
    print("created database airflow")
else:
    print("database airflow already exists")
PY
[ $? -eq 0 ] || exit 1

echo ">> migrating the Airflow metadata database"
airflow db migrate || exit 1
airflow pools set yarn 1 "one Spark application at a time (YARN node is 5 GB)"

echo ">> starting dag-processor, scheduler, api-server"
airflow dag-processor &
airflow scheduler &
airflow api-server --port 8080 &

# If any component dies, stop the container so Docker's restart policy brings everything back cleanly.
wait -n
echo "an Airflow component exited; stopping container" >&2
exit 1