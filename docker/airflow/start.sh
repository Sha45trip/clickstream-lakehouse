#!/usr/bin/env bash
# Starts the three Airflow components in one container (lean setup for a laptop).
# The metadata database (airflow-db, Postgres 16) is created by its own container; compose waits for it to be healthy.
set -uo pipefail

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