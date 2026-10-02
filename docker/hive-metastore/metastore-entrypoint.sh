#!/bin/bash
# Idempotent: init schema only if the Postgres DB is empty, then start the metastore.
set -u
export HIVE_CONF_DIR=/opt/hive/conf
if ! "$HIVE_HOME/bin/schematool" -dbType postgres -info >/dev/null 2>&1; then
  echo ">> Initialising metastore schema"
  "$HIVE_HOME/bin/schematool" -dbType postgres -initSchema || { echo "schema init failed"; exit 1; }
fi
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx1G"
exec "$HIVE_HOME/bin/hive" --skiphadoopversion --skiphbasecp --service metastore
