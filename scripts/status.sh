#!/usr/bin/env bash
# Health check for the whole stack. Run it after `docker compose up -d` (repeat until everything is OK; ~1-2 min).
# Exit code 0 = everything OK, 1 = something failed.
cd "$(dirname "$0")/.." || exit 1
DC="docker compose"
fail=0

check() {   # check "description" command args...
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then printf "  OK    %s\n" "$name"; else printf "  FAIL  %s\n" "$name"; fail=1; fi
}

echo "Containers:"
for c in namenode datanode resourcemanager nodemanager kafka postgres hive-metastore spark-client airflow-db airflow; do
  check "$c is running" bash -c "[ \"\$(docker inspect -f '{{.State.Running}}' $c 2>/dev/null)\" = true ]"
done

echo "Services:"
check "HDFS is out of safe mode"          bash -c "$DC exec -T namenode hdfs dfsadmin -safemode get | grep -q OFF"
check "HDFS has a live DataNode"          bash -c "$DC exec -T namenode hdfs dfsadmin -report | grep -q 'Live datanodes (1)'"
check "YARN NodeManager is RUNNING"       bash -c "$DC exec -T resourcemanager yarn node -list 2>/dev/null | grep -q RUNNING"
check "Hive metastore answers on :9083"   $DC exec -T spark-client bash -c 'timeout 3 bash -c "echo > /dev/tcp/hive-metastore/9083"'
check "Kafka topic rees46.events.raw"     bash -c "$DC exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -q '^rees46.events.raw\$'"
check "Airflow API on :8080"              bash -c '[ "$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v2/monitor/health)" = 200 ]'
check "Serving database reachable"        $DC exec -T airflow-db psql -U airflow -d serving -c "select 1"

echo "Spark applications currently on YARN (a running stream shows up here; batch jobs need the rest of the 5 GB):"
$DC exec -T resourcemanager yarn application -list 2>/dev/null | grep -E "^Total|application_" | cut -c1-120

echo
if [ "$fail" -eq 0 ]; then echo "All checks passed."; else echo "Some checks failed: wait a minute and run again; if it persists see README > Troubleshooting."; fi
exit $fail
