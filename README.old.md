# Clickstream Lakehouse on Hadoop

Kafka → HDFS → Spark on YARN → Hive-metastore tables (bronze / silver / gold), runnable on a laptop with Docker.

```
 generator ──► Kafka ───────────┐
 (or NDJSON.gz landing files) ──┤
                                ▼
                   [01] Spark Structured Streaming (availableNow, checkpointed)
                                ▼
   BRONZE  bronze.clickstream_raw      raw payload + lineage, Parquet, partitioned by ingest_date
                                ▼
                   [02] parse · validate · dedup · quarantine
                                ▼
   SILVER  silver.events               ORC, partitioned by event_date, deduped on event_id
           silver.events_quarantine    rejects + reject_reasons
                                ▼
                   [03] sessionize (30-min gap) · funnel
                                ▼
   GOLD    gold.sessions · gold.daily_funnel

 Runtime: HDFS (NN + DN) · YARN (RM + NM) · Hive Metastore (Postgres) · Spark 3.5 (client mode, executors on YARN)
```

## 1. Prerequisites

| | |
|---|---|
| Docker Desktop / Engine 24+ with Compose v2 | give Docker **≥ 10 GB RAM**, 4 CPUs, 25 GB disk |
| `make`, `python3` (3.9+) on the host | Windows: use WSL2 and run everything inside it |
| Internet on first build | image build downloads Hadoop 3.3.6, Spark 3.5.1, Kafka connector jars (~1.5 GB) |

Low on RAM? Lower `yarn.nodemanager.resource.memory-mb` in `conf/hadoop/yarn-site.xml` and `spark.executor.instances/memory` in `conf/spark/spark-defaults.conf` together.

## 2. Bring it up

```bash
make build        # ~10 min the first time
make up           # starts 8 containers
make init         # waits for HDFS, creates /lakehouse dirs, uploads Spark jars to HDFS, creates Kafka topic
make smoke        # HDFS report + YARN node list + SparkPi on YARN  -> should print "Pi is roughly 3.14..."
```

UIs: NameNode http://localhost:9870 · YARN http://localhost:8088 · Spark driver http://localhost:4040 (only while a job runs).

## 3. Run the pipeline

Path A, files (fastest to verify):

```bash
make gen-files EPD=200000 DAYS=7     # ~1.4M events -> ./data/landing  (dt= is ARRIVAL date)
make load-landing                    # ./data/landing -> hdfs:///lakehouse/landing
make bronze-files
make silver
make gold
```

Path B, Kafka (the real path):

```bash
pip install -r generator/requirements.txt
make gen-kafka EPD=200000 DAYS=7
make bronze-kafka silver gold
```

`make pipeline-files` / `make pipeline-kafka` run the whole chain. Explore with `make sql` and `sql/sample_queries.sql`.

Scale up for the resume numbers: `make gen-files EPD=20000000 DAYS=7` ≈ 140M events (~1–2 h of generator CPU on a laptop; use `--workers`/`--shards`).
Keep `--users` ≈ `EPD/4` (the Makefile does this).

## 4. What "working" looks like

- `make hdfs-ls P=/lakehouse/silver` shows `events/event_date=2026-09-0X/` ORC files.
- YARN UI shows each job as an application (state FINISHED) with 2 executors.
- `02` prints `bronze rows read | rejected -> quarantine | partitions rewritten`. Rejected ≈ 0.5% of input (injected corrupt records); valid rows ≈ input − rejects − ~1% duplicates.
- Re-run `make silver`: row counts must not change (idempotent). Re-run `make bronze-*`: no new rows (checkpoint).
- `gold.daily_funnel`: session conversion roughly 4–6%, view→cart ≈ 40%.

Local-mode test results for these jobs (90k events, Spark 3.5.1): silver = 89,529 valid / 489 quarantined, matching an independent pure-Python count exactly; silver re-run produced identical counts.

## 5. Design decisions (say these in interviews)

| Decision | Why |
|---|---|
| Bronze stores `raw_payload` as a string + Kafka offset/file path | Immutable raw record; any parsing bug is fixable by replaying bronze, nothing re-read from the source |
| Structured Streaming with `availableNow` + checkpoint | Incremental, exactly-once file sink, scheduled like a batch job (Airflow-friendly) |
| Quarantine table with `reject_reasons` | Bad data is observable and replayable, never silently dropped |
| Dedup on `event_id`, keep earliest `ingest_ts` | Kafka/at-least-once delivery produces duplicates; generator injects 1% to prove it |
| Rebuild affected `event_date` partitions (existing ∪ new) with dynamic partition overwrite | Late events land in old partitions; plain append would duplicate, plain overwrite would lose rows. Phase 3 swaps this for Iceberg `MERGE INTO` |
| Stable `session_id = md5(user_id, session_start)` | Re-runs and backfills regenerate identical keys |
| ORC + zstd for silver/gold, Parquet for bronze | Columnar + partitioned; benchmark against raw JSON in Phase 2 |
| One image (Java 11, Python 3.10) for all Hadoop roles and the Spark client | No driver/executor Java or Python mismatch on YARN |

## 6. Troubleshooting

| Symptom | Fix |
|---|---|
| Spark job stuck at `ACCEPTED` | NodeManager too small for AM + executors. Check http://localhost:8088; reduce executor memory or raise `yarn.nodemanager.resource.memory-mb` |
| `Connection refused ... namenode:8020` right after `make up` | NameNode still starting; wait ~20 s, retry `make init` |
| `Incompatible clusterIDs` in DataNode log | You deleted only one HDFS volume. `make nuke` then start again |
| Hive metastore container restarts | `make logs S=hive-metastore`; usually Postgres not ready yet (it self-heals) or a typo in `conf/hive-metastore/hive-site.xml` |
| `Invalid method name` / metastore thrift errors from Spark | Spark's built-in Hive 2.3.9 client vs Hive 3.1.3 metastore. Add to `spark-defaults.conf`: `spark.sql.hive.metastore.version 3.1.3` and `spark.sql.hive.metastore.jars maven` (first run downloads jars) |
| `Cannot overwrite a path that is also being read from` | Don't remove the `checkpoint(eager=True)` in job 02 |
| Executors can't reach the driver | `spark.driver.host=spark-client` and ports 7078/7079 must stay as in `spark-defaults.conf` |
| Kafka producer from host can't connect | Host uses `localhost:29092`; containers use `kafka:9092` (two listeners by design) |

## 7. Layout

```
docker-compose.yml        8 services on network "lake"
docker/base               Hadoop 3.3.6 + Spark 3.5.1 image (all roles)
docker/hive-metastore     Hive 3.1.3 + Postgres JDBC, idempotent schema init
conf/hadoop|spark|hive-metastore   plain XML / properties, mounted read-only
generator/gen_clickstream.py       synthetic events: funnel, skew, late, duplicate, corrupt
jobs/01_ingest_bronze.py · 02_bronze_to_silver.py · 03_silver_to_gold.py · common.py
sql/sample_queries.sql
Makefile                  every command above
```

## 8. Roadmap (next phases)

1. **Orchestration + DQ:** Airflow DAG (`ingest >> silver >> gold`, retries, SLA, backfill by date); Great Expectations/Deequ checks gating gold.
2. **Serving + benchmark:** Spark Thrift Server → Superset dashboards; benchmark JSON vs Parquet vs ORC, partitioned vs not, before/after compaction (this produces the resume numbers).
3. **Lakehouse:** Apache Iceberg on the same HDFS + metastore: `MERGE INTO` for silver, schema evolution, time travel, small-file compaction.
4. **Ops:** Prometheus + Grafana (NN/DN/YARN/Kafka lag), runbook for DataNode / NodeManager loss.
