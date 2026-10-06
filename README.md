# Clickstream Lakehouse on Hadoop

Real e-commerce clickstream (REES46, 42M events/month) → Kafka → Spark Structured Streaming on YARN → HDFS lakehouse
(bronze / silver / gold, Hive Metastore) → Airflow-orchestrated gold with data-quality gates → serving Postgres.
Runs on one laptop with Docker (WSL2 on Windows).

```
 REES46 CSV ──► replayer (paced) ──► Kafka ──► [10] bronze stream ──► bronze.rees46_raw      raw + lineage, Parquet
                                                    │
                                                    ▼
                                      [11] silver stream: parse · validate · map · 24 h dedup (RocksDB state)
                                                    │          │         └──► silver.rees46_quarantine (rejects + reason)
                                                    │          └──► serving.live_minute (events + revenue per minute)
                                                    ▼
                                           silver.rees46_events                              ORC, partitioned by event_date
                                                    │
        (10 + 11 + live aggregates run as ONE Spark application: jobs/20_streaming_app.py)
                                                    │
                          Airflow DAG rees46_daily_gold (per day):
                          check_silver ► [12] build_gold ► check_gold ► [13] publish_serving
                                                    │                                   │
                                                    ▼                                   ▼
                          gold.rees46_sessions / daily_funnel / category_daily     serving Postgres (dashboards, Phase 4)
```

| Phase | Status |
|---|---|
| 1. Real data + Kafka replayer (deterministic event ids, paced replay) | Done |
| 2. Continuous bronze + silver streams, gold, Airflow with quality gates | Done |
| 2b. Serving database + atomic publish step | Done |
| 4a. One streaming app with live per-minute aggregates; refresh DAG every 10 min | Done (new, see 4.A and 4.B) |
| 4b. Superset dashboard + measured end-to-end latency | **Next** |
| 3. Apache Iceberg (`MERGE INTO`, closes the crash-duplicate gap in silver) | Planned |
| Later: tests + CI, observability (Prometheus/Grafana), 100M+ scale benchmark | Planned |

---

## 1. Daily runbook

### Start (cold start: PC was off, or you closed everything)

1. Start **Docker Desktop** and wait until it says *Engine running*.
2. Open **Ubuntu** (WSL) and run:

```bash
cd ~/projects/clickstream-lakehouse
source .venv/bin/activate          # needed only for the replayer (host-side Python)
docker compose up -d               # starts all 10 containers; data is kept in Docker volumes
./scripts/status.sh                # health check; repeat every ~30 s until it prints "All checks passed."
```

Expect 1 to 2 minutes: HDFS leaves safe mode first, then the NodeManager registers, then the Hive metastore and Airflow
come up. A check that shows `FAIL` right after start is normal; one that stays `FAIL` after ~3 minutes is not
(see Troubleshooting).

UIs: NameNode http://localhost:9870 · YARN http://localhost:8088 · Airflow http://localhost:8080 ·
Spark driver http://localhost:4040 (only while a job runs).

### Shut down (end of the day)

```bash
# 1. stop streaming jobs: press Ctrl+C in their terminals and wait for "[bronze] rows in table" / "[silver] rows in silver"
# 2. confirm nothing is left running on YARN (expect "Total number of applications ...:0")
docker compose exec resourcemanager yarn application -list
#    if one is listed:  docker compose exec resourcemanager yarn application -kill <application_id>

# 3. stop all containers gracefully (data is kept)
docker compose stop
```

Optional, to give RAM back to Windows: in **PowerShell** run `wsl --shutdown`.

> **Never run `docker compose down -v`** unless you want to erase everything (HDFS, Kafka, metastore, Airflow history,
> serving data). `docker compose down` (without `-v`) is safe, and `stop` is the gentler habit.

A hard stop (power loss, killed terminal) is survivable: the streams resume from their checkpoints, and silver's dedup
removes any batch that was written twice.

### What survives a restart

| Kept (Docker volumes) | Lost |
|---|---|
| HDFS (bronze/silver/gold, checkpoints), Kafka topics, Hive metastore, Airflow history, serving database | Anything running in a terminal; unfinished YARN applications |

---

## 2. Resource rules (read once)

WSL has about 11 GiB (`C:\Users\<you>\.wslconfig`: `memory=12GB`, then `wsl --shutdown`). YARN has **5 GB** for Spark.

| Workload | Cost on YARN |
|---|---|
| One streaming job (AM + 1 executor) | about 2.5 GB |
| Streaming app `20_streaming_app.py` (AM 1 GB + 1 executor of 2 GB) | about 3 GB |
| Two separate streams (jobs 10 + 11) | about 5 GB, which is the whole node: do not run them together with anything else |
| A batch job with 2 executors | about 4 GB |

So: **run the one streaming app (`20`) and nothing else on YARN.** The 10-minute refresh DAG runs its Spark jobs in
local mode inside the Airflow container, so it needs no YARN room. The daily DAG's `build_gold` uses YARN: do not run it
while the streaming app is running. Ad-hoc SQL always runs in local mode (`--master local[1]`).

---

## 3. First-time setup (once)

Prerequisites: Docker Desktop with WSL2 integration, `make`, Python 3.9+, at least 12 GB RAM for WSL, 25 GB disk.

```bash
cd ~/projects/clickstream-lakehouse
python3 -m venv .venv && source .venv/bin/activate && pip install confluent-kafka

make build                 # ~10 min: images with Hadoop 3.3.6 + Spark 3.5.1 (+ Hive metastore, Airflow 3.3.2)
make up
make init                  # HDFS dirs, Spark jars to HDFS, topic clickstream.raw (synthetic path)
make smoke                 # SparkPi on YARN must print "Pi is roughly 3.14..."

# topic for the real data
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic rees46.events.raw --partitions 6 --replication-factor 1

# serving database (inside the airflow-db container) + schema
docker compose exec -T airflow-db psql -U airflow -d airflow -tc "SELECT 1 FROM pg_database WHERE datname='serving'" | grep -q 1 \
  || docker compose exec -T airflow-db psql -U airflow -d airflow -c "CREATE DATABASE serving"
docker compose exec -T airflow-db psql -U airflow -d serving < sql/serving_schema.sql   # idempotent; also creates the live_* tables

# pool for the 10-minute refresh DAG (one local Spark job at a time)
docker compose exec airflow airflow pools set local_spark 1 "one local Spark job at a time"
```

### Data (not in the repo)

Download the REES46 *"eCommerce behavior data from multi category store"* from Kaggle
(`mkechinov/ecommerce-behavior-data-from-multi-category-store`; check its licence), extract `2019-Oct.csv` into
`data/raw/`, then cut the two development slices:

```bash
F=data/raw/2019-Oct.csv
mkdir -p data/sample
head -n 1000001 $F > data/sample/oct_first_1m.csv
(head -n 1 $F; sed -n '1000002,2000001p;2000001q' $F) > data/sample/oct_second_1m.csv
```

`data/` is git-ignored. Never commit data files.

---

## 4. Running the pipeline

### A. Real data, streaming (2 terminals)

```bash
# Terminal 1: ONE application = bronze + silver + quarantine + live aggregates. Runs in the airflow container
# (it has the Postgres driver the live aggregates need). Do not run jobs 10 / 11 at the same time: same checkpoints.
docker compose exec airflow spark-submit --master yarn --deploy-mode client \
  --conf spark.driver.host=airflow --num-executors 1 --executor-cores 2 --executor-memory 1536m --driver-memory 1g \
  /opt/jobs/20_streaming_app.py

# Terminal 2: replay events into Kafka (1 = real time, 60 = 60x faster)
source .venv/bin/activate
python generator/replay_rees46.py --file data/sample/oct_third_1m.csv --speedup 60
```

Cut the next slice of the file for a live demo (events 2,000,001 to 3,000,000):
`(head -n 1 $F; sed -n '2000002,3000001p;3000001q' $F) > data/sample/oct_third_1m.csv`

Press Ctrl+C in terminal 1 to stop; it resumes from its checkpoints. If the live write to Postgres fails, the log shows
`LIVE WRITE FAILED` and the pipeline keeps going (the live tiles may miss that batch; gold is the exact source).
**Never replay events whose event time is more than 24 h ahead of the stream's newest event** (for example November
after October): the watermark would jump forward and the older events that follow would be dropped silently.
The standalone jobs `10_stream_bronze.py` and `11_stream_silver.py` still work, but use one or the other, not both
together with `20`.

Watch it live:

```bash
watch -n 10 "docker compose exec -T airflow-db psql -U airflow -d serving -c \"SELECT * FROM live_status\" -c \"SELECT minute_ts, event_type, events FROM live_minute ORDER BY minute_ts DESC, event_type LIMIT 9\""
```

### B. Gold, via Airflow (stop the streams first)

Open http://localhost:8080, find the DAG `rees46_daily_gold`, and unpause it. It runs 1 and 2 Oct 2019 (the days
loaded), one after the other. Each run is `check_silver ► build_gold ► check_gold ► publish_serving`. Task logs show the
`[dq]` gate lines, the session validation report and the `[publish]` read-back.

- Re-run a day: UI, open the run, then **Clear**.
- Extend to more days: load more data first, then raise `end_date` in `dags/rees46_daily_gold.py`.
- Manual gold without Airflow:
  ```bash
  docker compose exec spark-client spark-submit /opt/jobs/12_silver_to_gold_rees46.py \
    --from-date 2019-10-01 --to-date 2019-10-02 --gap-seconds 1800
  ```

### B2. Gold refresh every 10 minutes (speed layer)

DAG `rees46_gold_refresh` (created paused; unpause it while the streaming app runs):
`find_day -> build_gold -> publish_serving`. `find_day` asks `serving.live_status` which event day is "now" and **skips**
the run if nothing new arrived for 15 minutes. `build_gold` runs job 12 for that single day in local mode (2 cores, 2 GB).
These numbers are provisional: there is no reconciliation gate (silver keeps growing while gold is built); the daily DAG
rebuilds each settled day with the gates and overwrites them.

```bash
docker compose exec airflow airflow dags unpause rees46_gold_refresh
```

### C. Looking at results

```bash
# Spark SQL in local mode (slow over many small files; full counts take minutes)
docker compose exec spark-client spark-sql --master local[1] --conf spark.driver.memory=1g -e "
SELECT session_date, sessions, sessions_purchase, revenue, session_conversion_rate
FROM gold.rees46_daily_funnel ORDER BY 1"

# Serving Postgres (instant)
docker compose exec airflow-db psql -U airflow -d serving -c \
  "SELECT session_date, sessions, sessions_purchase, revenue FROM daily_funnel ORDER BY 1"
```

Reference numbers for the loaded data (1800 s session gap), to sanity-check a rebuild:

| Check | Expected |
|---|---|
| silver rows / unique `event_id` | 2,000,000 / 2,000,000 |
| silver `product_view` / `purchase` / `add_to_cart` | 1,936,686 / 33,877 / 29,437 (equal to the CSV's `view` / `purchase` / `cart` for the first 2M events) |
| quarantine | empty (the real data passes all rules) |
| gold sessions, 1 Oct / 2 Oct | 227,254 / 152,637 |
| revenue (silver = gold sessions = category_daily) | 10,923,294.64 |

### D. Legacy synthetic path (kept for tests)

`make pipeline-files` runs the original synthetic generator through jobs `01` to `03` (tables
`bronze.clickstream_raw`, `silver.events`, `gold.sessions`). `make help` lists all Makefile targets.

---

## 5. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Spark job stays `ACCEPTED` forever | YARN has no usable node or no room. `docker compose exec resourcemanager yarn node -list -all`: empty means `docker compose restart nodemanager`; `UNHEALTHY` means disk space (`df -h /`); `RUNNING` means another app holds the 5 GB (`yarn application -list`, stop a stream) |
| `/opt/jobs` or `/opt/airflow/dags` is empty inside a container | Stale bind mount after a WSL restart. `docker compose up -d --force-recreate --no-deps airflow` (or `spark-client`) |
| `hive-metastore` restarting, log says *authentication type 10 not supported* | Postgres 14+ uses SCRAM, the Hive JDBC driver does not. The shared `postgres` service must stay at **13** |
| Airflow container loops with `No module named 'asyncpg'` | Image built without the Postgres extra: the Dockerfile must install `apache-airflow[postgres]` |
| Airflow `Connection refused` in a task log | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` must point at the api-server port (8080 in compose) |
| Task fails: `/opt/jobs/dq_check.py: No such file` | File not in `jobs/` on the host, or a stale mount (row above) |
| HDFS stuck in safe mode after start | Wait about a minute; `docker compose exec namenode hdfs dfsadmin -safemode get` |
| `Incompatible clusterIDs` in the DataNode log | Only one HDFS volume was deleted. Delete both (`docker compose down -v`, which loses data) |
| Container killed with exit code 137, or heavy swapping | Out of memory. Stop streams, use `--num-executors 1`, raise `memory=` in `.wslconfig` |
| WARN `Caught Hive MetaException ... Falling back` plus a long stack trace | Harmless: Hive 3 cannot filter DATE partition columns, so Spark prunes them itself |
| `Service 'sparkDriver' could not bind on port 7078` in `spark-sql` | Harmless: another driver is running, and Spark picks the next port |
| Queries over silver or bronze take minutes | Many small files from 10 to 30 s micro-batches (known; compaction comes with Iceberg) |
| `Task Instance not found` or odd Airflow state | `docker compose restart airflow` |
| Streaming app crashes with `Invalid batch ... != ...` on the first run | Fixed in `20_streaming_app.py` (`ingest_date` declared in the bronze schema); make sure you run the current file |
| Refresh DAG always skipped | `SELECT * FROM live_status` is empty or older than 15 min: the streaming app is not running or Postgres writes fail (look for `LIVE WRITE FAILED`) |
| Daily DAG `build_gold` stuck in `ACCEPTED` while the stream runs | The stream holds 3 GB of the 5 GB YARN node. Stop the stream before running the daily DAG |

`./scripts/status.sh` checks the whole stack in one go.

---

## 6. Layout

```
docker-compose.yml        10 services on network "lake"
docker/base               Hadoop 3.3.6 + Spark 3.5.1 image (every Hadoop role + Spark client)
docker/hive-metastore     Hive 3.1.3 (Java 8) + Postgres driver
docker/airflow            Airflow 3.3.2 in a venv on top of the base image; add_airflow_service.py and
                          patch_dag_publish.py are one-off patch scripts
conf/hadoop|spark|hive-metastore   plain XML / properties, mounted read-only
generator/                gen_clickstream.py (synthetic) · replay_rees46.py (REES46 -> Kafka, paced)
jobs/                     20_streaming_app (bronze+silver+live) · 10/11 (standalone variants) · 12_silver_to_gold_rees46 · 13_publish_serving
                          dq_check (quality gates) · common · 01-03 (legacy synthetic batch)
dags/                     rees46_daily_gold.py (verified daily) · rees46_gold_refresh.py (every 10 min)
sql/                      serving_schema.sql · sample_queries.sql
scripts/status.sh         stack health check
data/                     raw/ and sample/ (git-ignored)
```

Dev credentials (laptop only, never reuse): shared Postgres `hive`/`hivepass`; `airflow-db` `airflow`/`airflow`
(databases `airflow` and `serving`); the Airflow UI has no login.

---

## 7. Design notes and findings

- **Event ids.** The source has none. `event_id = sha1(row)[:20] + "-" + n`, where n is an occurrence counter within the
  same second. Deterministic, so replays are idempotent, and identical source rows stay distinct.
- **Bronze = raw string + lineage.** Parsing bugs can be fixed by replaying bronze.
- **At-least-once writes.** `kill -9` on the bronze stream lost nothing but wrote 14,341 rows twice (0.48%). Silver's
  dedup removes them. A crash of the silver stream can leave one batch duplicated in silver (Iceberg `MERGE` closes this).
- **24 h dedup window** (RocksDB state): a duplicate is removed only while its first copy is within 24 h of the newest
  event time. **A first-time event older than that window is dropped silently** (tested in isolation on Spark 3.5.1:
  a 6-hours-late event is kept, a 3-days-late event is not). Pair with a periodic reconciliation job
  (distinct valid ids in bronze vs rows in silver) in production.
- **Live aggregates** are best effort and exactly-once per micro-batch (ledger table `live_batches`); gold is the exact source.
- **Speed layer vs batch layer.** The 10-minute refresh is provisional; the daily DAG with its gates overwrites it.
- **Quality gates in Airflow.** `check_silver` (rows, unique ids, nulls, negative prices) blocks the build;
  `check_gold` reconciles purchases and revenue against silver to the cent; `publish_serving` writes atomically and
  reads back.
- **Sessions.** 30-minute inactivity gap. The dataset's own `user_session` ids are not reproducible by any gap rule
  (sweep 300 to 1800 s: total mismatch stays between 14.7% and 16.8%, and sessions of ours that merge several source
  sessions never drop below 7.9%). They are kept in silver as `source_session_id`, and our definition is documented.
- **Cart tracking gap.** About 64% of purchase sessions have no cart event, so cart-to-purchase is not a trustworthy
  metric; view-to-purchase (6.7 to 7.6% per day) is. Cart coverage is a soft quality check.
- **Money.** Spark sums of doubles differ in the last digit depending on executor count, so the serving layer stores
  `NUMERIC(18,2)`.

### What changes in production

| Here (laptop) | Production |
|---|---|
| 1 NameNode, 1 DataNode, replication 1 | HA NameNode, at least 3 DataNodes, replication 3 |
| 1 Kafka broker | At least 3 brokers, replication factor 3 |
| HDFS permissions off, no auth | Kerberos + Ranger |
| Airflow LocalExecutor, no login | Celery or Kubernetes executor, SSO |
| Replay of a historical file | Live producers |

---

## 8. Roadmap

1. **Phase 4b (next):** Superset dashboard on the serving DB; a measured end-to-end latency number; a reconciliation
   check for late-dropped events.
2. **Iceberg:** transactional silver (`MERGE INTO`), compaction of small files, schema evolution, time travel.
3. **Quality of life:** pytest + CI, Prometheus/Grafana (Kafka lag, batch durations), 100M+ events and a format benchmark.
