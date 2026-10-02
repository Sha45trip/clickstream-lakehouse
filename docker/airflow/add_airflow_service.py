"""Adds (or replaces) the Airflow services in docker-compose.yml. Safe to run more than once."""
import base64, os, re, sys

p = "docker-compose.yml"
s = open(p).read()

# 1) remove any earlier version of the Airflow block and its volumes
s = re.sub(r"\n  # -+ Airflow 3.*?(?=\nnetworks:\n  lake:)", "", s, flags=re.S)
s = s.replace("\n  airflow-logs:", "").replace("\n  airflow-db-data:", "")

fernet = base64.urlsafe_b64encode(os.urandom(32)).decode()     # dev-only encryption key for Airflow connections
svc = f'''
  # ------------------------------------------------------------------ Airflow 3 (scheduler + dag-processor + api-server in one container)
  # Own Postgres 16: Airflow 3.3 is tested with PostgreSQL 14-18, and the shared 'postgres' service is 13 (pinned for Hive).
  airflow-db:
    image: postgres:16-alpine
    container_name: airflow-db
    restart: unless-stopped
    networks: [lake]
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - airflow-db-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow -d airflow"]
      interval: 5s
      timeout: 3s
      retries: 20

  airflow:
    build: ./docker/airflow
    image: lakehouse/airflow:1.0
    container_name: airflow
    hostname: airflow                  # Spark executors on YARN connect back to the driver by this name
    restart: unless-stopped
    networks: [lake]
    ports: ["8080:8080"]               # Airflow UI
    depends_on:
      airflow-db: {{ condition: service_healthy }}
      namenode: {{ condition: service_started }}
      resourcemanager: {{ condition: service_started }}
      hive-metastore: {{ condition: service_started }}
    environment:
      LAKE_ROOT: hdfs://namenode:8020/lakehouse
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__PARALLELISM: "2"
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://localhost:8080/execution/
      # DEV-ONLY secrets and no login. Never reuse these or expose this port outside your laptop.
      AIRFLOW__CORE__FERNET_KEY: "{fernet}"
      AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "True"
      AIRFLOW__API_AUTH__JWT_SECRET: "dev-only-jwt-secret-dev-only-jwt-secret-dev-only-jwt-secret-0123456789"
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-db:5432/airflow
      AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: "30"
    volumes:
      - ./dags:/opt/airflow/dags:ro
      - ./jobs:/opt/jobs:ro
      - ./conf/hadoop:/etc/hadoop/conf:ro
      - ./conf/spark:/opt/spark/conf:ro
      - airflow-logs:/opt/airflow/logs
'''
anchor = "\nnetworks:\n  lake:"
if anchor not in s or "\n  pg-data:" not in s:
    sys.exit("could not find the expected places in docker-compose.yml; send me the file")
s = s.replace(anchor, svc + anchor, 1).replace("\n  pg-data:", "\n  pg-data:\n  airflow-logs:\n  airflow-db-data:", 1)
open(p, "w").write(s)
print("airflow + airflow-db services written")