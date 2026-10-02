# Airflow 3 on top of the Hadoop+Spark image, so spark-submit, the Hadoop client and Python 3.10 are already here.
# Airflow lives in its own virtualenv: nothing in the system Python (used by Spark) is touched.
FROM lakehouse/hadoop-spark:1.0

USER root
ARG AIRFLOW_VERSION=3.3.2
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-venv python3-dev gcc \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/af-venv \
 && /opt/af-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/af-venv/bin/pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" psycopg2-binary \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"

COPY start.sh /start.sh
RUN chmod +x /start.sh \
 && mkdir -p /opt/airflow/dags /opt/airflow/logs \
 && chown -R hadoop:hadoop /opt/airflow

# venv bin goes LAST on PATH: `python3` stays the system Python (what Spark uses), `airflow` is found in the venv.
ENV AIRFLOW_HOME=/opt/airflow \
    PATH=$PATH:/opt/af-venv/bin

USER hadoop
WORKDIR /home/hadoop
ENTRYPOINT ["/start.sh"]