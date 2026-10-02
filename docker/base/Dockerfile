# One image for every Hadoop role (NameNode, DataNode, RM, NM) and the Spark client.
# Java 11 + Python 3.10 everywhere -> no driver/executor version mismatch on YARN.
FROM eclipse-temurin:11-jdk-jammy

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.1

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip curl ca-certificates procps netcat-openbsd less \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz -C /opt && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

# Kafka connector jars baked into Spark's jars dir (uploaded to HDFS by `make init`)
RUN cd /opt/spark/jars \
 && M=https://repo1.maven.org/maven2 \
 && curl -fsSLO $M/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
 && curl -fsSLO $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
 && curl -fsSLO $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSLO $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN useradd -m -u 1001 -s /bin/bash hadoop \
 && mkdir -p /data/hdfs /data/yarn /data/tmp /var/log/hadoop /etc/hadoop/conf \
 && chown -R hadoop:hadoop /data /var/log/hadoop /opt/hadoop-${HADOOP_VERSION} /opt/spark-${SPARK_VERSION}-bin-hadoop3

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/etc/hadoop/conf \
    YARN_CONF_DIR=/etc/hadoop/conf \
    HADOOP_LOG_DIR=/var/log/hadoop \
    HADOOP_ROOT_LOGGER=INFO,console \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:$PATH

COPY entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
USER hadoop
WORKDIR /home/hadoop
ENTRYPOINT ["/entrypoint.sh"]
