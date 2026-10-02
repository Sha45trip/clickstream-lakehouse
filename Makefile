# Usage: make <target>.   On Windows run everything inside WSL2.
SHELL := /bin/bash
DC    := docker compose
SUBMIT := $(DC) exec -T spark-client spark-submit
START ?= 2026-09-01
DAYS  ?= 7
EPD   ?= 200000

.PHONY: help build up down nuke ps logs init smoke gen-files gen-kafka load-landing \
        bronze-files bronze-kafka silver gold pipeline-files pipeline-kafka sql shell-spark hdfs-ls

help:            ## list targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*## /\t/'

build:           ## build images (first time ~10 min: downloads Hadoop + Spark tarballs)
	$(DC) build

up:              ## start the cluster
	$(DC) up -d
	@echo "NameNode http://localhost:9870 | YARN http://localhost:8088 | Spark UI http://localhost:4040 (while a job runs)"

down:            ## stop (data kept in volumes)
	$(DC) down

nuke:            ## stop AND delete all volumes (HDFS, Kafka, metastore)
	$(DC) down -v

ps:              ## container status
	$(DC) ps

logs:            ## tail logs: make logs S=namenode
	$(DC) logs -f --tail=100 $(S)

init:            ## one-time: HDFS dirs, Spark jars -> HDFS, Kafka topic
	@echo "waiting for HDFS (NameNode + 1 live DataNode)..."
	@until $(DC) exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (1)"; do sleep 3; done
	$(DC) exec -T namenode hdfs dfsadmin -safemode wait
	$(DC) exec -T namenode hdfs dfs -mkdir -p /lakehouse/landing /lakehouse/bronze /lakehouse/silver /lakehouse/gold \
	    /lakehouse/_checkpoints /user/hive/warehouse /spark/jars /spark-logs /tmp/logs
	$(DC) exec -T spark-client bash -c 'hdfs dfs -put -f /opt/spark/jars/* /spark/jars/'
	$(DC) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
	    --topic clickstream.raw --partitions 6 --replication-factor 1
	@echo "init done"

smoke:           ## verify HDFS + YARN + Spark-on-YARN end to end (computes Pi on the cluster)
	$(DC) exec -T namenode hdfs dfsadmin -report | head -12
	$(DC) exec -T resourcemanager yarn node -list
	$(SUBMIT) --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 50

# ---------------------------------------------------------------- data
gen-files:       ## generate synthetic events to ./data/landing (START, DAYS, EPD overridable)
	python3 generator/gen_clickstream.py --start-date $(START) --days $(DAYS) --events-per-day $(EPD) \
	    --users $$(( $(EPD) / 4 )) --sink files --out data/landing

gen-kafka:       ## produce the same events into Kafka (needs: pip install -r generator/requirements.txt)
	python3 generator/gen_clickstream.py --start-date $(START) --days $(DAYS) --events-per-day $(EPD) \
	    --users $$(( $(EPD) / 4 )) --sink kafka --bootstrap localhost:29092

load-landing:    ## copy ./data/landing into HDFS /lakehouse/landing
	$(DC) exec -T namenode bash -c 'hdfs dfs -put -f /host-data/landing/* /lakehouse/landing/'

# ---------------------------------------------------------------- pipeline
bronze-files:    ## landing files -> bronze
	$(SUBMIT) /opt/jobs/01_ingest_bronze.py --source files
bronze-kafka:    ## Kafka -> bronze
	$(SUBMIT) /opt/jobs/01_ingest_bronze.py --source kafka
silver:          ## bronze -> silver (today's ingest_date; use ARGS="--all" for everything)
	$(SUBMIT) /opt/jobs/02_bronze_to_silver.py $(ARGS)
gold:            ## silver -> gold for the generated date range
	$(SUBMIT) /opt/jobs/03_silver_to_gold.py --from-date $(START) \
	    --to-date $$(python3 -c "from datetime import date,timedelta; print(date.fromisoformat('$(START)')+timedelta(days=$(DAYS)))")

pipeline-files: gen-files load-landing bronze-files silver gold   ## full run via files
pipeline-kafka: gen-kafka bronze-kafka silver gold                ## full run via Kafka

# ---------------------------------------------------------------- explore
sql:             ## interactive Spark SQL on YARN
	$(DC) exec spark-client spark-sql
shell-spark:     ## shell in the Spark client container
	$(DC) exec spark-client bash
hdfs-ls:         ## tree of the lake: make hdfs-ls P=/lakehouse/silver
	$(DC) exec -T namenode hdfs dfs -ls -R $${P:-/lakehouse} | head -60
