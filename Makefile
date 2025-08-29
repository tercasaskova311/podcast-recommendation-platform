# Makefile for managing Big Data platform services

KAFKA_PATH=./docker/kafka
MONGO_PATH=./docker/mongodb
SPARK_PATH=./docker/spark
AIRFLOW_PATH=./docker/airflow

KAFKA_BIN=/opt/kafka/bin

# Load env vars from .env.development
ifneq (,$(wildcard .env.development))
  include .env.development
  export $(shell sed 's/=.*//' .env.development)
endif

.PHONY: up down restart logs status \
        kafka-up kafka-down kafka-logs kafka-status kafka-restart kafka-eval \
        create-topics list-topics \
        mongo-up mongo-down mongo-logs mongo-status mongo-shell \
        build-spark-image spark-up spark-down spark-logs spark-status \
        spark-up-metadata spark-up-transcripts-en spark-up-summary \
        build-airflow-image airflow-up airflow-down airflow-logs init

# --- Kafka Commands ---
kafka-up:
	docker compose --env-file .env.development -f $(KAFKA_PATH)/docker-compose.yml up -d

kafka-down:
	docker compose -f $(KAFKA_PATH)/docker-compose.yml down

kafka-restart:
	$(MAKE) kafka-down && $(MAKE) kafka-up

kafka-logs:
	docker compose -f $(KAFKA_PATH)/docker-compose.yml logs -f

kafka-status:
	docker compose -f $(KAFKA_PATH)/docker-compose.yml ps

kafka-eval:
	docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 bash

create-topics:
	# EPISODE METADATA (3x RF, 3 partitions)
	docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 bash -lc '\
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
		  --create --if-not-exists \
		  --topic "$(TOPIC_EPISODE_METADATA)" \
		  --partitions 3 --replication-factor 3' && \
	docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 bash -lc '\
		/opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka1:9092 --alter \
		  --topic "$(TOPIC_EPISODE_METADATA)" \
		  --add-config min.insync.replicas=2'

	# EPISODES ID (compacted; optional: infinite time retention)
	docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 bash -lc '\
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
		  --create --if-not-exists \
		  --topic "$(TOPIC_EPISODES_ID)" \
		  --partitions 3 --replication-factor 3' && \
	docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 bash -lc '\
		/opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka1:9092 --alter \
		  --topic "$(TOPIC_EPISODES_ID)" \
		  --add-config cleanup.policy=compact,min.insync.replicas=2,retention.ms=-1'

	# USER EVENTS (6 partitions, 7 days)
	docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 bash -lc '\
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
		  --create --if-not-exists \
		  --topic "$(TOPIC_USER_EVENTS_STREAMING)" \
		  --partitions 6 --replication-factor 3' && \
	docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 bash -lc '\
		/opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka1:9092 --alter \
		  --topic "$(TOPIC_USER_EVENTS_STREAMING)" \
		  --add-config retention.ms=604800000,min.insync.replicas=2'


list-topics:
	docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 bash -lc '\
		$(KAFKA_BIN)/kafka-topics.sh --bootstrap-server kafka1:9092 --list'

# --- Mongo Commands ---
mongo-up:
	docker compose --env-file .env.development -f $(MONGO_PATH)/docker-compose.yml up -d

mongo-down:
	docker compose -f $(MONGO_PATH)/docker-compose.yml down

mongo-logs:
	docker compose -f $(MONGO_PATH)/docker-compose.yml logs -f

mongo-status:
	docker compose -f $(MONGO_PATH)/docker-compose.yml ps

mongo-shell:
	docker exec -it mongodb mongosh

# -- Spark Commands ---
build-spark-image:
	docker build -t $(SPARK_IMAGE_NAME):$(SPARK_IMAGE_TAG) $(SPARK_PATH)/

spark-up:
	@$(MAKE) build-spark-image
	docker compose --env-file .env.development -f $(SPARK_PATH)/docker-compose.yml up -d --build

spark-down:
	docker compose -f $(SPARK_PATH)/docker-compose.yml down

spark-logs:
	docker compose -f $(SPARK_PATH)/docker-compose.yml logs -f

spark-status:
	docker compose -f $(SPARK_PATH)/docker-compose.yml ps

# -- Airflow Commands ---
build-airflow-image:
	docker build -t $(AIRFLOW_IMAGE_NAME):$(AIRFLOW_IMAGE_TAG) $(AIRFLOW_PATH)/

airflow-up:
	@$(MAKE) build-airflow-image
	docker compose --env-file .env.development -f $(AIRFLOW_PATH)/docker-compose.yml up -d --build

airflow-down:
	docker compose -f $(AIRFLOW_PATH)/docker-compose.yml down

airflow-logs:
	docker compose -f $(AIRFLOW_PATH)/docker-compose.yml logs -f

# --- General Aggregate Commands ---
up: kafka-up mongo-up spark-up airflow-up

down: mongo-down spark-down airflow-down kafka-down

restart: down up

logs:
	@echo "\n--- Kafka Logs ---"
	docker compose -f $(KAFKA_PATH)/docker-compose.yml logs --tail=20
	@echo "\n--- Mongo Logs ---"
	docker compose -f $(MONGO_PATH)/docker-compose.yml logs --tail=20
	@echo "\n--- Spark Logs ---"
	docker compose -f $(SPARK_PATH)/docker-compose.yml logs --tail=20
	@echo "\n--- Airflow Logs ---"
	docker compose -f $(AIRFLOW_PATH)/docker-compose.yml logs --tail=20

status:
	@echo "\n--- Kafka Status ---"
	docker compose -f $(KAFKA_PATH)/docker-compose.yml ps
	@echo "\n--- Mongo Status ---"
	docker compose -f $(MONGO_PATH)/docker-compose.yml ps
	@echo "\n--- Spark Status ---"
	docker compose -f $(SPARK_PATH)/docker-compose.yml ps

# --- Full Project Initialization ---
init:
	@$(MAKE) up
	@docker compose -f $(SPARK_PATH)/docker-compose.yml build
	@echo "Waiting for Kafka controller quorum..."
	@until docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 \
		bash -lc '$(KAFKA_BIN)/kafka-metadata-quorum.sh --bootstrap-server kafka1:9092 describe --status >/dev/null 2>&1'; do \
		echo "Kafka not ready, waiting..."; \
		sleep 2; \
	done
	@$(MAKE) create-topics
	@$(MAKE) status
