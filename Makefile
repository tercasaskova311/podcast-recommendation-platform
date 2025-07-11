# Makefile for managing Big Data platform services

KAFKA_PATH=./docker/kafka
MONGO_PATH=./docker/mongodb
SPARK_PATH=./docker/spark
AIRFLOW_PATH=./docker/airflow

# Load env vars from .env.development
ifneq (,$(wildcard .env.development))
  include .env.development
  export $(shell sed 's/=.*//' .env.development)
endif

.PHONY: up down restart logs status \
        kafka-up kafka-down kafka-logs kafka-status kafka-restart kafka-eval \
        create-topic list-topics \
		mongo-up mongo-down mongo-logs mongo-status mongo-shell \
		spark-up spark-down spark-logs spark-status \
		spark-up-metadata spark-up-transcripts-en spark-up-summary \
		airflow-up airflow-down airflow-logs

# --- Kafka Commands ---
kafka-up:
	docker-compose --env-file .env.development -f $(KAFKA_PATH)/docker-compose.yml up -d

kafka-down:
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml down

kafka-restart:
	make kafka-down && make kafka-up

kafka-logs:
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml logs -f

kafka-status:
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml ps

kafka-eval:
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 bash

create-topics:
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 kafka-topics.sh \
		--bootstrap-server kafka1:9092 --create --if-not-exists \
		--topic $(TOPIC_EPISODE_METADATA) --partitions 3 --replication-factor 3 && \
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 kafka-topics.sh \
		--bootstrap-server kafka1:9092 --create --if-not-exists \
		--topic $(TOPIC_EPISODES_ID) --partitions 3 --replication-factor 3 --config retention.ms=-1  --config cleanup.policy=compact

list-topics:
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 kafka-topics.sh \
		--bootstrap-server kafka1:9092 --list

# --- Mongo Commands ---
mongo-up:
	docker-compose --env-file .env.development -f $(MONGO_PATH)/docker-compose.yml up -d

mongo-down:
	docker-compose -f $(MONGO_PATH)/docker-compose.yml down

mongo-logs:
	docker-compose -f $(MONGO_PATH)/docker-compose.yml logs -f

mongo-status:
	docker-compose -f $(MONGO_PATH)/docker-compose.yml ps

mongo-shell:
	docker exec -it mongodb mongosh

# -- Spark Commands ---
build-spark-image:
	docker build -t $(SPARK_IMAGE_NAME):$(SPARK_IMAGE_TAG) $(SPARK_PATH)/

spark-up:
	@make build-spark-image
	docker-compose --env-file .env.development -f $(SPARK_PATH)/docker-compose.yml up -d --build

spark-down:
	docker-compose -f $(SPARK_PATH)/docker-compose.yml down

spark-logs:
	docker-compose -f $(SPARK_PATH)/docker-compose.yml logs -f

spark-status:
	docker-compose -f $(SPARK_PATH)/docker-compose.yml ps

# -- Airflow Commands ---

build-airflow-image:
	docker build -t $(AIRFLOW_IMAGE_NAME):$(AIRFLOW_IMAGE_TAG) $(AIRFLOW_PATH)/

airflow-up: 
	@make build-airflow-image
	@docker-compose --env-file .env.development -f $(AIRFLOW_PATH)/docker-compose.yml up -d --build

airflow-down:
	@docker-compose -f $(AIRFLOW_PATH)/docker-compose.yml down

airflow-logs:
	@docker-compose -f $(AIRFLOW_PATH)/docker-compose.yml logs -f


# --- General Aggregate Commands ---
up: kafka-up mongo-up spark-up airflow-up

down: kafka-down mongo-down spark-down airflow-down

restart: down up

logs:
	@echo "\n--- Kafka Logs ---"
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml logs --tail=20
	@echo "\n--- Mongo Logs ---"
	docker-compose -f $(MONGO_PATH)/docker-compose.yml logs --tail=20
	@echo "\n--- Spark Logs ---"
	docker-compose -f $(SPARK_PATH)/docker-compose.yml logs --tail=20
	@echo "\n--- Airflow Logs ---"
	docker-compose -f $(AIRFLOW_PATH)/docker-compose.yml logs --tail=20

status:
	@echo "\n--- Kafka Status ---"
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml ps
	@echo "\n--- Mongo Status ---"
	docker-compose -f $(MONGO_PATH)/docker-compose.yml ps
	@echo "\n--- Spark Status ---"
	docker-compose -f $(SPARK_PATH)/docker-compose.yml ps

# --- Full Project Initialization ---
init:
	@make up
	@docker compose -f $(SPARK_PATH)/docker-compose.yml build
	@echo "Waiting for Kafka to be ready..."
	@until docker-compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 kafka-topics.sh --bootstrap-server kafka1:9092 --list > /dev/null 2>&1; do \
		echo "Kafka not ready, waiting..."; \
		sleep 2; \
	done
	@make create-topics
	@make status

