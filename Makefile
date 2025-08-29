# Makefile for managing Big Data platform services

KAFKA_PATH=./docker/kafka
MONGO_PATH=./docker/mongodb
SPARK_PATH=./docker/spark
AIRFLOW_PATH=./docker/airflow
STREAMLIT_PATH=./docker/streamlit        # <-- NEW (dashboard compose lives here)

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
        build-airflow-image airflow-up airflow-down airflow-logs \
		dashboard-up dashboard-down dashboard-logs dashboard-status \
		init

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

# NEW: initialize DB indexes used by the recsys
mongo-init:
	@echo "Waiting for MongoDB..."
	@until docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping')" >/dev/null 2>&1; do \
		echo "  Mongo not ready, waiting..."; sleep 2; \
	done
	@echo "Creating MongoDB indexes..."
	@docker exec mongodb mongosh --quiet --eval '\
		db = db.getSiblingDB("$(MONGO_DB)"); \
		db.user_history_snapshot.createIndex({ user_id:1 }); \
		db.user_history_snapshot.createIndex({ user_id:1, episode_id:1 }, { unique:true }); \
		db.final_recommendations.createIndex({ user_id:1 }); \
		db.final_recommendations.createIndex({ recommended_episode_id:1 }); \
		db.$(MONGO_COLLECTION_USER_EVENTS).createIndex({ user_id:1 }); \
		db.$(MONGO_COLLECTION_USER_EVENTS).createIndex({ episode_id:1 }); \
	'

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

# -- Dashboard (Streamlit) ---
dashboard-up:
	docker compose --env-file .env.development -f $(STREAMLIT_PATH)/docker-compose.yml up -d --build
dashboard-down:
	docker compose -f $(STREAMLIT_PATH)/docker-compose.yml down
dashboard-logs:
	docker compose -f $(STREAMLIT_PATH)/docker-compose.yml logs -f
dashboard-status:
	docker compose -f $(STREAMLIT_PATH)/docker-compose.yml ps

DASHBOARD_PORT ?= 8510
DASHBOARD_URL  ?= http://localhost:$(DASHBOARD_PORT)

wait-for-dashboard:
	@echo "Waiting for $(DASHBOARD_URL)…"
	@until curl -fsS "$(DASHBOARD_URL)" >/dev/null 2>&1; do sleep 1; printf "."; done; echo " up!"

open-dashboard:
	@URL="$(DASHBOARD_URL)"; \
	if command -v open >/dev/null 2>&1; then open $$URL; \
	elif command -v xdg-open >/dev/null 2>&1; then xdg-open $$URL; \
	elif command -v powershell.exe >/dev/null 2>&1; then powershell.exe Start-Process $$URL; \
	else echo "Open $$URL in your browser."; fi

# --- General Aggregate Commands ---
up: kafka-up mongo-up spark-up airflow-up dashboard-up

down: mongo-down spark-down airflow-down kafka-down dashboard-down

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
	@echo "\n--- Dashboard Logs ---";
	docker compose -f $(STREAMLIT_PATH)/docker-compose.yml logs --tail=20

status:
	@echo "\n--- Kafka Status ---"
	docker compose -f $(KAFKA_PATH)/docker-compose.yml ps
	@echo "\n--- Mongo Status ---"
	docker compose -f $(MONGO_PATH)/docker-compose.yml ps
	@echo "\n--- Spark Status ---"
	docker compose -f $(SPARK_PATH)/docker-compose.yml ps
	@echo "\n--- Dashboard Status ---"
	docker compose -f $(STREAMLIT_PATH)/docker-compose.yml ps

# --- Full Project Initialization ---
init:
	@$(MAKE) kafka-up mongo-up airflow-up
	@docker compose -f $(SPARK_PATH)/docker-compose.yml build
	@echo "Waiting for Kafka controller quorum..."
	@until docker compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 \
		bash -lc '$(KAFKA_BIN)/kafka-metadata-quorum.sh --bootstrap-server kafka1:9092 describe --status >/dev/null 2>&1'; do \
		echo "Kafka not ready, waiting..."; \
		sleep 2; \
	done
	@$(MAKE) create-topics
	@$(MAKE) mongo-init
	@$(MAKE) wait-for-dashboard
	@$(MAKE) open-dashboard
	@$(MAKE) spark-up
	@$(MAKE) status
