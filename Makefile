# Makefile for managing Big Data platform services

KAFKA_PATH=./docker/kafka
MONGO_PATH=./docker/mongodb

.PHONY: up down restart logs status \
        kafka-up kafka-down kafka-logs kafka-status kafka-restart kafka-eval \
        create-topic list-topics \
		mongo-up mongo-down mongo-logs mongo-status mongo-shell

# --- Kafka Commands ---
kafka-up:
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml up -d

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
		--topic podcast-metadata --partitions 3 --replication-factor 3 --config retention.ms=-1 && \
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 kafka-topics.sh \
		--bootstrap-server kafka1:9092 --create --if-not-exists \
		--topic transcripts-en --partitions 9 --replication-factor 3 && \
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 kafka-topics.sh \
		--bootstrap-server kafka1:9092 --create --if-not-exists \
		--topic transcripts-foreign --partitions 9 --replication-factor 3 && \
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 kafka-topics.sh \
		--bootstrap-server kafka1:9092 --create --if-not-exists \
		--topic user-events-stream --partitions 24 --replication-factor 3

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

# --- General Aggregate Commands ---
up: kafka-up mongo-up

down: kafka-down mongo-down

restart: down up

logs:
	@echo "\n--- Kafka Logs ---"
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml logs --tail=20
	@echo "\n--- Mongo Logs ---"
	docker-compose -f $(MONGO_PATH)/docker-compose.yml logs --tail=20

status:
	@echo "\n--- Kafka Status ---"
	docker-compose -f $(KAFKA_PATH)/docker-compose.yml ps
	@echo "\n--- Kafka Status ---"
	docker-compose -f $(MONGO_PATH)/docker-compose.yml ps

# --- Full Project Initialization ---
init:
	@make up
	@echo "Waiting for Kafka to be ready..."
	@until docker-compose -f $(KAFKA_PATH)/docker-compose.yml exec kafka1 kafka-topics.sh --bootstrap-server kafka1:9092 --list > /dev/null 2>&1; do \
		echo "Kafka not ready, waiting..."; \
		sleep 2; \
	done
	@make create-topics
	@make status

