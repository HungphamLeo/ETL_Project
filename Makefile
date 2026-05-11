# ============================
# Project variables
# ============================
PROJECT_ROOT := $(shell pwd)
COMPOSE_FILE := infra/docker_compose/docker-compose.dev.yml
DOCKER_COMPOSE := docker-compose -f $(COMPOSE_FILE)
PROJECT_NAME=lakehouse-dev

# ============================
# Targets
# ============================

.PHONY: help build up down restart logs ps clean rebuild

help:
	@echo "Make targets:"
	@echo "  make build        Build all Docker images"
	@echo "  make up           Start containers (detached)"
	@echo "  make down         Stop and remove containers"
	@echo "  make restart      Restart containers"
	@echo "  make logs         Tail logs (all services)"
	@echo "  make ps           Show running containers"
	@echo "  make clean        Remove containers + volumes"
	@echo "  make rebuild      Clean + build + up"

# ----------------------------
# Docker compose actions
# ----------------------------

build:
	$(DOCKER_COMPOSE) pull
	$(DOCKER_COMPOSE) build

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

restart:
	$(DOCKER_COMPOSE) down && $(DOCKER_COMPOSE) up -d

logs:
	$(DOCKER_COMPOSE) logs -f

ps:
	$(DOCKER_COMPOSE) ps

clean:
	$(DOCKER_COMPOSE) down -v

rebuild:
	$(DOCKER_COMPOSE) down -v
	$(DOCKER_COMPOSE) build --no-cache
	$(DOCKER_COMPOSE) up -d

# ====================================
# LAKEHOUSE INFRASTRUCTURE (NEW)
# ====================================

.PHONY: infra-up infra-down infra-init infra-status sqlmesh-init sqlmesh-plan sqlmesh-run test help-lakehouse

help-lakehouse:
	@echo ""
	@echo "LAKEHOUSE INFRASTRUCTURE COMMANDS:"
	@echo "  make infra-up            - Start dev infrastructure (MinIO, Postgres, Prefect, Kafka)"
	@echo "  make infra-down          - Stop infrastructure"
	@echo "  make infra-init          - Initialize MinIO lakehouse buckets"
	@echo "  make infra-status        - Show container status"
	@echo ""
	@echo "SQLMESH COMMANDS:"
	@echo "  make sqlmesh-init        - Initialize SQLMesh DuckDB project"
	@echo "  make sqlmesh-plan        - Show execution plan"
	@echo "  make sqlmesh-run         - Execute transforms"
	@echo ""
	@echo "TESTING:"
	@echo "  make test                - Run unit + integration tests"
	@echo ""

infra-up:
	@echo "🚀 Starting Lakehouse infrastructure..."
	docker-compose -f infra/docker_compose/docker-compose.dev.yml up -d
	@echo "⏳ Waiting for services..."
	sleep 10
	@echo "✅ Services started - use 'make infra-status' to check"

infra-down:
	@echo "🛑 Stopping infrastructure..."
	docker-compose -f infra/docker_compose/docker-compose.dev.yml down
	@echo "✅ Stopped"

infra-status:
	docker-compose -f infra/docker_compose/docker-compose.dev.yml ps

infra-init: infra-up
	sleep 5
	@echo "📂 Initializing MinIO..."
	python infra/init_lakehouse_storage.py

sqlmesh-init:
	@echo "⚙️  Initializing SQLMesh..."
	cd sqlmesh && sqlmesh init duckdb && echo "✅ Done"

sqlmesh-plan:
	cd sqlmesh && sqlmesh plan

sqlmesh-run:
	cd sqlmesh && sqlmesh run

test:
	pytest tests/ -v --tb=short
