# ============================
# Project variables
# ============================
PROJECT_ROOT := $(shell pwd)
COMPOSE_FILE := infra/docker_compose/cophieu68.yml
DOCKER_COMPOSE := docker-compose -f $(COMPOSE_FILE)
PROJECT_NAME=cophieu68

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
