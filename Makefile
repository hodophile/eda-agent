# Convenience targets for the Phase 1 agent system.
MODEL ?= llama3
SERVICES := orchestrator:8000 llm_service:8001 tool_service:8002

.PHONY: help install up up-all down logs topics list-topics \
        ollama-pull ollama-models run-orchestrator run-llm run-tool test watch clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install:  ## Create the uv virtualenv and install deps
	uv sync

up:  ## Start Kafka, Redis, Ollama and initialise topics + model
	docker compose up -d kafka redis ollama
	docker compose up kafka-init
	MODEL=$(MODEL) docker compose up ollama-init
	@echo "\nInfra ready: kafka :9092  redis :6379  ollama :11434"

up-all:  ## Also bring up the optional Phase 2 stores
	docker compose --profile phase2 up -d

down:  ## Stop all containers (keeps volumes)
	docker compose --profile phase2 down

logs:  ## Tail infra logs
	docker compose logs -f kafka redis ollama

topics:  ## (Re)create the agent.* topics
	docker compose up kafka-init

list-topics:  ## List Kafka topics
	docker exec eda-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

ollama-pull:  ## Pull the LLM model into Ollama
	MODEL=$(MODEL) docker compose up ollama-init

ollama-models:  ## List installed Ollama models
	docker exec eda-ollama ollama list

run-orchestrator:  ## Run the orchestrator (:8000)
	uv run uvicorn orchestrator.main:app --reload --port 8000

run-llm:  ## Run the LLM service (:8001)
	OLLAMA_MODEL=$(MODEL) uv run uvicorn llm_service.main:app --reload --port 8001

run-mock:  ## Run a deterministic mock LLM (plumbing test, no model needed)
	uv run python scripts/mock_llm.py

run-tool:  ## Run the tool service (:8002)
	uv run uvicorn tool_service.main:app --reload --port 8002

test:  ## Send a sample multi-step prompt to the running orchestrator
	uv run python -m test_client.main

watch:  ## Stream agent.lifecycle.events
	uv run python scripts/watch_events.py

clean:  ## Stop containers AND delete volumes (full reset)
	docker compose --profile phase2 down -v
