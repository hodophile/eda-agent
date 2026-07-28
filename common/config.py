"""Centralised configuration read from environment variables.

Keeps every service in sync so the orchestrator, LLM service and tool service
all agree on brokers, topics and the model to use.
"""
from __future__ import annotations

import os


def _get(name: str, default: str) -> str:
    return os.getenv(name, default)


# --- Kafka -----------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = _get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topics. Keyed by task_id everywhere for strict per-loop ordering.
TOPIC_TASKS_IN = "agent.tasks.in"
TOPIC_LLM_REQUESTS = "agent.llm.requests"
TOPIC_LLM_RESPONSES = "agent.llm.responses"
TOPIC_TOOL_REQUESTS = "agent.tool.requests"
TOPIC_TOOL_RESPONSES = "agent.tool.responses"
TOPIC_LIFECYCLE = "agent.lifecycle.events"

ALL_TOPICS = [
    TOPIC_TASKS_IN,
    TOPIC_LLM_REQUESTS,
    TOPIC_LLM_RESPONSES,
    TOPIC_TOOL_REQUESTS,
    TOPIC_TOOL_RESPONSES,
    TOPIC_LIFECYCLE,
]

# Consumer group ids.
GROUP_ORCHESTRATOR = "orchestrator"
GROUP_LLM = "llm-service"
GROUP_TOOL = "tool-service"

# --- Redis -----------------------------------------------------------------
REDIS_URL = _get("REDIS_URL", "redis://localhost:6379/0")
TASK_KEY_PREFIX = "task:"


def task_key(task_id: str) -> str:
    return f"{TASK_KEY_PREFIX}{task_id}"


# --- LLM -------------------------------------------------------------------
OLLAMA_BASE_URL = _get("OLLAMA_BASE_URL", "http://localhost:11434/v1")
OLLAMA_API_KEY = _get("OLLAMA_API_KEY", "ollama")
OLLAMA_MODEL = _get("OLLAMA_MODEL", "llama3")

# --- Safety ----------------------------------------------------------------
MAX_STEPS = int(_get("MAX_STEPS", "10"))

# --- Lifecycle statuses ----------------------------------------------------
STATUS_PENDING = "PENDING"
STATUS_THINKING = "THINKING"
STATUS_ACTING = "ACTING"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
