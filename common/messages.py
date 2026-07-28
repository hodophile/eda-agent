"""Message schemas shared across Kafka topics.

Every message is keyed by `task_id` (the Kafka message key) so that all events
belonging to a single ReAct loop land on the same partition and stay ordered.
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class TaskRequest(BaseModel):
    """agent.tasks.in -- external trigger that kicks off a loop."""

    task_id: str
    prompt: str


class LLMRequest(BaseModel):
    """agent.llm.requests -- orchestrator asks the LLM service to infer.

    The full conversation history is sent every time so the LLM service stays
    completely stateless.
    """

    task_id: str
    messages: list[dict[str, Any]]


class LLMResponse(BaseModel):
    """agent.llm.responses -- raw LLM output. The orchestrator parses it."""

    task_id: str
    content: str = ""
    error: Optional[str] = None


class ToolRequest(BaseModel):
    """agent.tool.requests -- orchestrator asks the tool service to act."""

    task_id: str
    tool: str
    input: str = Field(description="Single string argument for the tool")


class ToolResponse(BaseModel):
    """agent.tool.responses -- tool observation fed back into the loop."""

    task_id: str
    tool: str
    input: str
    observation: str


class LifecycleEvent(BaseModel):
    """agent.lifecycle.events -- UI-friendly progress stream."""

    task_id: str
    status: str
    message: str = ""
