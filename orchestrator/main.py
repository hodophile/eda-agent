"""Orchestrator (the State Machine).

The single hub that closes the asynchronous ReAct loop. It consumes three
topics and, based on which topic a message came in on, updates the Redis
scratchpad and produces the next event:

    agent.tasks.in      -> seed state, ask the LLM
    agent.llm.responses -> parse; tool call -> ask the tool, FINAL -> done
    agent.tool.responses-> append observation, ask the LLM again

State is persisted under `task:{task_id}` in Redis so any step is crash-safe.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common import config, kafka
from common.messages import (
    LLMRequest,
    LLMResponse,
    LifecycleEvent,
    TaskRequest,
    ToolResponse,
)
from common.react import SYSTEM_PROMPT, parse_react
from common.redis_client import get_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [orch] %(message)s")
log = logging.getLogger("orchestrator")


# --- Redis state helpers ---------------------------------------------------
async def load_state(task_id: str) -> dict[str, Any] | None:
    raw = await get_redis().get(config.task_key(task_id))
    return json.loads(raw) if raw else None


async def save_state(state: dict[str, Any]) -> None:
    state["updated_at"] = time.time()
    await get_redis().set(config.task_key(state["task_id"]), json.dumps(state))


# --- Event producers -------------------------------------------------------
async def ask_llm(producer, task_id: str, messages: list[dict[str, Any]]) -> None:
    req = LLMRequest(task_id=task_id, messages=messages)
    await kafka.send_json(producer, config.TOPIC_LLM_REQUESTS, task_id, req.model_dump_json())


async def call_tool(producer, task_id: str, tool: str, action_input: str) -> None:
    from common.messages import ToolRequest

    req = ToolRequest(task_id=task_id, tool=tool, input=action_input)
    await kafka.send_json(producer, config.TOPIC_TOOL_REQUESTS, task_id, req.model_dump_json())


async def emit(producer, task_id: str, status: str, message: str = "") -> None:
    log.info("task=%s lifecycle %s: %s", task_id, status, message)
    event = LifecycleEvent(task_id=task_id, status=status, message=message)
    await kafka.send_json(producer, config.TOPIC_LIFECYCLE, task_id, event.model_dump_json())


# --- Routing rules ---------------------------------------------------------
async def on_task_in(producer, req: TaskRequest) -> None:
    existing = await load_state(req.task_id)
    if existing is not None and existing.get("status") != config.STATUS_PENDING:
        # Genuine replay of a task already in flight -- dedupe it.
        log.warning("task=%s already %s, ignoring", req.task_id, existing.get("status"))
        return

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": req.prompt},
    ]
    state = {
        "task_id": req.task_id,
        "status": config.STATUS_THINKING,
        "user_prompt": req.prompt,
        "messages": messages,
        "final_answer": None,
        "error": None,
        "step_count": 0,
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    await save_state(state)
    await emit(producer, req.task_id, "thinking", "Starting task")
    await ask_llm(producer, req.task_id, messages)


async def on_llm_response(producer, resp: LLMResponse) -> None:
    state = await load_state(resp.task_id)
    if state is None:
        log.warning("task=%s unknown, dropping LLM response", resp.task_id)
        return

    # Hard failure from the LLM service (e.g. Ollama down).
    if resp.error:
        state["status"] = config.STATUS_FAILED
        state["error"] = resp.error
        await save_state(state)
        await emit(producer, resp.task_id, "failed", f"LLM error: {resp.error}")
        return

    state["step_count"] += 1
    if state["step_count"] > config.MAX_STEPS:
        state["status"] = config.STATUS_FAILED
        state["error"] = f"exceeded MAX_STEPS={config.MAX_STEPS}"
        await save_state(state)
        await emit(producer, resp.task_id, "failed", state["error"])
        return

    call = parse_react(resp.content)

    # Unparseable output -> nudge the model with a correction and retry.
    if call is None:
        log.warning("task=%s unparseable LLM output: %r", resp.task_id, resp.content[:120])
        state["messages"].append({"role": "assistant", "content": resp.content})
        state["messages"].append(
            {
                "role": "user",
                "content": (
                    "Your previous response was not a valid JSON object. Reply "
                    "with ONLY the JSON object described in the system prompt."
                ),
            }
        )
        state["status"] = config.STATUS_THINKING
        await save_state(state)
        await ask_llm(producer, resp.task_id, state["messages"])
        return

    # Record the assistant turn so the model remembers its reasoning.
    state["messages"].append(
        {"role": "assistant", "content": json.dumps({"thought": call.thought, "action": call.action, "action_input": call.action_input})}
    )

    if call.is_final:
        state["status"] = config.STATUS_COMPLETED
        state["final_answer"] = call.action_input
        await save_state(state)
        await emit(producer, resp.task_id, "completed", call.action_input)
        return

    # Tool call -> ask the hands.
    state["status"] = config.STATUS_ACTING
    await save_state(state)
    await emit(producer, resp.task_id, "acting", f"Calling {call.action}({call.action_input!r})")
    await call_tool(producer, resp.task_id, call.action, call.action_input)


async def on_tool_response(producer, resp: ToolResponse) -> None:
    state = await load_state(resp.task_id)
    if state is None:
        log.warning("task=%s unknown, dropping tool response", resp.task_id)
        return

    # Feed the observation back as a user message and loop.
    state["messages"].append(
        {"role": "user", "content": f"Observation: {resp.observation}"}
    )
    state["status"] = config.STATUS_THINKING
    await save_state(state)
    await emit(producer, resp.task_id, "thinking", f"Observed: {resp.observation}")
    await ask_llm(producer, resp.task_id, state["messages"])


# --- Consumer loop ---------------------------------------------------------
class Orchestrator:
    def __init__(self) -> None:
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: kafka.AIOKafkaProducer | None = None
        self._task: asyncio.Task | None = None

    async def _dispatch(self, topic: str, raw: str) -> None:
        try:
            if topic == config.TOPIC_TASKS_IN:
                await on_task_in(self.producer, TaskRequest(**json.loads(raw)))
            elif topic == config.TOPIC_LLM_RESPONSES:
                await on_llm_response(self.producer, LLMResponse(**json.loads(raw)))
            elif topic == config.TOPIC_TOOL_RESPONSES:
                await on_tool_response(self.producer, ToolResponse(**json.loads(raw)))
            else:
                log.debug("ignoring topic %s", topic)
        except Exception:
            log.exception("error dispatching message on %s", topic)

    async def _loop(self) -> None:
        assert self.consumer is not None
        async for msg in self.consumer:
            await self._dispatch(msg.topic, msg.value)
            await self.consumer.commit()

    async def start(self) -> None:
        self.producer = kafka.make_producer()
        await self.producer.start()
        self.consumer = kafka.make_consumer(
            config.GROUP_ORCHESTRATOR,
            [config.TOPIC_TASKS_IN, config.TOPIC_LLM_RESPONSES, config.TOPIC_TOOL_RESPONSES],
        )
        await self.consumer.start()
        self._task = asyncio.create_task(self._loop(), name="orchestrator-consumer")
        log.info("Orchestrator up, consuming control topics")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self.consumer:
            await self.consumer.stop()
        await self.producer.stop()
        log.info("Orchestrator stopped")


orch = Orchestrator()


@asynccontextmanager
async def lifespan(_app):
    await orch.start()
    try:
        yield
    finally:
        await orch.stop()


app = FastAPI(title="Orchestrator", lifespan=lifespan)


# --- HTTP API (Step 5) -----------------------------------------------------
class RunRequest(BaseModel):
    prompt: str


@app.post("/agent/run")
async def run_agent(req: RunRequest) -> dict:
    """Accept a prompt, seed a PENDING task, and publish to agent.tasks.in."""
    task_id = str(uuid.uuid4())
    state = {
        "task_id": task_id,
        "status": config.STATUS_PENDING,
        "user_prompt": req.prompt,
        "messages": [],
        "final_answer": None,
        "error": None,
        "step_count": 0,
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    await save_state(state)
    await kafka.send_json(
        orch.producer,
        config.TOPIC_TASKS_IN,
        task_id,
        TaskRequest(task_id=task_id, prompt=req.prompt).model_dump_json(),
    )
    return {"task_id": task_id, "status": config.STATUS_PENDING}


@app.get("/agent/status/{task_id}")
async def status(task_id: str) -> dict:
    state = await load_state(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="task not found")
    return state


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
