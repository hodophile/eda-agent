"""Tool Service (the Hands).

Stateless consumer of `agent.tool.requests`: dispatches to the registered tool
and publishes the observation to `agent.tool.responses`.
"""
from __future__ import annotations

import asyncio
import json
import logging

from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI

from common import config, kafka
from common.messages import ToolRequest, ToolResponse

from .tools import TOOL_REGISTRY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [tool] %(message)s")
log = logging.getLogger("tool")


class ToolService:
    def __init__(self) -> None:
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: kafka.AIOKafkaProducer | None = None
        self._task: asyncio.Task | None = None

    async def _handle(self, raw: str) -> None:
        try:
            req = ToolRequest(**json.loads(raw))
        except Exception as exc:
            log.error("dropping unparseable request: %s", exc)
            return

        tool = TOOL_REGISTRY.get(req.tool)
        if tool is None:
            observation = f"Error: unknown tool '{req.tool}'. Available: {list(TOOL_REGISTRY)}"
        else:
            try:
                observation = tool(req.input)
            except Exception as exc:
                observation = f"Error: {exc}"

        log.info("task=%s %s(%r) -> %s", req.task_id, req.tool, req.input, observation)
        out = ToolResponse(
            task_id=req.task_id,
            tool=req.tool,
            input=req.input,
            observation=observation,
        )
        await kafka.send_json(
            self.producer, config.TOPIC_TOOL_RESPONSES, req.task_id, out.model_dump_json()
        )

    async def _loop(self) -> None:
        assert self.consumer is not None
        async for msg in self.consumer:
            try:
                await self._handle(msg.value)
                await self.consumer.commit()
            except Exception:
                log.exception("unhandled error processing message")

    async def start(self) -> None:
        self.producer = kafka.make_producer()
        await self.producer.start()
        self.consumer = kafka.make_consumer(config.GROUP_TOOL, [config.TOPIC_TOOL_REQUESTS])
        await self.consumer.start()
        self._task = asyncio.create_task(self._loop(), name="tool-consumer")
        log.info("Tool service up, registered tools: %s", list(TOOL_REGISTRY))

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self.consumer:
            await self.consumer.stop()
        await self.producer.stop()
        log.info("Tool service stopped")


service = ToolService()


@asynccontextmanager
async def lifespan(_app):
    await service.start()
    try:
        yield
    finally:
        await service.stop()


app = FastAPI(title="Tool Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "tools": list(TOOL_REGISTRY)}
