"""LLM Service (the Brain).

A deliberately dumb, stateless consumer: it reads a full message history from
`agent.llm.requests`, calls Ollama via the OpenAI-compatible client, and writes
the raw text to `agent.llm.responses`. All ReAct parsing happens in the
orchestrator -- this service never makes any routing decisions.
"""
from __future__ import annotations

import asyncio
import json
import logging

from contextlib import asynccontextmanager
from openai import AsyncOpenAI

from common import config, kafka
from common.messages import LLMRequest, LLMResponse

from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [llm] %(message)s")
log = logging.getLogger("llm")


class LLMService:
    def __init__(self) -> None:
        self.client = AsyncOpenAI(
            base_url=config.OLLAMA_BASE_URL,
            api_key=config.OLLAMA_API_KEY,
        )
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None
        self._task: asyncio.Task | None = None

    async def _infer(self, req: LLMRequest) -> LLMResponse:
        log.info("task=%s inferring over %d messages", req.task_id, len(req.messages))
        resp = await self.client.chat.completions.create(
            model=config.OLLAMA_MODEL,
            messages=req.messages,
            response_format={"type": "json_object"},
            temperature=0,
        )
        content = (resp.choices[0].message.content or "").strip()
        log.info("task=%s produced %d chars", req.task_id, len(content))
        return LLMResponse(task_id=req.task_id, content=content)

    async def _handle(self, raw: str) -> None:
        try:
            req = LLMRequest(**json.loads(raw))
        except Exception as exc:  # malformed message -> nothing we can route on
            log.error("dropping unparseable request: %s", exc)
            return

        try:
            out = await self._infer(req)
        except Exception as exc:
            log.exception("task=%s LLM call failed", req.task_id)
            out = LLMResponse(task_id=req.task_id, error=str(exc))

        await kafka.send_json(
            self.producer, config.TOPIC_LLM_RESPONSES, req.task_id, out.model_dump_json()
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
        self.consumer = kafka.make_consumer(config.GROUP_LLM, [config.TOPIC_LLM_REQUESTS])
        await self.consumer.start()
        self._task = asyncio.create_task(self._loop(), name="llm-consumer")
        log.info("LLM service up (model=%s, broker=%s)", config.OLLAMA_MODEL, config.KAFKA_BOOTSTRAP_SERVERS)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self.consumer:
            await self.consumer.stop()
        await self.producer.stop()
        log.info("LLM service stopped")


service = LLMService()


@asynccontextmanager
async def lifespan(_app):
    await service.start()
    try:
        yield
    finally:
        await service.stop()


from fastapi import FastAPI  # noqa: E402

app = FastAPI(title="LLM Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "model": config.OLLAMA_MODEL}
