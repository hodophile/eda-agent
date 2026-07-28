#!/usr/bin/env python3
"""Deterministic mock LLM for plumbing tests (NOT the real Phase 1 LLM service).

It consumes `agent.llm.requests` and emits valid ReAct JSON exactly like a
well-behaved model would, so we can prove the orchestrator + tool + Kafka +
Redis loop works end-to-end without waiting on a multi-GB model download.

Logic (decided from the conversation history, fully stateless):
  - 0 observations seen -> call Calculator on the first `A * B` in the prompt
  - 1 observation        -> if prompt says "add N", call Calculator(obs + N);
                            else FINAL(obs)
  - 2+ observations      -> FINAL(last observation)

Run only the real llm_service OR this mock at any one time (they share a topic).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re

from aiokafka import AIOKafkaConsumer

from common import config, kafka
from common.messages import LLMRequest, LLMResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [mock-llm] %(message)s")
log = logging.getLogger("mock-llm")

_OBS_RE = re.compile(r"Observation:\s*(-?\d+(?:\.\d+)?)")
_MUL_RE = re.compile(r"(\d+)\s*\*\s*(\d+)")
_ADD_RE = re.compile(r"add\s+(\d+)")


def _decide(messages: list[dict]) -> dict:
    prompt = next((m["content"] for m in messages if m["role"] == "user" and not m["content"].startswith("Observation:")), "")
    observations = [float(m.group(1)) for m in (_OBS_RE.search(m["content"]) for m in messages if m["role"] == "user") if m]
    observations = [o for o in observations]

    if not observations:
        m = _MUL_RE.search(prompt)
        if m:
            expr = f"{m.group(1)} * {m.group(2)}"
            return {"thought": f"need to compute {expr}", "action": "Calculator", "action_input": expr}
        return {"thought": "no arithmetic needed", "action": "FINAL", "action_input": prompt}

    last = observations[-1]
    last_int = int(last) if float(last).is_integer() else last
    if len(observations) == 1:
        m = _ADD_RE.search(prompt)
        if m:
            n = m.group(1)
            return {"thought": f"now add {n} to {last_int}", "action": "Calculator", "action_input": f"{last_int} + {n}"}
    return {"thought": "have the result", "action": "FINAL", "action_input": str(last_int)}


async def main() -> None:
    consumer: AIOKafkaConsumer = kafka.make_consumer("mock-llm", [config.TOPIC_LLM_REQUESTS])
    producer = kafka.make_producer()
    await producer.start()
    await consumer.start()
    log.info("mock LLM up")
    try:
        async for msg in consumer:
            req = LLMRequest(**json.loads(msg.value))
            payload = _decide(req.messages)
            content = json.dumps(payload)
            log.info("task=%s -> %s", req.task_id, content)
            await kafka.send_json(producer, config.TOPIC_LLM_RESPONSES, req.task_id, LLMResponse(task_id=req.task_id, content=content).model_dump_json())
            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
