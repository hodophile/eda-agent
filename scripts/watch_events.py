#!/usr/bin/env python3
"""Tail agent.lifecycle.events to demonstrate async UI observability."""
from __future__ import annotations

import asyncio
import json

from common import config, kafka
from common.messages import LifecycleEvent


async def main() -> None:
    consumer = kafka.make_consumer("watcher-cli", [config.TOPIC_LIFECYCLE])
    await consumer.start()
    print(f"watching {config.TOPIC_LIFECYCLE} ... (Ctrl-C to stop)")
    try:
        async for msg in consumer:
            try:
                ev = LifecycleEvent(**json.loads(msg.value))
            except Exception:
                continue
            print(f"[{ev.task_id[:8]}] {ev.status:<10} {ev.message}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
