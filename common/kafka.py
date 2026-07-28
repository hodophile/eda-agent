"""Thin aiokafka helpers.

All values are JSON strings; the Kafka message key is always the task_id so
events for one loop are strictly ordered on a single partition.
"""
from __future__ import annotations

from typing import Iterable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from . import config


def make_producer() -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        enable_idempotence=True,
    )


def make_consumer(group_id: str, topics: Iterable[str]) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        value_deserializer=lambda b: b.decode("utf-8") if b else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )


async def send_json(producer: AIOKafkaProducer, topic: str, task_id: str, payload: str) -> None:
    """Publish a JSON-serialised payload string keyed by task_id."""
    await producer.send_and_wait(topic, key=task_id, value=payload)
