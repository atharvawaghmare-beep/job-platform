"""RabbitMQ publisher.

Uses aio-pika (not the sync `pika` the worker uses) because this runs inside
FastAPI's event loop — a blocking pika call here would stall every other
request on the same worker process. The connection is opened once at app
startup (see `lifespan` in main.py) and reused across requests.

Only the job id is published; the worker re-reads job state from Postgres,
which stays the single source of truth.

Phase 5: the `jobs` queue is declared with `x-max-priority` so RabbitMQ
attempts higher-priority-first delivery among ready messages — see
common/priority.py for the mapping from our `priority` field to RabbitMQ's
scale. worker/main.py declares the same queue with the same ceiling; the
two must always agree; a mismatch makes RabbitMQ reject the declare.
"""
import asyncio
import json
import logging

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from app.config import settings
from common.priority import RABBITMQ_MAX_PRIORITY

JOBS_QUEUE = "jobs"

logger = logging.getLogger("app.queue")

_connection: AbstractRobustConnection | None = None
_channel: AbstractChannel | None = None


async def connect(max_attempts: int = 10, delay_seconds: float = 3.0) -> None:
    """`connect_robust` handles reconnection after a connection that's
    already up drops, but not a first attempt that's refused outright — and
    RabbitMQ's healthcheck can report healthy slightly before its AMQP
    listener is actually bound (worker/main.py hits the same race, with the
    same fix). Retry the initial connect ourselves rather than relying on
    `depends_on: condition: service_healthy` alone.
    """
    global _connection, _channel
    for attempt in range(1, max_attempts + 1):
        try:
            _connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            break
        except ConnectionError:
            if attempt == max_attempts:
                raise
            logger.warning(
                "RabbitMQ not ready yet (attempt %d/%d), retrying in %.0fs",
                attempt, max_attempts, delay_seconds,
            )
            await asyncio.sleep(delay_seconds)

    _channel = await _connection.channel()
    await _channel.declare_queue(JOBS_QUEUE, durable=True, arguments={"x-max-priority": RABBITMQ_MAX_PRIORITY})


async def disconnect() -> None:
    global _connection, _channel
    if _connection is not None:
        await _connection.close()
    _connection = None
    _channel = None


async def publish_job(job_id: str, priority: int = 0) -> None:
    """`priority` is already on RabbitMQ's [0, RABBITMQ_MAX_PRIORITY] scale
    — callers compute it via common.priority.to_rabbitmq_priority (and
    optionally apply_fairness_downshift) rather than passing the job's raw
    `priority` field through, keeping this module agnostic of that mapping.
    """
    if _channel is None:
        raise RuntimeError("queue channel not initialized — was connect() called at startup?")

    message = aio_pika.Message(
        body=json.dumps({"job_id": job_id}).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        priority=priority,
    )
    await _channel.default_exchange.publish(message, routing_key=JOBS_QUEUE)
