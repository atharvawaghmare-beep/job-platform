"""Async Redis client — used as a fast, best-effort dedupe cache for
idempotency keys (see routers/jobs.py).

Deliberately not the source of truth: Postgres's unique constraint on
`idempotency_key` is what actually prevents duplicate jobs. Redis only saves
a repeat submission from needing a DB lookup, so every call site here is
wrapped by the caller to degrade gracefully (fall back to Postgres) if Redis
is slow, empty, or unreachable — nothing breaks if this cache is wrong.

Connection is lazy (redis-py connects on first command), so unlike the
RabbitMQ clients there's no eager-connect startup race to guard against.
"""
import redis.asyncio as redis

from app.config import settings

_redis: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(settings.redis_url, decode_responses=True)
    return _redis


async def disconnect() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
    _redis = None
