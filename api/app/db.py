"""Async SQLAlchemy engine/session setup for Postgres.

Job and Workflow ORM models (Phase 1+) will subclass `Base`. Routes obtain a
session via the `get_db` FastAPI dependency rather than importing the engine
directly, so tests can override it later.

Engine creation is lazy (no connection is opened at import time), so this
module is safe to import even before Postgres is reachable.
"""
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


engine = create_async_engine(settings.database_url, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_db() -> AsyncSession:
    """FastAPI dependency yielding a request-scoped DB session."""
    async with AsyncSessionLocal() as session:
        yield session
