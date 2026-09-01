"""Job submission and lookup — `POST /jobs`, `GET /jobs/{id}`.

Phase 3 adds idempotency-key dedupe on submit: resubmitting the same key
with the same type/payload returns the existing job (200) instead of
creating a duplicate; resubmitting it with a different type/payload is a
409 conflict. See app/redis_client.py for the cache-then-DB lookup strategy.

Phase 5 adds priority-ordered consumption: `priority` (already stored since
Phase 1 but unused until now) is mapped onto RabbitMQ's native priority
queues at publish time, plus an optional per-owner fairness downshift — see
common/priority.py for both.

Still out of scope here: `depends_on` (that's POST /workflows — Phase 4).
"""
import logging
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, ConfigDict, Field
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.priority_service import effective_rabbitmq_priority
from app.queue import publish_job
from app.redis_client import get_redis
from common.models import Job, JobStatus
from common.priority import PRIORITY_NORMAL

logger = logging.getLogger("app.jobs")

router = APIRouter(tags=["jobs"])

DEFAULT_MAX_RETRIES = 3
IDEMPOTENCY_CACHE_TTL_SECONDS = 24 * 60 * 60


class JobCreate(BaseModel):
    type: str = Field(min_length=1)
    payload: dict = Field(default_factory=dict)
    priority: int | None = None
    idempotency_key: str | None = None
    max_retries: int | None = Field(default=None, ge=0)
    owner_id: str | None = None


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    idempotency_key: str | None
    type: str
    payload: dict
    status: JobStatus
    priority: int
    retry_count: int
    max_retries: int
    claimed_by: str | None
    claim_count: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    result: dict | None
    error: str | None
    owner_id: str | None
    workflow_id: uuid.UUID | None
    workflow_key: str | None
    depends_on: list[uuid.UUID]


def _matches(existing: Job, body: JobCreate) -> bool:
    return existing.type == body.type and existing.payload == body.payload


async def _find_by_idempotency_key(db: AsyncSession, idempotency_key: str) -> Job | None:
    """The correctness backstop — a plain indexed DB lookup."""
    result = await db.execute(select(Job).where(Job.idempotency_key == idempotency_key))
    return result.scalar_one_or_none()


async def _lookup_idempotent_job(db: AsyncSession, idempotency_key: str) -> Job | None:
    """Redis-first, DB-fallback lookup. A cache hit still re-reads the job
    by primary key so the response reflects current status, not a stale
    snapshot — the cache only saves the indexed lookup on the key itself.
    """
    cached_job_id = None
    try:
        cached_job_id = await get_redis().get(f"idem:{idempotency_key}")
    except RedisError:
        logger.warning("redis unavailable for idempotency lookup, falling back to DB", exc_info=True)

    if cached_job_id is not None:
        job = await db.get(Job, uuid.UUID(cached_job_id))
        if job is not None:
            return job
        # Cache pointed at a job_id no longer present — shouldn't happen
        # (jobs aren't deleted), but fall through to the DB lookup rather
        # than trust a stale/bad cache entry.

    return await _find_by_idempotency_key(db, idempotency_key)


async def _cache_idempotent_job(idempotency_key: str, job_id: uuid.UUID) -> None:
    try:
        await get_redis().set(f"idem:{idempotency_key}", str(job_id), ex=IDEMPOTENCY_CACHE_TTL_SECONDS)
    except RedisError:
        logger.warning("redis unavailable, skipping idempotency cache write", exc_info=True)


@router.post("/jobs", response_model=JobOut, status_code=status.HTTP_201_CREATED)
async def create_job(body: JobCreate, response: Response, db: AsyncSession = Depends(get_db)) -> Job:
    if body.idempotency_key:
        existing = await _lookup_idempotent_job(db, body.idempotency_key)
        if existing is not None:
            if _matches(existing, body):
                response.status_code = status.HTTP_200_OK
                return existing
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="idempotency_key already in use with a different type/payload",
            )

    job = Job(
        type=body.type,
        payload=body.payload,
        priority=body.priority if body.priority is not None else PRIORITY_NORMAL,
        idempotency_key=body.idempotency_key,
        max_retries=body.max_retries if body.max_retries is not None else DEFAULT_MAX_RETRIES,
        owner_id=body.owner_id,
        status=JobStatus.PENDING,
    )
    db.add(job)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        # Lost a race with a concurrent request using the same key between
        # our check above and this insert. Re-check rather than assume — the
        # other request may have used the same (matching) payload.
        existing = await _find_by_idempotency_key(db, body.idempotency_key) if body.idempotency_key else None
        if existing is not None and _matches(existing, body):
            response.status_code = status.HTTP_200_OK
            return existing
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="idempotency_key already in use with a different type/payload",
        ) from exc
    await db.refresh(job)

    if body.idempotency_key:
        await _cache_idempotent_job(body.idempotency_key, job.id)

    # Status must be `queued` in the DB *before* the message can possibly
    # reach a worker — not after. Publishing first (then flipping the
    # status) leaves a window where a fast worker claims the message while
    # the row still says `pending`; Phase 3's atomic claim (worker/main.py)
    # correctly refuses that claim since `pending` isn't a claimable state,
    # so the job would get stuck forever. Committing `queued` first means
    # the worst case on a publish failure is a job stuck at `queued` with no
    # matching message — visible and reconcilable, unlike a stuck `pending`
    # row that silently drops the job (this exact race is what surfaced it:
    # it worked around it silently by claiming unconditionally).
    job.status = JobStatus.QUEUED
    await db.commit()
    await db.refresh(job)

    try:
        priority = await effective_rabbitmq_priority(db, job)
        await publish_job(str(job.id), priority)
    except Exception as exc:
        # No auto-retry-publish sweep exists yet for this stuck-at-queued
        # case; that'd be a reasonable further hardening.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="job stored (status=queued) but failed to enqueue its message",
        ) from exc

    return job


@router.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)) -> Job:
    job = await db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return job
