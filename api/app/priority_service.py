"""Async helper for computing a job's effective RabbitMQ publish priority —
shared by routers/jobs.py and routers/workflows.py (both need the exact
same fairness-aware computation for their respective "publish this job
right now" call sites).
"""
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.models import Job, JobStatus
from common.priority import apply_fairness_downshift, to_rabbitmq_priority


async def effective_rabbitmq_priority(db: AsyncSession, job: Job) -> int:
    """The job's own priority, mapped to RabbitMQ's scale and (if it has an
    owner) fairness-downshifted by how many of that owner's other jobs are
    already in flight. `job` must already be committed (excluded from its
    own in-flight count by id).
    """
    base = to_rabbitmq_priority(job.priority)
    if not job.owner_id:
        return base

    in_flight = await db.scalar(
        select(func.count()).select_from(Job).where(
            Job.owner_id == job.owner_id,
            Job.id != job.id,
            Job.status.in_((JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING)),
        )
    )
    return apply_fairness_downshift(base, in_flight or 0)
