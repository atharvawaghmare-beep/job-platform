"""`GET /metrics` — Prometheus exposition format text (CLAUDE.md Phase 6:
"queue depth, throughput, failure rate, retry count").

Two categories of data:
- Job-level state, straight from Postgres (source of truth): how many jobs
  are in each status right now (gauge), and cumulative totals for terminal
  statuses plus summed retry attempts (counters — legitimately monotonic
  here, since jobs are never deleted).
- Broker-level queue depth, from RabbitMQ's own Prometheus plugin
  (its "detailed" per-object endpoint — see `_rabbitmq_queue_depths`):
  the actual number of messages sitting in each queue right now.
  Deliberately a different signal from the DB status counts above — a job
  "in backoff" and a job "freshly queued" are both DB status=queued, but
  only one of them is sitting in `jobs.retry` at the broker level, and
  that's usually what "queue depth" means operationally. If the endpoint
  can't be reached, that queue's metric is simply omitted (not reported as
  a misleading 0).

This exposes raw counters/gauges, not pre-computed rates — that's the
standard Prometheus pattern (a real Prometheus server's `rate()`/`irate()`
derives throughput and failure rate from these over time). This project
doesn't run an actual Prometheus server (see README for why); the
lightweight status page at GET /dashboard does the equivalent client-side
by diffing successive scrapes.
"""
import httpx
from fastapi import APIRouter, Depends, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from common.models import Job, JobStatus

router = APIRouter(tags=["metrics"])

RABBITMQ_QUEUES = ("jobs", "jobs.retry", "jobs.dead_letter")
TERMINAL_STATUSES = (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTER)


async def _job_status_counts(db: AsyncSession) -> dict[str, int]:
    result = await db.execute(select(Job.status, func.count()).group_by(Job.status))
    counts = {status.value: 0 for status in JobStatus}
    for status_value, count in result.all():
        counts[status_value.value] = count
    return counts


async def _retry_count_total(db: AsyncSession) -> int:
    return (await db.scalar(select(func.coalesce(func.sum(Job.retry_count), 0)))) or 0


async def _rabbitmq_queue_depths() -> dict[str, int | None]:
    """Queries RabbitMQ's own Prometheus plugin "detailed" endpoint (a
    single request covers all queues) rather than its classic HTTP
    management API — see the module docstring for why: the management
    API's queue depth numbers come from a periodically-refreshed stats
    database and empirically lagged real state by ~8-10s in testing,
    whereas this endpoint computes per-object metrics on demand.
    """
    depths: dict[str, int | None] = {queue: None for queue in RABBITMQ_QUEUES}
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(
                f"{settings.rabbitmq_prometheus_url}/metrics/detailed",
                params={"family": "queue_coarse_metrics"},
            )
            resp.raise_for_status()
        for line in resp.text.splitlines():
            if not line.startswith("rabbitmq_detailed_queue_messages{"):
                continue
            for queue in RABBITMQ_QUEUES:
                if f'queue="{queue}"' in line:
                    depths[queue] = int(float(line.rsplit(" ", 1)[1]))
    except httpx.HTTPError:
        pass  # leave everything None — omitted from the exposition output below
    return depths


def render_prometheus_text(
    status_counts: dict[str, int], retry_total: int, queue_depths: dict[str, int | None]
) -> str:
    """Pure formatting — no I/O — so this is unit-testable without a live
    DB/RabbitMQ (see api/tests/test_metrics_unit.py).
    """
    lines = [
        "# HELP job_status_count Current number of jobs in each status.",
        "# TYPE job_status_count gauge",
    ]
    for status_value, count in status_counts.items():
        lines.append(f'job_status_count{{status="{status_value}"}} {count}')

    lines += [
        "# HELP jobs_terminal_total Cumulative jobs that have reached a terminal status.",
        "# TYPE jobs_terminal_total counter",
    ]
    for status in TERMINAL_STATUSES:
        lines.append(f'jobs_terminal_total{{status="{status.value}"}} {status_counts.get(status.value, 0)}')

    lines += [
        "# HELP job_retry_count_total Cumulative retry attempts summed across all jobs.",
        "# TYPE job_retry_count_total counter",
        f"job_retry_count_total {retry_total}",
    ]

    lines += [
        "# HELP rabbitmq_queue_messages Current message count in a RabbitMQ queue (via RabbitMQ's own Prometheus plugin).",
        "# TYPE rabbitmq_queue_messages gauge",
    ]
    for queue, depth in queue_depths.items():
        if depth is not None:
            lines.append(f'rabbitmq_queue_messages{{queue="{queue}"}} {depth}')

    return "\n".join(lines) + "\n"


@router.get("/metrics")
async def metrics(db: AsyncSession = Depends(get_db)) -> Response:
    status_counts = await _job_status_counts(db)
    retry_total = await _retry_count_total(db)
    queue_depths = await _rabbitmq_queue_depths()
    body = render_prometheus_text(status_counts, retry_total, queue_depths)
    return Response(content=body, media_type="text/plain; version=0.0.4; charset=utf-8")
