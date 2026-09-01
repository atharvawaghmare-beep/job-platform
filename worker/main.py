"""Worker entrypoint.

Consumes job messages from RabbitMQ, executes the matching handler, and
persists the outcome to Postgres — acking the message only after that
commit (and any follow-up publish, see below) succeeds. That ordering is
deliberate: a crash between "handler ran" and "DB commit" must cause
RabbitMQ to redeliver the message, never silently drop it (the
at-least-once guarantee called out in /CLAUDE.md).

Retry/backoff/dead-letter (Phase 2): on failure, `retry_count` is
incremented. If it's still within `max_retries`, the job goes back to
`queued` and its id is republished — not straight back onto `jobs`, but
onto `jobs.retry`, a queue nothing consumes from, with a per-message TTL
equal to the computed backoff delay (see backoff.py). `jobs.retry` is
declared with `jobs` as its dead-letter target, so once a message's TTL
expires, RabbitMQ moves it onto `jobs` on its own — that's the whole
delay mechanism, no polling or scheduler needed. This is the standard
"delay queue via per-message TTL + DLX" pattern: RabbitMQ has no native
delayed-delivery without a plugin, and this needs none.

Caveat worth knowing: RabbitMQ only checks the *head* of a queue for TTL
expiry. If many jobs with different backoff delays are in flight at once, a
longer-delay message stuck at the head can hold up shorter-delay messages
behind it. Not a concern at this project's scale; a fleet of fixed-delay
"bucket" queues (or the delayed-message-exchange plugin) would be the fix
if it ever became one.

Once `retry_count` exceeds `max_retries`, the job is terminal: status
`dead_letter`, and its id is published (once, for visibility/inspection —
nothing consumes it yet) to `jobs.dead_letter`.

Exactly-once execution under at-least-once delivery (Phase 3): a message
can be delivered more than once for the same job — most commonly, this
worker (or another replica, once horizontally scaled) crashes after
committing a terminal DB state but before acking, so RabbitMQ redelivers.
The claim step below is an atomic
`UPDATE jobs SET status='running', ... WHERE id=:id AND status IN
('queued','running') RETURNING id`. If the job is already terminal
(succeeded/failed/dead_letter), that WHERE clause matches nothing, the
claim affects 0 rows, and the message is acked as a no-op — that's the
redelivery dedupe. This also correctly covers the chaos-test case: a worker
killed mid-job never reaches a terminal status, so the job is still
'running' (or 'queued', if it crashed before even claiming), and the WHERE
clause lets a *different* worker reclaim and actually run it.

Known limitation, worth being upfront about: this is a status-based claim,
not a leased one. If RabbitMQ redelivers because of a missed heartbeat
while the original worker is still genuinely alive and running the handler
(pika's BlockingConnection can't process heartbeat frames during a long
blocking call — see the heartbeat setting below for the mitigation), a
second worker could reclaim and run the same handler concurrently with the
first. For this project's demo handlers (side-effect-free) that's harmless;
a handler with real side effects would need a leased claim
(`claimed_by` + an expiry the claimant renews) instead. Noted rather than
solved — solving it properly is closer to reimplementing a chunk of what a
task-queue library like Celery already does.

DAG workflows (Phase 4): a job created via `POST /workflows` carries a
`workflow_id` and populated `depends_on`. When such a job succeeds or
dead-letters, see workflow_scheduler.py for what happens next — downstream
jobs get unblocked (queued + published) or blocked (`failed`, never
queued), and the workflow's own status is kept in sync. Standalone jobs
(`workflow_id is None`) are completely unaffected by any of this.

Priority (Phase 5): the `jobs` queue is declared with `x-max-priority` (see
common/priority.py), so a retried job's republish onto `jobs.retry` carries
its priority through — the DLX-forward back onto `jobs` preserves message
properties, priority included. Fairness downshifting is deliberately NOT
reapplied here: it's an admission-time concern (see common/priority.py's
`apply_fairness_downshift` docstring), so a retry just keeps the priority
it already had.
"""
import json
import logging
import socket
import time
import uuid
from datetime import datetime, timezone

import pika
from sqlalchemy import update

from backoff import compute_backoff_delay_seconds
from config import settings
from db import SessionLocal
from handlers import HANDLERS
from workflow_scheduler import advance_workflow_on_success, cascade_workflow_failure

from common.logging_config import configure_logging
from common.models import Job, JobStatus
from common.priority import RABBITMQ_MAX_PRIORITY, to_rabbitmq_priority

configure_logging()
logger = logging.getLogger("worker")

QUEUE_NAME = "jobs"
RETRY_QUEUE_NAME = "jobs.retry"
DEAD_LETTER_QUEUE_NAME = "jobs.dead_letter"

WORKER_HOSTNAME = socket.gethostname()

# Generous relative to MAX_SLEEP_SECONDS (handlers.py, 30s): pika's
# BlockingConnection can't send/process heartbeat frames while a handler is
# synchronously blocking (e.g. time.sleep), so a heartbeat timeout shorter
# than the longest handler risks RabbitMQ deciding a live worker is dead and
# redelivering its message elsewhere — see the module docstring.
HEARTBEAT_SECONDS = 120


def _publish_retry(channel, job_id: uuid.UUID, delay_seconds: float, priority: int) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=RETRY_QUEUE_NAME,
        body=json.dumps({"job_id": str(job_id)}).encode(),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            expiration=str(int(delay_seconds * 1000)),  # per-message TTL, in ms
            priority=priority,
        ),
    )


def _publish_dead_letter(channel, job_id: uuid.UUID, error: str, retry_count: int) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=DEAD_LETTER_QUEUE_NAME,
        body=json.dumps({"job_id": str(job_id), "error": error, "retry_count": retry_count}).encode(),
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
    )


def _claim_job(session, job_id: uuid.UUID) -> Job | None:
    """Atomically transition the job to `running`, recording who claimed it.
    Returns the refreshed Job if this call actually won the claim, or None
    if there was nothing claimable (already terminal, or no such job) — see
    the module docstring for why this is the whole redelivery-dedupe and
    horizontal-scaling-safety mechanism.
    """
    claim_stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status.in_((JobStatus.QUEUED, JobStatus.RUNNING)))
        .values(
            status=JobStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
            claimed_by=WORKER_HOSTNAME,
            claim_count=Job.claim_count + 1,
        )
    )
    result = session.execute(claim_stmt)
    session.commit()

    if result.rowcount == 0:
        return None
    return session.get(Job, job_id)


def process_message(channel, method, _properties, body: bytes) -> None:
    try:
        message = json.loads(body)
        job_id = uuid.UUID(message["job_id"])
    except (json.JSONDecodeError, KeyError, ValueError):
        logger.exception("dropping unparseable message", extra={"raw_body": body.decode(errors="replace")})
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    session = SessionLocal()
    try:
        job = _claim_job(session, job_id)
        if job is None:
            existing = session.get(Job, job_id)
            if existing is None:
                logger.error("job not found in DB, dropping message", extra={"job_id": str(job_id)})
            else:
                logger.info(
                    "job already at terminal status, skipping duplicate delivery",
                    extra={"job_id": str(job_id), "status": existing.status.value},
                )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        log_ctx = {
            "job_id": str(job.id), "job_type": job.type, "worker_hostname": WORKER_HOSTNAME,
            "retry_count": job.retry_count, "claim_count": job.claim_count,
        }
        logger.info("job claimed", extra=log_ctx)

        handler = HANDLERS.get(job.type)
        try:
            if handler is None:
                raise ValueError(f"unknown job type: {job.type!r}")
            result = handler(job.payload)
        except Exception as exc:  # noqa: BLE001 - a bad handler must not crash the worker
            logger.exception(
                "job failed",
                extra={**log_ctx, "attempt": job.retry_count + 1, "error": str(exc)},
            )
            job.retry_count += 1
            job.error = str(exc)

            if job.retry_count <= job.max_retries:
                job.status = JobStatus.QUEUED
                session.commit()
                delay = compute_backoff_delay_seconds(job.retry_count)
                logger.info(
                    "job scheduled for retry",
                    extra={
                        "job_id": str(job.id), "retry_count": job.retry_count,
                        "max_retries": job.max_retries, "delay_seconds": round(delay, 2),
                    },
                )
                _publish_retry(channel, job.id, delay, to_rabbitmq_priority(job.priority))
            else:
                job.status = JobStatus.DEAD_LETTER
                job.finished_at = datetime.now(timezone.utc)
                session.commit()
                logger.error(
                    "job exhausted retries, moved to dead_letter",
                    extra={
                        "job_id": str(job.id), "retry_count": job.retry_count,
                        "max_retries": job.max_retries,
                    },
                )
                _publish_dead_letter(channel, job.id, job.error, job.retry_count)
                if job.workflow_id is not None:
                    cascade_workflow_failure(session, job)
                    logger.info(
                        "cascaded failure through workflow",
                        extra={"job_id": str(job.id), "workflow_id": str(job.workflow_id)},
                    )
        else:
            job.status = JobStatus.SUCCEEDED
            job.result = result
            job.finished_at = datetime.now(timezone.utc)
            session.commit()
            logger.info("job succeeded", extra={**log_ctx, "result": result})
            if job.workflow_id is not None:
                advance_workflow_on_success(session, channel, job)
    finally:
        session.close()

    # Ack only now that the terminal (or retry-scheduled) state is durably
    # committed, and any follow-up publish above has gone through.
    channel.basic_ack(delivery_tag=method.delivery_tag)


def _connect_with_retry(max_attempts: int = 10, delay_seconds: float = 3.0) -> pika.BlockingConnection:
    """RabbitMQ's healthcheck can report healthy slightly before its AMQP
    listener is actually bound (the Erlang node comes up first) — so even
    with `depends_on: condition: service_healthy`, the very first connection
    attempt can race a connection refusal. This is startup resiliency, not
    the job-level retry/backoff Phase 2 adds.
    """
    params = pika.URLParameters(settings.rabbitmq_url)
    params.heartbeat = HEARTBEAT_SECONDS

    for attempt in range(1, max_attempts + 1):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            if attempt == max_attempts:
                raise
            logger.warning(
                "RabbitMQ not ready yet (attempt %d/%d), retrying in %.0fs",
                attempt, max_attempts, delay_seconds,
            )
            time.sleep(delay_seconds)
    raise RuntimeError("unreachable")  # pragma: no cover


def main() -> None:
    connection = _connect_with_retry()
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True, arguments={"x-max-priority": RABBITMQ_MAX_PRIORITY})
    channel.queue_declare(
        queue=RETRY_QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": QUEUE_NAME,
        },
    )
    channel.queue_declare(queue=DEAD_LETTER_QUEUE_NAME, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_message)

    logger.info("worker started (hostname=%s), waiting for jobs on queue %r", WORKER_HOSTNAME, QUEUE_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
