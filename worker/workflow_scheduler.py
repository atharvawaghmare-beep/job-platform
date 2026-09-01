"""Worker-side DAG scheduler (Phase 4): reacts to a workflow job reaching a
terminal state.

- On success: finds `pending` jobs in the same workflow that directly
  depend on the job that just succeeded, and for each whose *entire*
  dependency set has now succeeded, atomically flips it to `queued` and
  publishes it. Also marks the workflow `completed` once every one of its
  jobs has succeeded.
- On a job's final failure (`dead_letter`): cascades a `failed` ("blocked")
  status to every downstream job in the workflow, transitively, and marks
  the workflow `failed`. A blocked job is never queued — that's what
  "a failure in A blocks B, C, D" (CLAUDE.md Phase 4 DoD) means in
  practice: no message is ever published for it, so it's never claimed,
  never run.

Both paths use the same atomic-conditional-UPDATE pattern as the worker's
own job claim (main.py's `_claim_job`) to stay race-safe: two dependencies
of the same downstream job finishing at nearly the same moment, on
different workers, can't double-queue (or double-block) that job.
"""
import json
import logging
from datetime import datetime, timezone

import pika
from sqlalchemy import select, update

from common.models import Job, JobStatus, Workflow, WorkflowStatus
from common.priority import to_rabbitmq_priority

logger = logging.getLogger("worker")

QUEUE_NAME = "jobs"


def _publish_job(channel, job_id, priority: int) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps({"job_id": str(job_id)}).encode(),
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent, priority=priority),
    )


def _direct_dependents(session, workflow_id, job_id) -> list[Job]:
    """Jobs in the same workflow whose `depends_on` array contains `job_id`."""
    return list(
        session.execute(
            select(Job).where(Job.workflow_id == workflow_id, Job.depends_on.any(job_id))
        ).scalars()
    )


def _try_queue_job(session, job_id) -> bool:
    """Atomically flips a job from `pending` to `queued`. Only the caller
    that wins this (returns True) should publish a message — otherwise a
    job with multiple dependencies finishing near-simultaneously, on
    different workers, could get queued (and published) twice.
    """
    result = session.execute(
        update(Job).where(Job.id == job_id, Job.status == JobStatus.PENDING).values(status=JobStatus.QUEUED)
    )
    session.commit()
    return result.rowcount > 0


def _maybe_complete_workflow(session, workflow_id) -> None:
    statuses = session.execute(select(Job.status).where(Job.workflow_id == workflow_id)).scalars().all()
    if statuses and all(s == JobStatus.SUCCEEDED for s in statuses):
        workflow = session.get(Workflow, workflow_id)
        if workflow is not None and workflow.status != WorkflowStatus.COMPLETED:
            workflow.status = WorkflowStatus.COMPLETED
            session.commit()


def advance_workflow_on_success(session, channel, completed_job: Job) -> None:
    if completed_job.workflow_id is None:
        return

    for candidate in _direct_dependents(session, completed_job.workflow_id, completed_job.id):
        if candidate.status != JobStatus.PENDING:
            continue  # already queued/running/blocked via another path

        dep_statuses = session.execute(
            select(Job.status).where(Job.id.in_(candidate.depends_on))
        ).scalars().all()
        all_deps_succeeded = len(dep_statuses) == len(candidate.depends_on) and all(
            s == JobStatus.SUCCEEDED for s in dep_statuses
        )
        if all_deps_succeeded and _try_queue_job(session, candidate.id):
            _publish_job(channel, candidate.id, to_rabbitmq_priority(candidate.priority))
            logger.info(
                "downstream job unblocked and queued",
                extra={
                    "job_id": str(candidate.id), "workflow_id": str(completed_job.workflow_id),
                    "unblocked_by_job_id": str(completed_job.id),
                },
            )

    _maybe_complete_workflow(session, completed_job.workflow_id)


def cascade_workflow_failure(session, failed_job: Job) -> None:
    if failed_job.workflow_id is None:
        return

    workflow_id = failed_job.workflow_id
    to_visit = list(_direct_dependents(session, workflow_id, failed_job.id))
    seen = {j.id for j in to_visit}

    while to_visit:
        current = to_visit.pop()
        if current.status != JobStatus.PENDING:
            continue  # already resolved via another path — don't reprocess

        unmet = [
            str(dep_id)
            for dep_id in current.depends_on
            if (dep := session.get(Job, dep_id)) is None or dep.status != JobStatus.SUCCEEDED
        ]

        current.status = JobStatus.FAILED
        current.error = f"blocked: dependency job(s) not succeeded: {unmet}"
        current.finished_at = datetime.now(timezone.utc)
        session.commit()
        logger.info(
            "downstream job blocked by upstream failure",
            extra={
                "job_id": str(current.id), "workflow_id": str(workflow_id),
                "blocked_by_job_id": str(failed_job.id), "unmet_dependencies": unmet,
            },
        )

        for dependent in _direct_dependents(session, workflow_id, current.id):
            if dependent.id not in seen:
                seen.add(dependent.id)
                to_visit.append(dependent)

    workflow = session.get(Workflow, workflow_id)
    if workflow is not None:
        workflow.status = WorkflowStatus.FAILED
        session.commit()
