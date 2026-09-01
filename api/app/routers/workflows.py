"""Workflow submission and lookup — `POST /workflows`, `GET /workflows/{id}`.

A workflow is a client-defined DAG of jobs, referenced by a request-local
`key` string per job (e.g. "A", "B" — see app/dag.py) used only to express
`depends_on` edges within that one request; it isn't a durable identifier
elsewhere. The server resolves keys to real job UUIDs, validates the DAG,
and creates the workflow row + all its job rows in a single transaction —
either the whole thing exists or none of it does.

Jobs with no dependencies are queued immediately (same status-before-publish
ordering as api/app/routers/jobs.py, for the same reason: a worker must
never be able to see a message before the DB reflects `queued`). Jobs with
dependencies start at `pending` and wait for the worker-side scheduler (see
worker/main.py's `_advance_workflow_on_success` / `_cascade_workflow_failure`)
to unblock or block them as their dependencies resolve.
"""
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dag import DagValidationError, topological_order
from app.db import get_db
from app.priority_service import effective_rabbitmq_priority
from app.queue import publish_job
from common.models import Job, JobStatus, Workflow, WorkflowStatus
from common.priority import PRIORITY_NORMAL

router = APIRouter(tags=["workflows"])

DEFAULT_MAX_RETRIES = 3


class WorkflowJobCreate(BaseModel):
    key: str = Field(min_length=1)
    type: str = Field(min_length=1)
    payload: dict = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    priority: int | None = None
    max_retries: int | None = Field(default=None, ge=0)
    owner_id: str | None = None


class WorkflowCreate(BaseModel):
    name: str | None = None
    jobs: list[WorkflowJobCreate] = Field(min_length=1)


class WorkflowJobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    workflow_key: str | None
    type: str
    status: JobStatus
    depends_on: list[uuid.UUID]
    priority: int
    owner_id: str | None
    retry_count: int
    max_retries: int
    result: dict | None
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class WorkflowOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str | None
    status: WorkflowStatus
    created_at: datetime
    updated_at: datetime
    jobs: list[WorkflowJobOut]


async def _build_workflow_out(db: AsyncSession, workflow: Workflow) -> WorkflowOut:
    result = await db.execute(select(Job).where(Job.workflow_id == workflow.id))
    jobs = result.scalars().all()
    return WorkflowOut(
        id=workflow.id,
        name=workflow.name,
        status=workflow.status,
        created_at=workflow.created_at,
        updated_at=workflow.updated_at,
        jobs=[WorkflowJobOut.model_validate(j) for j in jobs],
    )


@router.post("/workflows", response_model=WorkflowOut, status_code=status.HTTP_201_CREATED)
async def create_workflow(body: WorkflowCreate, db: AsyncSession = Depends(get_db)) -> WorkflowOut:
    try:
        topological_order([(j.key, j.depends_on) for j in body.jobs])
    except DagValidationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    workflow = Workflow(id=uuid.uuid4(), name=body.name, status=WorkflowStatus.RUNNING)
    db.add(workflow)
    # Without an ORM relationship() between Job and Workflow, the unit of
    # work has no reason to insert `workflows` before a batched `jobs`
    # insert in the same flush — an explicit flush here (not a commit; still
    # rolls back together with the jobs below on error) guarantees the
    # workflow row exists before any job's FK references it.
    await db.flush()

    key_to_id = {j.key: uuid.uuid4() for j in body.jobs}
    root_jobs: list[Job] = []
    for j in body.jobs:
        is_root = not j.depends_on
        job = Job(
            id=key_to_id[j.key],
            workflow_id=workflow.id,
            workflow_key=j.key,
            type=j.type,
            payload=j.payload,
            priority=j.priority if j.priority is not None else PRIORITY_NORMAL,
            max_retries=j.max_retries if j.max_retries is not None else DEFAULT_MAX_RETRIES,
            owner_id=j.owner_id,
            depends_on=[key_to_id[dep] for dep in j.depends_on],
            status=JobStatus.QUEUED if is_root else JobStatus.PENDING,
        )
        db.add(job)
        if is_root:
            root_jobs.append(job)

    # Single commit: the workflow row and every job row, with root jobs
    # already `queued`, land together or not at all.
    await db.commit()

    publish_failures = []
    for job in root_jobs:
        try:
            priority = await effective_rabbitmq_priority(db, job)
            await publish_job(str(job.id), priority)
        except Exception:
            publish_failures.append(job.id)

    await db.refresh(workflow)
    workflow_out = await _build_workflow_out(db, workflow)

    if publish_failures:
        # Everything is durably persisted (workflow + all jobs); these
        # specific root jobs are stuck at `queued` with no matching queue
        # message — same known, documented gap as api/app/routers/jobs.py.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"workflow created but failed to enqueue root jobs: {publish_failures}",
        )

    return workflow_out


@router.get("/workflows/{workflow_id}", response_model=WorkflowOut)
async def get_workflow(workflow_id: uuid.UUID, db: AsyncSession = Depends(get_db)) -> WorkflowOut:
    workflow = await db.get(Workflow, workflow_id)
    if workflow is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="workflow not found")
    return await _build_workflow_out(db, workflow)
