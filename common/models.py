"""SQLAlchemy ORM models shared by the API (async) and worker (sync).

The `Job` table matches the data model in /CLAUDE.md in full — including
columns like `depends_on` and `priority` that aren't acted on until later
phases (DAG scheduling in Phase 4, priority ordering in Phase 5) — so the
schema doesn't need reshaping as each phase lands, only new behavior wired
on top of it.

Both api/app/db.py (async engine) and worker/db.py (sync engine) bind their
own `Session`/`AsyncSession` to these same classes.
"""
import enum
import uuid
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class WorkflowStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Workflow(Base):
    """A named DAG of jobs. See Job.depends_on / Job.workflow_id for how
    edges and membership are represented — this table only tracks the
    workflow's own identity and overall status, set by the worker-side
    scheduler in worker/main.py as member jobs complete.
    """

    __tablename__ = "workflows"

    id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)

    status: Mapped[WorkflowStatus] = mapped_column(
        sa.Enum(WorkflowStatus, name="workflow_status", native_enum=True, values_callable=lambda enum_cls: [e.value for e in enum_cls]),
        nullable=False,
        default=WorkflowStatus.PENDING,
    )

    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False
    )


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Unique per the data model, but NOT enforced as a dedupe check in the API
    # yet — that's a Phase 3 deliverable ("idempotency key handling: dedupe on
    # submit + on redelivery"). The DB constraint is added now so the schema
    # is right from the start; until Phase 3, a duplicate key submission just
    # gets a plain 409 (see api/app/routers/jobs.py) rather than the smarter
    # "return the existing job" behavior the full API contract describes.
    idempotency_key: Mapped[str | None] = mapped_column(sa.String(255), nullable=True, unique=True)

    type: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    status: Mapped[JobStatus] = mapped_column(
        # values_callable makes Postgres store the lowercase `.value`s
        # ("pending", "queued", ...) rather than SQLAlchemy's default of the
        # member *names* — matches the data model doc and keeps raw SQL
        # against the DB unsurprising.
        sa.Enum(JobStatus, name="job_status", native_enum=True, values_callable=lambda enum_cls: [e.value for e in enum_cls]),
        nullable=False,
        default=JobStatus.PENDING,
    )

    priority: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=100)

    # DAG edges (Phase 4). Empty for every job until then.
    depends_on: Mapped[list[uuid.UUID]] = mapped_column(
        ARRAY(PGUUID(as_uuid=True)), nullable=False, default=list
    )

    retry_count: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=3)

    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True), nullable=True)

    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(sa.Text, nullable=True)

    owner_id: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)

    # --- Phase 3 additions (not in the original CLAUDE.md data model table,
    # added for horizontal-scaling observability + the chaos test) ---

    # Hostname of whichever worker process most recently claimed this job.
    # Distinct from `owner_id` above, which is the *submitting user* — this
    # is the *executing worker*.
    claimed_by: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)

    # How many times this job's row was successfully claimed for execution —
    # NOT the same as retry_count. retry_count only increments on a handler
    # *failure* (drives the backoff/dead-letter decision). claim_count also
    # increments on a crash-triggered reclaim (a worker killed mid-job never
    # raises an exception; the process just vanishes) — that's not a
    # "failure" in the retry sense, but it is a second claim, and it's the
    # evidence the chaos test checks for.
    claim_count: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0)

    # --- Phase 4 additions (DAG workflows) ---

    # NULL for a standalone job (submitted via plain POST /jobs). Set only
    # by POST /workflows, which creates the workflow + all its member jobs
    # together — see api/app/routers/workflows.py.
    workflow_id: Mapped[uuid.UUID | None] = mapped_column(
        PGUUID(as_uuid=True), sa.ForeignKey("workflows.id"), nullable=True
    )

    # The client-supplied local label for this job within its workflow
    # submission (e.g. "A", "B" in a diamond DAG) — used only to express
    # `depends_on` edges in the request and for readability in responses.
    # Not a unique identifier outside that one request.
    workflow_key: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)
