# Distributed Job Processing & Workflow Platform

## Project Purpose
Portfolio project for backend/distributed-systems internship applications. The system lets users submit long-running jobs via a REST API. Jobs are queued and processed asynchronously by independent, horizontally-scalable worker processes. The platform supports job dependencies (DAG workflows), idempotent retries, priority scheduling, and observability — not just a bare producer/consumer demo.

**Guiding principle for whoever (human or Claude) is building this:** favor correctness and clearly-reasoned trade-offs over speed. Every design decision should be something the author can defend in an interview.

---

## Tech Stack (default — confirm before deviating)
- **API**: FastAPI (Python 3.11+)
- **Queue**: RabbitMQ
- **Metadata/state store**: PostgreSQL
- **Fast lookups / locks / dedupe**: Redis
- **Workers**: standalone Python processes, containerized, horizontally scalable
- **Orchestration**: Docker Compose (local), optionally Kubernetes manifests later
- **Observability**: structured JSON logging + Prometheus metrics + Grafana dashboard (stretch goal)
- **Testing**: pytest, plus a `chaos/` folder for failure-injection scripts

Do not silently swap stack pieces. If a change seems justified while building, stop and flag it rather than proceeding.

---

## Core Concepts & Data Model

### Job
| field | type | notes |
|---|---|---|
| id | UUID | primary key |
| idempotency_key | string, unique nullable | client-supplied, used to dedupe resubmits |
| type | string | e.g. "resize_image", "generate_report" |
| payload | JSONB | job-specific input |
| status | enum | `pending`, `queued`, `running`, `succeeded`, `failed`, `dead_letter` |
| priority | int | lower = higher priority (or use named tiers: high/normal/low) |
| depends_on | UUID[] | job IDs that must succeed first (DAG edges) |
| retry_count | int | current attempt number |
| max_retries | int | default 3 |
| created_at / updated_at / started_at / finished_at | timestamp | |
| result | JSONB nullable | output on success |
| error | text nullable | last error message |
| owner_id | string | for fairness/priority-per-user later |

### Workflow (optional grouping)
A workflow is a named DAG of jobs. Store as `workflow_id` on each job plus a `workflows` table tracking overall status (`pending`/`running`/`completed`/`failed`).

---

## API Contract (v1)

- `POST /jobs` — submit a job. Body: `{type, payload, priority?, depends_on?, idempotency_key?, max_retries?}`. Returns `201` with job id + status. If `idempotency_key` already exists, return the existing job (200), don't create a duplicate.
- `GET /jobs/{id}` — job status + result/error.
- `GET /jobs?status=&owner_id=&type=` — list/filter jobs (paginated).
- `POST /jobs/{id}/cancel` — best-effort cancel (mark cancelled if not yet picked up; can't hard-kill an in-flight worker in MVP).
- `POST /workflows` — submit a DAG of jobs atomically.
- `GET /workflows/{id}` — workflow + child job statuses.
- `GET /health` — liveness.
- `GET /metrics` — Prometheus format (queue depth, jobs/sec, failure rate, worker count if trackable).

All responses JSON. Use proper HTTP status codes (404 for missing job, 409 for conflicting idempotency key with different payload, 422 for validation errors).

---

## Architecture Flow
1. Client `POST /jobs` → API validates payload, writes row to Postgres with status `pending`, publishes message to RabbitMQ with status `queued`.
2. Worker consumes message, claims job (update status to `running`, set `started_at`), executes job handler for `type`.
3. On success: update status `succeeded`, store `result`. If job is part of a DAG, check if downstream jobs are now unblocked and enqueue them.
4. On failure: increment `retry_count`. If `< max_retries`, requeue with backoff (exponential, jittered). If exhausted, move to `dead_letter` queue and mark status `dead_letter`.
5. Worker acks the message only after DB state is durably updated — never ack-then-crash-before-persisting (this is the core "at-least-once" guarantee to get right).
6. Idempotency: before executing, worker checks Redis (or a DB unique constraint) on `idempotency_key` to avoid double-processing on redelivery.

---

## Build Phases (work through in order; each phase should be fully working + tested before moving on)

### Phase 0 — Scaffolding
- Repo structure, Docker Compose with Postgres + RabbitMQ + Redis, FastAPI skeleton with `/health`.
- DB migrations tool set up (alembic).
- Definition of done: `docker compose up` brings up all services; `/health` returns 200.

### Phase 1 — Basic job submission + single worker
- `POST /jobs`, `GET /jobs/{id}` implemented.
- One worker type (pick something simple, e.g. "sleep and return payload" or "resize an image").
- Worker consumes from queue, updates status, no retries yet.
- DoD: submit a job via curl, watch it go pending → queued → running → succeeded.

### Phase 2 — Retries, failure handling, dead-letter queue
- Simulate failures (random or forced), implement retry with exponential backoff + jitter.
- Dead-letter queue + `dead_letter` status after max retries.
- DoD: force a job to fail repeatedly, confirm it lands in dead-letter and stops retrying.

### Phase 3 — Idempotency + horizontal worker scaling
- Idempotency key handling (dedupe on submit + on redelivery).
- Run multiple worker containers (`docker compose up --scale worker=3`), confirm jobs distribute and no job is double-processed.
- DoD: kill a worker mid-job (chaos script), confirm another worker picks up the job exactly once.

### Phase 4 — DAG workflows
- `depends_on` support, `POST /workflows`.
- Scheduler logic: a job only becomes eligible for the queue once all its dependencies have succeeded; if a dependency fails, downstream jobs are marked `failed` (or `blocked`) rather than run.
- DoD: submit a 3-job diamond-shaped DAG (A → B, A → C, B+C → D), confirm correct ordering and that a failure in A blocks B, C, D.

### Phase 5 — Priority + fairness
- Multiple priority queues or a priority field consumed in order.
- Optional: per-owner fairness so one user can't starve others.
- DoD: submit high and low priority jobs from a full queue, confirm high priority is processed first.

### Phase 6 — Observability
- Structured logging (job id in every log line).
- Prometheus metrics endpoint: queue depth, throughput, failure rate, retry count.
- Simple Grafana dashboard or a lightweight status page.
- DoD: dashboard visibly shows queue depth rising/falling as jobs are submitted/processed.

### Phase 7 (stretch) — Kubernetes + autoscaling
- K8s manifests, HPA scaling workers based on queue depth (KEDA or custom metric).
- DoD: load test, confirm worker pod count scales up under load and back down after.

---

## Testing Expectations
- Unit tests for job state transitions and DAG dependency resolution logic.
- Integration test: submit job through real API + queue + worker in Docker Compose, assert final state.
- `chaos/kill_worker_midjob.sh` (or Python equivalent): starts a job, kills the worker process partway through, asserts the job completes exactly once via another worker.

## Coding Conventions
- Type hints everywhere (Python).
- Every module gets a docstring explaining its role in the flow above.
- No silent `except: pass` — failures must be logged with job id and either retried or dead-lettered, never swallowed.
- Config via environment variables (12-factor), never hardcoded connection strings.

## Working Notes for Claude Code
- Work phase by phase. Do not jump ahead to Phase 4 features while Phase 1 is incomplete.
- After finishing a phase, summarize what was built and how to manually verify it (exact commands) before moving on.
- If a requirement here is ambiguous, make the most reasonable engineering decision, note the assumption in a comment or commit message, and proceed — don't stall waiting for clarification unless it would mean redoing significant work.
- Prefer readable, defensible code over clever code. This is a resume piece meant to be discussed in interviews.
