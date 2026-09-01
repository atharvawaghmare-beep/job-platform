# Distributed Job Processing & Workflow Platform

See [CLAUDE.md](CLAUDE.md) for the full spec, data model, API contract, and
phased build plan. This README covers how to run what currently exists.

## Status

**Phase 6 — Observability.** Structured JSON logging throughout (job id as
its own field, not just embedded in message text), a real Prometheus
`/metrics` endpoint, and a lightweight live status page at `/dashboard`.
This closes out all of CLAUDE.md's numbered build phases (0–6); only the
stretch Phase 7 (Kubernetes + autoscaling) remains.

## Running it

```bash
cp .env.example .env      # first time only
docker compose up --build
```

This starts Postgres, RabbitMQ, Redis, the API, and the worker. Then, apply
migrations once (see below), and:

- API: http://localhost:8000/health → `{"status": "ok"}`
- API docs: http://localhost:8000/docs
- RabbitMQ management UI: http://localhost:15672 (user/pass from `.env`)
- Postgres: `localhost:5433` (user/pass/db from `.env`; host port bumped from the default 5432 to avoid clashing with other local Postgres instances)
- Redis: `localhost:6379`

### Submitting a job manually

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "sleep", "payload": {"seconds": 3}}'
# => {"id": "...", "status": "queued", ...}

curl http://localhost:8000/jobs/<id-from-above>
# watch `status` go queued -> running -> succeeded, `result` -> {"slept_seconds": 3.0}
```

Worker logs (`docker compose logs -f worker`) show each job being claimed,
processed, and its outcome.

### Forcing failures (retry / dead-letter)

Three job types exist purely to exercise failure handling:

```bash
# Always fails -> retries with backoff -> dead_letter once max_retries is exhausted
curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"type": "fail", "payload": {"message": "demo"}, "max_retries": 2}'

# Fails ~60% of the time -> often recovers via retry instead of dying
curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"type": "flaky", "payload": {"fail_probability": 0.6}, "max_retries": 5}'

# An unrecognized `type` is treated the same as any other handler failure
curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"type": "bogus", "payload": {}}'
```

Poll `GET /jobs/{id}` and watch `retry_count` climb and `status` cycle
`queued -> running -> queued -> running -> ... -> dead_letter` (or
`succeeded`, for `flaky`). Two RabbitMQ queues back this — visible at
http://localhost:15672 — `jobs.retry` (a delay queue: messages sit there
briefly per a per-message TTL, then RabbitMQ auto-moves them back onto
`jobs`) and `jobs.dead_letter` (archival — one message per job that
exhausted its retries; nothing consumes it yet).

### Idempotency

Resubmitting the same `idempotency_key` with the same `type`/`payload`
returns the original job (`200`, same `id`) instead of creating a new one.
Resubmitting it with a different `type`/`payload` is a `409`:

```bash
curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"type": "sleep", "payload": {"seconds": 1}, "idempotency_key": "demo-key"}'
# run the exact same command again -> 200, same job id

curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"type": "sleep", "payload": {"seconds": 99}, "idempotency_key": "demo-key"}'
# -> 409 (same key, different payload)
```

Backed by a Redis cache in front of Postgres's unique constraint on
`idempotency_key` — the cache is a performance optimization only; if Redis
is down or empty, submissions still dedupe correctly via the DB, just with
one extra query.

### Horizontal scaling

```bash
docker compose up -d --scale worker=3
```

Multiple worker containers consume from the same `jobs` queue — RabbitMQ
round-robins messages across them (confirmed: 12 jobs submitted to 3
workers came back 4/4/4). Each job's `claimed_by` field (its processing
worker's hostname) and `claim_count` (how many times it was actually
claimed) are visible via `GET /jobs/{id}` — a healthy job should always
show `claim_count: 1`.

### Chaos test: kill a worker mid-job

```bash
pip install -r chaos/requirements.txt   # or use the venv below
python chaos/kill_worker_midjob.py
```

Submits a long job, waits for a worker to claim it, `docker kill`s that
specific container mid-execution, and asserts a *different* worker
completes it — exactly once (`claim_count >= 2` proves a reclaim happened;
`retry_count` alone wouldn't, since a killed process never raises the
exception that increments it). Automatically scales to 2 workers first if
fewer are running, and restores the killed container afterward.

### DAG workflows

```bash
curl -X POST http://localhost:8000/workflows -H "Content-Type: application/json" -d '{
  "name": "diamond-demo",
  "jobs": [
    {"key": "A", "type": "sleep", "payload": {"seconds": 1}},
    {"key": "B", "type": "sleep", "payload": {"seconds": 1}, "depends_on": ["A"]},
    {"key": "C", "type": "sleep", "payload": {"seconds": 1}, "depends_on": ["A"]},
    {"key": "D", "type": "sleep", "payload": {"seconds": 1}, "depends_on": ["B", "C"]}
  ]
}'
# => {"id": "...", "status": "running", "jobs": [...]}

curl http://localhost:8000/workflows/<id-from-above>
# watch workflow status go running -> completed, each job pending -> queued -> running -> succeeded
```

`key` is a request-local label only, used to express `depends_on` edges in
this one submission — the server resolves it to a real job id and returns
it as each job's `workflow_key` in the response. Jobs with no dependencies
are queued immediately; others wait for the worker's scheduler
(`worker/workflow_scheduler.py`) to unblock them once every dependency has
succeeded — with 2+ workers running, independent jobs (like B and C above)
genuinely execute in parallel, on different workers.

Make `A` fail instead (`{"type": "fail", "payload": {}, "max_retries": 0}`)
and B, C, D all end up `failed` ("blocked: dependency job(s) not
succeeded: [...]") without ever running — `started_at` stays `null` for all
three — and the workflow itself ends up `failed`.

An invalid DAG (an unknown `depends_on` key, a self-dependency, or a cycle)
is rejected with `422` and creates nothing at all.

### Priority + fairness

`priority` is an int, **lower = more urgent** (0–200; named tiers:
`0`=high, `100`=normal (default), `200`=low). It's mapped onto RabbitMQ's
native priority queues, so a high-priority job jumps ahead of a backlog of
lower-priority ones — demonstrate it with one worker running:

```bash
# Flood the queue with slow, low-priority jobs first...
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
    -d '{"type": "sleep", "payload": {"seconds": 2}, "priority": 200}' > /dev/null
done
# ...then submit one high-priority job, behind all 5 in arrival order.
curl -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"type": "sleep", "payload": {"seconds": 1}, "priority": 0}'
```

Watch `docker compose logs -f worker` — the high-priority job gets claimed
right after whichever low-priority job was already in flight, jumping
every other low-priority job still sitting in the backlog.

Optional per-owner fairness: pass `owner_id` on submission, and each
additional job an owner already has in flight (pending/queued/running)
shaves one priority level off their *next* submission — so a single owner
flooding the queue at a given tier gradually loses their edge relative to a
less-busy owner at that same tier. See `common/priority.py` for the exact
mapping and the reasoning for scoping it to submission time only (not
retries or DAG-unblocked jobs).

### Observability

**Structured logging.** Every application log line (worker + API) is a
single JSON object — `docker compose logs -f worker` now prints lines like:

```json
{"timestamp": "...", "level": "INFO", "logger": "worker", "message": "job claimed", "job_id": "...", "job_type": "sleep", "worker_hostname": "...", "retry_count": 0, "claim_count": 1}
```

job/workflow ids are their own top-level fields (not just embedded in the
message text), so a real log aggregator could filter/group by `job_id`
directly. Pipe through `jq` for readability: `docker compose logs -f worker | jq .`
(Uvicorn's own HTTP access log lines are untouched — see
`common/logging_config.py` for why.)

**`GET /metrics`** — real Prometheus exposition format: current job counts
per status (gauge), cumulative succeeded/failed/dead-lettered and total
retry attempts (counters), and live RabbitMQ queue depth per queue (gauge,
sourced from RabbitMQ's own Prometheus plugin rather than its classic
management HTTP API — the latter empirically lagged real state by ~8-10s in
testing; the former reflects a submitted backlog within ~1-2s).

**`GET /dashboard`** — a small self-contained live status page (no
external JS/CSS, no build step) that polls `/metrics` every 2s and draws a
live queue-depth chart plus stat tiles (queued/running/pending, cumulative
outcomes, retry total, computed throughput and failure rate). This is the
"lightweight status page" alternative Phase 6 offers instead of standing up
Prometheus + Grafana as two more containers — CLAUDE.md's tech stack
already marks Grafana as a stretch goal. Try it:

```bash
# with the stack up, open in a browser:
#   http://localhost:8000/dashboard
# then, in another terminal, generate a visible backlog:
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
    -d '{"type": "sleep", "payload": {"seconds": 4}}' > /dev/null
done
```

Watch the "jobs" line on the chart rise as the backlog forms, then fall
back down as the worker drains it — the Phase 6 DoD.

## Local dev environment (optional)

Nothing requires this — `api` and `worker` each install their own deps
inside their own Docker image, so the host Python is never actually used to
run the app. A venv is just for IDE support and running tests without
Docker:

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r api\requirements-dev.txt -r worker\requirements.txt -r chaos\requirements.txt
```

(One venv covers all three — they don't have conflicting dependencies at
this size. `api/requirements-dev.txt` already includes `api/requirements.txt`.)

## Running tests

Pure unit tests (validation, no Docker needed):

```bash
cd api
pytest    # or ..\.venv\Scripts\pytest on Windows if using the venv above
```

With the stack up (`docker compose up`), the same `pytest` run also
exercises `test_job_lifecycle_integration.py`, which submits real jobs
through the live API/queue/worker and asserts on the final state — otherwise
those tests are skipped automatically.

## Database migrations (Alembic)

Run **inside the api container** (not on the host) — `common/` and the
container-network Postgres hostname only exist there:

```bash
docker compose up -d postgres   # if not already running
docker compose exec api alembic revision --autogenerate -m "message"
docker compose exec api alembic upgrade head
```

## Repo layout

- `api/` — FastAPI app (async, SQLAlchemy + asyncpg)
- `worker/` — standalone consumer process (sync, SQLAlchemy + psycopg2 + pika)
- `common/` — ORM models shared by both (`common.models.Job`, `JobStatus`)
- `chaos/` — failure-injection scripts (`kill_worker_midjob.py`, run against the host — see above)

After editing `worker/` code, restart it to pick up the change (no
auto-reload for a plain consumer loop): `docker compose restart worker`. The
API's `uvicorn --reload` picks up `api/` and `common/` edits live.
