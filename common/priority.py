"""Priority mapping shared by the API (initial publish) and worker (retry /
DAG-unblock republish) — both need to agree on how the job model's own
`priority` field (int, lower = more urgent — see the data model in
/CLAUDE.md) maps onto RabbitMQ's native priority queues (`x-max-priority`),
which use the opposite convention (higher number = delivered first) and
must be declared with the same ceiling everywhere the `jobs` queue is
declared (api/app/queue.py and worker/main.py).

RabbitMQ's own guidance is to keep the priority range small (a handful of
levels, not hundreds) for performance — hence RABBITMQ_MAX_PRIORITY = 10
rather than mirroring the full job-priority range 1:1.
"""

MIN_JOB_PRIORITY = 0  # most urgent
MAX_JOB_PRIORITY = 200  # least urgent
RABBITMQ_MAX_PRIORITY = 10

# Named tiers for convenience (CLAUDE.md: "or use named tiers: high/normal/low").
# Any int in [MIN_JOB_PRIORITY, MAX_JOB_PRIORITY] works too, mapped proportionally.
PRIORITY_HIGH = 0
PRIORITY_NORMAL = 100
PRIORITY_LOW = 200


def to_rabbitmq_priority(job_priority: int) -> int:
    """Inverts and clamps our priority (lower = more urgent) into RabbitMQ's
    [0, RABBITMQ_MAX_PRIORITY] scale (higher = delivered first).
    """
    clamped = max(MIN_JOB_PRIORITY, min(job_priority, MAX_JOB_PRIORITY))
    urgency_fraction = 1 - (clamped - MIN_JOB_PRIORITY) / (MAX_JOB_PRIORITY - MIN_JOB_PRIORITY)
    return round(urgency_fraction * RABBITMQ_MAX_PRIORITY)


def apply_fairness_downshift(rabbitmq_priority: int, owner_in_flight_count: int) -> int:
    """Per-owner fairness (CLAUDE.md Phase 5, optional): every job an owner
    already has in flight (pending/queued/running) shaves one level off the
    RabbitMQ priority of their *next* submission, floored at 0. A simple
    anti-monopoly mechanism — an owner flooding the queue quickly loses
    their priority edge relative to a less-busy owner at the same nominal
    tier — chosen over per-owner queues plus a custom fair-dispatch
    consumer, which would be considerably more machinery for what's an
    optional, demo-scale requirement here.

    Applied only at submission time (POST /jobs, POST /workflows' root
    jobs) — not on retry or DAG-unblock republishing, since fairness among
    *fresh admissions* from different owners is the actual concern; a retry
    or an unblocked downstream job is a continuation of already-admitted
    work, not new queue contention.
    """
    return max(0, rabbitmq_priority - owner_in_flight_count)
