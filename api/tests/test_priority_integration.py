"""Live integration tests for Phase 5 (CLAUDE.md DoD: "submit high and low
priority jobs from a full queue, confirm high priority is processed
first"), plus the optional per-owner fairness downshift.

Both tests rely on a single worker (prefetch_count=1) being genuinely
saturated by a backlog of slow low-priority jobs before the
higher-priority one is submitted — that's what "from a full queue" means
in practice here. Requires `docker compose up` with exactly one worker
replica (the default); skipped automatically if the API isn't reachable.
"""
import time

import httpx
import pytest

API_BASE_URL = "http://localhost:8000"


def _api_reachable() -> bool:
    try:
        httpx.get(f"{API_BASE_URL}/health", timeout=1.0)
        return True
    except httpx.HTTPError:
        return False


pytestmark = pytest.mark.skipif(
    not _api_reachable(),
    reason="live stack not reachable at localhost:8000 — run `docker compose up` first",
)


def _wait_for_terminal(client: httpx.Client, job_id: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    job: dict = {}
    while time.time() < deadline:
        job = client.get(f"/jobs/{job_id}").json()
        if job.get("status") in ("succeeded", "failed", "dead_letter"):
            return job
        time.sleep(0.3)
    return job


def test_high_priority_job_jumps_a_backlog_of_low_priority_jobs():
    with httpx.Client(base_url=API_BASE_URL, timeout=10.0) as client:
        # Flood the queue with slow, low-priority jobs — with one worker
        # (prefetch_count=1), these form a genuine backlog: the first gets
        # claimed almost immediately, the rest sit `queued`.
        low_ids = [
            client.post(
                "/jobs", json={"type": "sleep", "payload": {"seconds": 2}, "priority": 200}
            ).json()["id"]
            for _ in range(6)
        ]

        # Now a high-priority job arrives, behind all 6 in submission order.
        high_resp = client.post(
            "/jobs", json={"type": "sleep", "payload": {"seconds": 1}, "priority": 0}
        )
        assert high_resp.status_code == 201
        high_id = high_resp.json()["id"]

        high_job = _wait_for_terminal(client, high_id)
        assert high_job["status"] == "succeeded", high_job

        # If priority ordering worked, the high-priority job jumped the
        # backlog — at most the one low job already claimed before it was
        # even submitted should have finished by now.
        finished_low_count = sum(
            1 for job_id in low_ids if client.get(f"/jobs/{job_id}").json()["status"] == "succeeded"
        )
        assert finished_low_count <= 1, (
            f"expected at most 1 low-priority job finished before the high-priority one, got {finished_low_count}"
        )


def test_fairness_downshift_lets_a_fresh_owner_jump_a_busy_owners_backlog():
    with httpx.Client(base_url=API_BASE_URL, timeout=10.0) as client:
        # Same owner, same nominal priority, submitted back-to-back — each
        # one after the first should be progressively downshifted by the
        # in-flight count it sees at submission time.
        busy_ids = [
            client.post(
                "/jobs",
                json={
                    "type": "sleep",
                    "payload": {"seconds": 2},
                    "priority": 100,
                    "owner_id": "busy-owner",
                },
            ).json()["id"]
            for _ in range(4)
        ]

        # A different, previously-idle owner submits at the same nominal
        # priority — their first job gets no downshift at all.
        fresh_resp = client.post(
            "/jobs",
            json={
                "type": "sleep",
                "payload": {"seconds": 1},
                "priority": 100,
                "owner_id": "fresh-owner",
            },
        )
        assert fresh_resp.status_code == 201
        fresh_id = fresh_resp.json()["id"]

        fresh_job = _wait_for_terminal(client, fresh_id)
        assert fresh_job["status"] == "succeeded", fresh_job

        # Only busy-owner's very first job (which had 0 in-flight siblings
        # at its own submission, so no downshift, same priority as fresh)
        # should have beaten it.
        finished_busy_count = sum(
            1 for job_id in busy_ids if client.get(f"/jobs/{job_id}").json()["status"] == "succeeded"
        )
        assert finished_busy_count <= 1, (
            f"expected at most 1 busy-owner job finished before the fresh owner's, got {finished_busy_count}"
        )
