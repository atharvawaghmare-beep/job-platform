"""End-to-end integration test: submits a real job through the live API and
confirms it runs to completion via the actual worker + RabbitMQ + Postgres
(see /CLAUDE.md's Testing Expectations: "submit job through real API + queue
+ worker in Docker Compose, assert final state").

Requires `docker compose up` to already be running. Skipped automatically if
the API isn't reachable, so a plain `pytest` run offline still passes.
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


def _wait_for_terminal_status(client: httpx.Client, job_id: str, timeout: float = 10.0) -> dict:
    deadline = time.time() + timeout
    job = {}
    while time.time() < deadline:
        job = client.get(f"/jobs/{job_id}").json()
        if job["status"] in ("succeeded", "failed", "dead_letter"):
            return job
        time.sleep(0.5)
    return job


def test_sleep_job_runs_to_completion():
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        response = client.post("/jobs", json={"type": "sleep", "payload": {"seconds": 1}})
        assert response.status_code == 201
        job = response.json()
        assert job["status"] == "queued"

        job = _wait_for_terminal_status(client, job["id"])
        assert job["status"] == "succeeded", job
        assert job["result"] == {"slept_seconds": 1.0}
        assert job["started_at"] is not None
        assert job["finished_at"] is not None


def test_unknown_job_type_fails_terminally():
    # As of Phase 2, an unrecognized type is just another handler failure and
    # goes through the same retry -> dead_letter path as any other error (see
    # test_retry_and_dead_letter_integration.py) — max_retries=0 here keeps
    # this test fast and focused on "it doesn't crash the worker or hang".
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        response = client.post(
            "/jobs", json={"type": "does_not_exist", "payload": {}, "max_retries": 0}
        )
        assert response.status_code == 201
        job = _wait_for_terminal_status(client, response.json()["id"])
        assert job["status"] == "dead_letter"
        assert "unknown job type" in job["error"]


def test_get_job_404_for_unknown_id():
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        response = client.get("/jobs/00000000-0000-0000-0000-000000000000")
        assert response.status_code == 404
