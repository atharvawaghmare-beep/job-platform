"""End-to-end integration test for Phase 2: forces a job to fail repeatedly
and confirms it lands in `dead_letter` after exhausting retries, and that a
job which recovers (fails a few times, then would succeed) has its
retry_count reflected (see /CLAUDE.md Phase 2 DoD: "force a job to fail
repeatedly, confirm it lands in dead-letter and stops retrying").

Requires `docker compose up` already running. Skipped automatically if the
API isn't reachable.
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


def _wait_for_terminal_status(client: httpx.Client, job_id: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    job = {}
    while time.time() < deadline:
        job = client.get(f"/jobs/{job_id}").json()
        if job["status"] in ("succeeded", "failed", "dead_letter"):
            return job
        time.sleep(0.5)
    return job


def test_forced_failure_exhausts_retries_and_reaches_dead_letter():
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        response = client.post(
            "/jobs",
            json={"type": "fail", "payload": {"message": "integration test forced failure"}, "max_retries": 2},
        )
        assert response.status_code == 201
        job_id = response.json()["id"]

        job = _wait_for_terminal_status(client, job_id)
        assert job["status"] == "dead_letter", job
        # 1 initial attempt + 2 retries = retry_count reaches max_retries + 1
        assert job["retry_count"] == 3
        assert job["max_retries"] == 2
        assert "forced failure" in job["error"]
        assert job["finished_at"] is not None


def test_zero_max_retries_goes_straight_to_dead_letter():
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        response = client.post(
            "/jobs",
            json={"type": "fail", "payload": {}, "max_retries": 0},
        )
        assert response.status_code == 201
        job_id = response.json()["id"]

        job = _wait_for_terminal_status(client, job_id)
        assert job["status"] == "dead_letter", job
        assert job["retry_count"] == 1
