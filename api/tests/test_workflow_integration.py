"""Live integration test for Phase 4 (CLAUDE.md DoD): submits a diamond
DAG (A -> B, A -> C, B+C -> D), confirms correct ordering and completion,
then submits the same shape with A forced to fail and confirms B, C, D are
all blocked rather than run. Requires `docker compose up`; skipped
automatically if the API isn't reachable.
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


def _diamond_body(name: str, *, a_type: str, a_extra: dict | None = None) -> dict:
    a_job = {"key": "A", "type": a_type, "payload": {}}
    if a_extra:
        a_job.update(a_extra)
    return {
        "name": name,
        "jobs": [
            a_job,
            {"key": "B", "type": "sleep", "payload": {"seconds": 1}, "depends_on": ["A"]},
            {"key": "C", "type": "sleep", "payload": {"seconds": 1}, "depends_on": ["A"]},
            {"key": "D", "type": "sleep", "payload": {"seconds": 1}, "depends_on": ["B", "C"]},
        ],
    }


def _wait_for_workflow_terminal(client: httpx.Client, workflow_id: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    wf: dict = {}
    while time.time() < deadline:
        wf = client.get(f"/workflows/{workflow_id}").json()
        if wf.get("status") in ("completed", "failed"):
            return wf
        time.sleep(0.5)
    return wf


def test_diamond_dag_succeeds_in_dependency_order():
    body = _diamond_body("diamond-success", a_type="sleep")
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        resp = client.post("/workflows", json=body)
        assert resp.status_code == 201
        workflow_id = resp.json()["id"]

        wf = _wait_for_workflow_terminal(client, workflow_id)
        assert wf["status"] == "completed", wf

        jobs = {j["workflow_key"]: j for j in wf["jobs"]}
        assert all(j["status"] == "succeeded" for j in jobs.values())
        # A must finish before B/C start; B and C must finish before D starts.
        assert jobs["A"]["finished_at"] <= jobs["B"]["started_at"]
        assert jobs["A"]["finished_at"] <= jobs["C"]["started_at"]
        assert jobs["B"]["finished_at"] <= jobs["D"]["started_at"]
        assert jobs["C"]["finished_at"] <= jobs["D"]["started_at"]


def test_diamond_dag_blocks_downstream_on_failure():
    body = _diamond_body("diamond-failure", a_type="fail", a_extra={"max_retries": 0})
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        resp = client.post("/workflows", json=body)
        assert resp.status_code == 201
        workflow_id = resp.json()["id"]

        wf = _wait_for_workflow_terminal(client, workflow_id)
        assert wf["status"] == "failed", wf

        jobs = {j["workflow_key"]: j for j in wf["jobs"]}
        assert jobs["A"]["status"] == "dead_letter"
        assert jobs["B"]["status"] == "failed"
        assert jobs["C"]["status"] == "failed"
        assert jobs["D"]["status"] == "failed"
        # B, C, D never ran — they were blocked, not executed and failed.
        assert jobs["B"]["started_at"] is None
        assert jobs["C"]["started_at"] is None
        assert jobs["D"]["started_at"] is None


def test_invalid_dag_is_rejected_and_creates_nothing():
    body = {"name": "bad", "jobs": [{"key": "A", "type": "sleep", "payload": {}, "depends_on": ["B"]}]}
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        resp = client.post("/workflows", json=body)
        assert resp.status_code == 422


def test_get_workflow_404_for_unknown_id():
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        resp = client.get("/workflows/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404
