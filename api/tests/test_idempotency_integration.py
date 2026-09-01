"""Live integration tests for idempotency-key dedupe on submit (Phase 3):
same key + same type/payload -> 200 with the existing job; same key +
different type/payload -> 409. Requires `docker compose up`; skipped
automatically if the API isn't reachable.
"""
import uuid

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


def test_resubmitting_same_key_and_payload_returns_existing_job():
    key = f"test-{uuid.uuid4()}"
    body = {"type": "sleep", "payload": {"seconds": 1}, "idempotency_key": key}

    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        first = client.post("/jobs", json=body)
        assert first.status_code == 201
        first_id = first.json()["id"]

        second = client.post("/jobs", json=body)
        assert second.status_code == 200
        assert second.json()["id"] == first_id


def test_resubmitting_same_key_with_different_payload_is_409():
    key = f"test-{uuid.uuid4()}"

    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        first = client.post(
            "/jobs", json={"type": "sleep", "payload": {"seconds": 1}, "idempotency_key": key}
        )
        assert first.status_code == 201

        conflicting = client.post(
            "/jobs", json={"type": "sleep", "payload": {"seconds": 2}, "idempotency_key": key}
        )
        assert conflicting.status_code == 409

        conflicting_type = client.post(
            "/jobs", json={"type": "fail", "payload": {"seconds": 1}, "idempotency_key": key}
        )
        assert conflicting_type.status_code == 409


def test_no_idempotency_key_never_dedupes():
    with httpx.Client(base_url=API_BASE_URL, timeout=5.0) as client:
        body = {"type": "sleep", "payload": {"seconds": 1}}
        first = client.post("/jobs", json=body)
        second = client.post("/jobs", json=body)
        assert first.status_code == 201
        assert second.status_code == 201
        assert first.json()["id"] != second.json()["id"]
