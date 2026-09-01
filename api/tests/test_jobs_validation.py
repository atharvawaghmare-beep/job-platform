"""Pure request-validation tests for POST /jobs. No DB/queue needed — FastAPI
rejects an invalid body via Pydantic before the route body (and thus any
DB/queue call) ever runs.
"""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_create_job_missing_type_returns_422():
    response = client.post("/jobs", json={"payload": {}})
    assert response.status_code == 422


def test_get_job_invalid_uuid_returns_422():
    response = client.get("/jobs/not-a-uuid")
    assert response.status_code == 422
