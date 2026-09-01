"""Pure unit test for the idempotency-key match logic — no DB/Redis needed.
`Job` and `JobCreate` can both be constructed in memory without touching a
session.
"""
from app.routers.jobs import JobCreate, _matches
from common.models import Job


def test_matches_same_type_and_payload():
    existing = Job(type="sleep", payload={"seconds": 1})
    body = JobCreate(type="sleep", payload={"seconds": 1})
    assert _matches(existing, body) is True


def test_matches_false_for_different_payload():
    existing = Job(type="sleep", payload={"seconds": 1})
    body = JobCreate(type="sleep", payload={"seconds": 2})
    assert _matches(existing, body) is False


def test_matches_false_for_different_type():
    existing = Job(type="sleep", payload={})
    body = JobCreate(type="fail", payload={})
    assert _matches(existing, body) is False
