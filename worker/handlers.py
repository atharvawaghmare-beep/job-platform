"""Job type handlers.

Each handler takes the job's `payload` dict, returns a JSON-serializable
result dict on success, or raises on failure. `fail` and `flaky` exist
specifically to exercise the retry/backoff/dead-letter pipeline added in
Phase 2 (CLAUDE.md: "Simulate failures (random or forced)") — more real
job types get added as actual use cases show up.
"""
import random
import time

MAX_SLEEP_SECONDS = 30.0


def handle_sleep(payload: dict) -> dict:
    """Sleeps for `payload["seconds"]` (default 1, capped at 30s) and returns
    how long it slept. Deliberately trivial — it exists to exercise the
    queued -> running -> succeeded/failed pipeline, not to do real work.
    """
    seconds = float(payload.get("seconds", 1))
    seconds = max(0.0, min(seconds, MAX_SLEEP_SECONDS))
    time.sleep(seconds)
    return {"slept_seconds": seconds}


def handle_fail(payload: dict) -> dict:
    """Always raises — a deterministic way to force a job through every
    retry attempt and into `dead_letter`, for testing/demo purposes.
    """
    raise RuntimeError(payload.get("message", "forced failure for testing"))


def handle_flaky(payload: dict) -> dict:
    """Fails with probability `payload["fail_probability"]` (default 0.7),
    otherwise succeeds like `sleep`. Demonstrates a job that recovers via
    retry rather than always ending up dead-lettered.
    """
    fail_probability = float(payload.get("fail_probability", 0.7))
    if random.random() < fail_probability:
        raise RuntimeError("flaky handler: simulated random failure")
    seconds = max(0.0, min(float(payload.get("seconds", 0)), MAX_SLEEP_SECONDS))
    time.sleep(seconds)
    return {"slept_seconds": seconds, "flaky": True}


HANDLERS = {
    "sleep": handle_sleep,
    "fail": handle_fail,
    "flaky": handle_flaky,
}
