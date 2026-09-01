"""Pure unit tests for job handlers — no DB/RabbitMQ needed."""
import pytest

import handlers
from handlers import handle_fail, handle_sleep


def test_handle_fail_always_raises():
    with pytest.raises(RuntimeError):
        handle_fail({})


def test_handle_fail_uses_custom_message():
    with pytest.raises(RuntimeError, match="boom"):
        handle_fail({"message": "boom"})


def test_handle_sleep_returns_slept_seconds():
    result = handle_sleep({"seconds": 0})
    assert result == {"slept_seconds": 0.0}


def test_handle_sleep_caps_at_max(monkeypatch):
    # Don't actually sleep 30s in a unit test — just check the value is
    # capped before it's handed to time.sleep().
    monkeypatch.setattr(handlers.time, "sleep", lambda _seconds: None)
    result = handle_sleep({"seconds": 999})
    assert result["slept_seconds"] == 30.0
