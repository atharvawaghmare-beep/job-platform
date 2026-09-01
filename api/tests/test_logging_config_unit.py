"""Pure unit tests for the shared JSON log formatter — no DB/queue needed."""
import json
import logging

from common.logging_config import JsonFormatter


def _make_record(msg: str, extra: dict | None = None, exc_info=None) -> logging.LogRecord:
    record = logging.LogRecord(
        name="worker", level=logging.INFO, pathname=__file__, lineno=1,
        msg=msg, args=(), exc_info=exc_info,
    )
    for key, value in (extra or {}).items():
        setattr(record, key, value)
    return record


def test_basic_fields_present():
    record = _make_record("job succeeded")
    payload = json.loads(JsonFormatter().format(record))
    assert payload["message"] == "job succeeded"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "worker"
    assert "timestamp" in payload


def test_extra_fields_are_included_as_top_level_keys():
    record = _make_record("job claimed", extra={"job_id": "abc-123", "retry_count": 2})
    payload = json.loads(JsonFormatter().format(record))
    assert payload["job_id"] == "abc-123"
    assert payload["retry_count"] == 2


def test_exception_info_is_included_when_present():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys
        record = _make_record("job failed", exc_info=sys.exc_info())
    payload = json.loads(JsonFormatter().format(record))
    assert "boom" in payload["exception"]


def test_output_is_valid_single_line_json():
    record = _make_record("hello")
    output = JsonFormatter().format(record)
    assert "\n" not in output
    json.loads(output)  # doesn't raise
