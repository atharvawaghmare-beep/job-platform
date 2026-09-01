"""Shared structured (JSON) logging setup for both the API and worker
(CLAUDE.md: "structured JSON logging" + "job id in every log line").

Call sites pass job/workflow context via the standard `extra=` kwarg
(e.g. `logger.info("...", extra={"job_id": str(job.id)})`) rather than only
embedding it in the message text — that's what makes it genuinely
*structured*: a log aggregator can filter/group by the `job_id` field
directly instead of regexing message strings.

Scope note: this configures our own application loggers (the worker's
"worker" logger, the API's "app.*" loggers) — it does not reconfigure
uvicorn's own "uvicorn"/"uvicorn.access" loggers, which install their own
handlers/formatters at startup independent of the root logger. Uvicorn's
HTTP access log lines stay in their default format; every application log
line about a job goes through this JSON formatter.
"""
import json
import logging
import sys
from datetime import datetime, timezone

# Attributes present on every LogRecord by default — anything else found on
# a record (i.e. passed via `extra=`) is application-supplied context and
# gets included in the JSON output.
_STANDARD_LOG_RECORD_ATTRS = set(logging.LogRecord("", 0, "", 0, "", None, None).__dict__.keys())


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOG_RECORD_ATTRS:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
