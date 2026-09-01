"""Pure unit tests for Prometheus text rendering — no DB/RabbitMQ needed."""
from app.routers.metrics import render_prometheus_text


def _all_statuses(**overrides) -> dict[str, int]:
    base = {"pending": 0, "queued": 0, "running": 0, "succeeded": 0, "failed": 0, "dead_letter": 0}
    base.update(overrides)
    return base


def test_job_status_count_lines_present_for_every_status():
    text = render_prometheus_text(_all_statuses(queued=3, running=1), retry_total=0, queue_depths={})
    assert 'job_status_count{status="queued"} 3' in text
    assert 'job_status_count{status="running"} 1' in text
    assert 'job_status_count{status="pending"} 0' in text


def test_terminal_totals_reflect_status_counts():
    text = render_prometheus_text(
        _all_statuses(succeeded=10, failed=2, dead_letter=1), retry_total=0, queue_depths={}
    )
    assert 'jobs_terminal_total{status="succeeded"} 10' in text
    assert 'jobs_terminal_total{status="failed"} 2' in text
    assert 'jobs_terminal_total{status="dead_letter"} 1' in text


def test_retry_count_total_line():
    text = render_prometheus_text(_all_statuses(), retry_total=7, queue_depths={})
    assert "job_retry_count_total 7" in text


def test_queue_depth_line_included_when_known():
    text = render_prometheus_text(_all_statuses(), retry_total=0, queue_depths={"jobs": 5})
    assert 'rabbitmq_queue_messages{queue="jobs"} 5' in text


def test_queue_depth_omitted_when_unreachable():
    text = render_prometheus_text(_all_statuses(), retry_total=0, queue_depths={"jobs": None})
    assert 'queue="jobs"' not in text


def test_output_has_help_and_type_lines():
    text = render_prometheus_text(_all_statuses(), retry_total=0, queue_depths={})
    assert "# HELP job_status_count" in text
    assert "# TYPE job_status_count gauge" in text
    assert "# TYPE jobs_terminal_total counter" in text
