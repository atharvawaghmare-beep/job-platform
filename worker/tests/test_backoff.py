"""Pure unit tests for backoff computation — no DB/RabbitMQ needed."""
import pytest

from backoff import compute_backoff_delay_seconds


def test_delay_within_bounds_for_several_attempts():
    for retry_count in range(1, 8):
        delay = compute_backoff_delay_seconds(retry_count, base=2.0, cap=30.0)
        assert 0 <= delay <= 30.0


def test_delay_cap_is_respected_for_high_retry_counts():
    # Without the cap, 2 * 2**19 would dwarf this.
    delay = compute_backoff_delay_seconds(20, base=2.0, cap=30.0)
    assert 0 <= delay <= 30.0


def test_rejects_retry_count_below_one():
    with pytest.raises(ValueError):
        compute_backoff_delay_seconds(0)
