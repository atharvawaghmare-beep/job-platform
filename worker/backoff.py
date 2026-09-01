"""Retry backoff delay computation.

Uses the "full jitter" strategy from AWS's Exponential Backoff And Jitter
architecture blog: delay = random_uniform(0, min(cap, base * 2**(attempt-1))).
Full jitter (rather than a bare capped exponential) spreads retries out
instead of having every failing job hammer the queue again at the exact
same instant.
"""
import random

BASE_DELAY_SECONDS = 2.0
MAX_DELAY_SECONDS = 30.0  # kept small — this is a portfolio demo, not prod tuning


def compute_backoff_delay_seconds(
    retry_count: int, base: float = BASE_DELAY_SECONDS, cap: float = MAX_DELAY_SECONDS
) -> float:
    """`retry_count` is the attempt about to be made (1 = first retry)."""
    if retry_count < 1:
        raise ValueError("retry_count must be >= 1")
    exp_delay = min(cap, base * (2 ** (retry_count - 1)))
    return random.uniform(0, exp_delay)
