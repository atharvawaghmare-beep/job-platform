"""Pure unit tests for the priority/fairness mapping — no DB/queue needed."""
from common.priority import (
    PRIORITY_HIGH,
    PRIORITY_LOW,
    PRIORITY_NORMAL,
    RABBITMQ_MAX_PRIORITY,
    apply_fairness_downshift,
    to_rabbitmq_priority,
)


def test_high_priority_maps_to_rabbitmq_max():
    assert to_rabbitmq_priority(PRIORITY_HIGH) == RABBITMQ_MAX_PRIORITY


def test_low_priority_maps_to_rabbitmq_min():
    assert to_rabbitmq_priority(PRIORITY_LOW) == 0


def test_normal_priority_maps_to_middle():
    assert to_rabbitmq_priority(PRIORITY_NORMAL) == RABBITMQ_MAX_PRIORITY // 2


def test_out_of_range_priority_is_clamped():
    assert to_rabbitmq_priority(-50) == RABBITMQ_MAX_PRIORITY
    assert to_rabbitmq_priority(9999) == 0


def test_higher_urgency_beats_lower_urgency():
    assert to_rabbitmq_priority(PRIORITY_HIGH) > to_rabbitmq_priority(PRIORITY_NORMAL) > to_rabbitmq_priority(PRIORITY_LOW)


def test_fairness_no_downshift_for_idle_owner():
    assert apply_fairness_downshift(RABBITMQ_MAX_PRIORITY, owner_in_flight_count=0) == RABBITMQ_MAX_PRIORITY


def test_fairness_downshifts_one_level_per_in_flight_job():
    assert apply_fairness_downshift(10, owner_in_flight_count=3) == 7


def test_fairness_never_goes_below_zero():
    assert apply_fairness_downshift(2, owner_in_flight_count=100) == 0
