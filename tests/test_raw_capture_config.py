from copy import deepcopy

import pytest

from src.paper_trading.runtime_config import (
    RAW_QUEUE_SLOT_BYTES, raw_capture_reserved_bytes, validate_bot_config)

from bot_v1_support import valid_config

MAX_EVENT = 262144


def section(**overrides):
    value = dict(enabled=True, queue_capacity=100, queue_bytes=MAX_EVENT, fsync_interval_seconds=1.0)
    value.update(overrides)
    return value


DEFAULT = object()


def with_raw(raw=DEFAULT, **overrides):
    config = valid_config()
    config['raw_capture'] = section(**overrides) if raw is DEFAULT else raw
    return config


def test_config_without_the_section_is_unchanged():
    config = valid_config()
    assert validate_bot_config(config) == config
    assert raw_capture_reserved_bytes(config) == 0


def test_valid_section_is_accepted_and_kept():
    validated = validate_bot_config(with_raw())
    assert validated['raw_capture'] == section()


@pytest.mark.parametrize('raw', [
    None,
    [],
    {key: value for key, value in section().items() if key != 'enabled'},
    dict(section(), extra=1),
    dict(section(), max_payload_bytes=MAX_EVENT),
    dict(section(), retry_interval_seconds=1.0),
])
def test_section_needs_exactly_the_four_keys(raw):
    with pytest.raises(ValueError, match='raw_capture must contain exactly'):
        validate_bot_config(with_raw(raw))


@pytest.mark.parametrize('overrides', [
    dict(enabled=1),
    dict(enabled='true'),
    dict(queue_capacity=0),
    dict(queue_capacity=True),
    dict(queue_capacity=10.0),
    dict(queue_bytes=-1),
    dict(queue_bytes=float(MAX_EVENT)),
    dict(fsync_interval_seconds=0),
    dict(fsync_interval_seconds='1'),
])
def test_wrong_types_and_values_are_rejected(overrides):
    with pytest.raises(ValueError, match='raw_capture'):
        validate_bot_config(with_raw(**overrides))


@pytest.mark.parametrize('value', [float('inf'), float('nan')])
def test_non_finite_interval_is_rejected_by_the_existing_json_check(value):
    with pytest.raises(ValueError, match='must be finite'):
        validate_bot_config(with_raw(fsync_interval_seconds=value))


def test_fsync_interval_is_at_most_sixty_seconds():
    assert validate_bot_config(with_raw(fsync_interval_seconds=60))
    with pytest.raises(ValueError, match='at most 60'):
        validate_bot_config(with_raw(fsync_interval_seconds=60.001))


def test_queue_bytes_must_hold_one_maximum_size_frame():
    with pytest.raises(ValueError, match='at least max_event_bytes'):
        validate_bot_config(with_raw(queue_bytes=MAX_EVENT - 1))


def test_disabled_section_is_still_checked():
    with pytest.raises(ValueError, match='raw_capture.queue_capacity'):
        validate_bot_config(with_raw(enabled=False, queue_capacity=0))


def test_enabled_queue_counts_payload_bytes_and_slots():
    config = with_raw(queue_capacity=1000, queue_bytes=2 * MAX_EVENT)
    assert raw_capture_reserved_bytes(config) == 2 * MAX_EVENT + 1000 * RAW_QUEUE_SLOT_BYTES
    config['raw_capture']['enabled'] = False
    assert raw_capture_reserved_bytes(config) == 0


def test_enabled_queue_counts_against_the_memory_budget():
    budget = valid_config()['runtime']['limits']['memory_budget_bytes']
    too_many_bytes = with_raw(queue_bytes=budget)
    with pytest.raises(ValueError, match='memory_budget_bytes'):
        validate_bot_config(too_many_bytes)
    # Small frames in a huge queue still cost per-slot memory.
    too_many_slots = with_raw(queue_capacity=budget // RAW_QUEUE_SLOT_BYTES)
    with pytest.raises(ValueError, match='memory_budget_bytes'):
        validate_bot_config(too_many_slots)


def test_disabled_queue_does_not_count_against_the_memory_budget():
    budget = valid_config()['runtime']['limits']['memory_budget_bytes']
    assert validate_bot_config(with_raw(enabled=False, queue_bytes=budget))


def test_max_event_bytes_is_always_present_when_enabled():
    """The spec's 'enabled requires max_event_bytes' rule is the existing exact-limits check."""
    config = with_raw()
    del config['runtime']['limits']['max_event_bytes']
    with pytest.raises(ValueError, match='runtime.limits must contain all specified limits'):
        validate_bot_config(config)


def test_section_without_a_runtime_section_is_rejected():
    config = with_raw()
    del config['runtime']
    with pytest.raises(ValueError, match='raw_capture requires a runtime section'):
        validate_bot_config(config)


def test_validation_does_not_modify_the_input():
    config = with_raw()
    original = deepcopy(config)
    validate_bot_config(config)
    assert config == original
