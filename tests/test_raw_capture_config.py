import asyncio
from copy import deepcopy
import json

import pytest

from src.bot import BotRunner
from src.paper_trading.clock import VirtualClock
from src.paper_trading.recording import read_capture
from src.paper_trading.replay import replay_runtime
from src.paper_trading.runtime_config import (
    RAW_QUEUE_SLOT_BYTES, raw_capture_reserved_bytes, validate_bot_config)

from bot_v1_support import START, VENUES, ControlledFeed, eventually, strategy_config, valid_config

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


# Capture schema version 5


class Idle:
    async def on_snapshot(self, snapshot):
        return None


def idle_config(raw=None):
    config = valid_config(strategies=[strategy_config('idle', factory='idle')])
    if raw is not None:
        config['raw_capture'] = raw
    return config


async def record(directory, config):
    clock = VirtualClock(START)
    feeds = [ControlledFeed(venue, clock=clock) for venue in VENUES]
    runner = BotRunner(config, output_directory=directory, feeds=feeds, clock=clock,
                       strategy_factories={'idle': lambda context: Idle()})
    task = asyncio.create_task(runner.run())
    await eventually(lambda: task.done() or all(feed.started.is_set() for feed in feeds), task=task)
    for _ in range(3):
        clock.advance(clock() + .05)
        for feed in feeds:
            await feed.push_book()
    for feed in feeds:
        await feed.end()
    await asyncio.wait_for(task, 5)
    return json.loads((directory / 'manifest.json').read_text())


def rewrite_manifest(directory, change):
    path = directory / 'manifest.json'
    manifest = json.loads(path.read_text())
    change(manifest)
    path.write_text(json.dumps(manifest))


@pytest.mark.asyncio
async def test_section_writes_schema_5_and_loads(tmp_path):
    manifest = await record(tmp_path / 'capture', idle_config(section()))
    assert manifest['schema_version'] == 5
    assert manifest['runtime_config']['raw_capture'] == section()
    loaded, records = read_capture(tmp_path / 'capture')
    assert loaded['schema_version'] == 5
    assert {row['schema_version'] for row in records} == {5}


@pytest.mark.asyncio
async def test_disabled_section_still_writes_schema_5(tmp_path):
    manifest = await record(tmp_path / 'capture', idle_config(section(enabled=False)))
    assert manifest['schema_version'] == 5


@pytest.mark.asyncio
async def test_config_without_the_section_still_writes_schema_3(tmp_path):
    manifest = await record(tmp_path / 'capture', idle_config())
    assert manifest['schema_version'] == 3 and 'raw_capture' not in manifest['runtime_config']


@pytest.mark.asyncio
@pytest.mark.parametrize('version', [3, 4])
async def test_older_schema_with_the_section_is_rejected(tmp_path, version):
    await record(tmp_path / 'capture', idle_config())

    def add_section(manifest):
        manifest['schema_version'] = version
        manifest['runtime_config']['raw_capture'] = section()
    rewrite_manifest(tmp_path / 'capture', add_section)
    with pytest.raises(ValueError, match='Only capture schema 5 has a raw_capture'):
        read_capture(tmp_path / 'capture')


@pytest.mark.asyncio
async def test_schema_5_without_the_section_is_rejected(tmp_path):
    await record(tmp_path / 'capture', idle_config(section()))
    rewrite_manifest(tmp_path / 'capture', lambda manifest: manifest['runtime_config'].pop('raw_capture'))
    with pytest.raises(ValueError, match='Only capture schema 5 has a raw_capture'):
        read_capture(tmp_path / 'capture')


@pytest.mark.asyncio
async def test_schema_5_replays_like_schema_3_without_creating_raw(tmp_path):
    await record(tmp_path / 'v5', idle_config(section()))
    await record(tmp_path / 'v3', idle_config())
    with_raw_summary = await replay_runtime(tmp_path / 'v5', tmp_path / 'v5-replay')
    plain_summary = await replay_runtime(tmp_path / 'v3', tmp_path / 'v3-replay')
    for summary in (with_raw_summary, plain_summary):
        summary.pop('processing_wall_time')     # real elapsed time, different on every run
    assert with_raw_summary == plain_summary
    assert not (tmp_path / 'v5-replay' / 'raw').exists()
    replayed = json.loads((tmp_path / 'v5-replay' / 'manifest.json').read_text())
    assert replayed['schema_version'] == 3 and 'raw_capture' not in replayed['runtime_config']


@pytest.mark.asyncio
async def test_fingerprint_covers_the_raw_capture_code(tmp_path):
    manifest = await record(tmp_path / 'capture', idle_config())
    files = manifest['provenance']['source_fingerprint']['files']
    assert {'src/raw_capture/records.py', 'src/raw_capture/capture.py',
            'src/raw_capture/validator.py'} <= files.keys()
