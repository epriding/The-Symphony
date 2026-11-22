from itertools import count
import json
import threading
import time

import pytest

from src.raw_capture.capture import RawCapture
from src.raw_capture.records import RawMessage
from src.raw_capture.validator import open_raw_capture, validate_raw_capture


def make(tmp_path, *, gate=None, **overrides):
    wall = count(1795190000)
    monotonic = count(5000)
    options = dict(session_id='s1', queue_capacity=1000, queue_bytes=1 << 20, max_payload_bytes=4096,
                   fsync_interval_seconds=0.05, wall=lambda: float(next(wall)),
                   monotonic_ns=lambda: next(monotonic), _gate=gate)
    options.update(overrides)
    return RawCapture(tmp_path, **options)


def eventually(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError('condition not reached in time')
        time.sleep(0.005)


def manifest(tmp_path):
    return json.loads((tmp_path / 'raw' / 'manifest.json').read_text())


def paused():
    """A writer gate that starts closed; set() lets the writer run."""
    return threading.Event()


def test_clean_run_is_complete_and_in_order(tmp_path):
    capture = make(tmp_path)
    payloads = [f'{{"n":{n}}}' if n % 3 else bytes([n % 256, 0, 255]) for n in range(200)]
    for n, payload in enumerate(payloads):
        capture.offer('kalshi' if n % 2 else 'polymarket_us', 'public_ws', n // 50, payload)
    assert capture.close(ended_wall=1795199999.0)
    report = validate_raw_capture(tmp_path)
    assert report.verdict == 'complete', report.errors
    records = list(open_raw_capture(tmp_path))
    assert [r.seq for r in records] == list(range(1, 201))
    assert [r.payload for r in records] == payloads
    assert records[0] == RawMessage(seq=1, venue='polymarket_us', stream='public_ws', connection_epoch=0,
                                    received_wall=1795190001.0, received_monotonic_ns=5000,
                                    payload=payloads[0])
    written = manifest(tmp_path)
    assert (written['status'], written['finalized'], written['ended_wall']) == ('closed', True, 1795199999.0)
    assert written['last_assigned_seq'] == 200 and capture.status == 'closed'


def test_manifest_exists_from_the_start(tmp_path):
    capture = make(tmp_path)
    try:
        written = manifest(tmp_path)
        assert (written['status'], written['finalized'], written['records_sha256']) == ('writing', False, None)
        assert validate_raw_capture(tmp_path).verdict == 'incomplete'
    finally:
        capture.close()


def test_records_reach_disk_before_close(tmp_path):
    capture = make(tmp_path)
    try:
        capture.offer('synthetic', 'test', 0, 'hello')
        eventually(lambda: (tmp_path / 'raw' / 'records.jsonl').stat().st_size > 0)
    finally:
        capture.close()


def test_refuses_an_existing_raw_directory(tmp_path):
    make(tmp_path).close()
    with pytest.raises(FileExistsError):
        make(tmp_path)


def test_trailing_drops_become_a_gap_at_close(tmp_path):
    gate = paused()
    capture = make(tmp_path, gate=gate, queue_capacity=3)
    for n in range(10):
        capture.offer('synthetic', 'test', 0, str(n))
    assert capture.dropped_queue_full == 7
    gate.set()
    assert capture.close()
    report = validate_raw_capture(tmp_path)
    assert report.verdict == 'incomplete', report.errors
    assert report.missing_ranges == [dict(first_seq=4, last_seq=10, reason='queue_full')]
    assert report.message_count == 3 and report.last_assigned_seq == 10


def test_drops_in_the_middle_become_a_gap_before_the_next_message(tmp_path):
    gate = paused()
    capture = make(tmp_path, gate=gate, queue_capacity=2)
    for n in range(5):
        capture.offer('synthetic', 'test', 0, str(n))
    gate.set()
    eventually(capture._queue.empty)
    capture.offer('synthetic', 'test', 0, 'after')
    assert capture.close()
    report = validate_raw_capture(tmp_path)
    assert report.missing_ranges == [dict(first_seq=3, last_seq=5, reason='queue_full')]
    assert line_kinds(tmp_path) == [1, 2, 'gap', 6]


def line_kinds(tmp_path):
    """Each line's seq for a message, or 'gap', in file order."""
    lines = (tmp_path / 'raw' / 'records.jsonl').read_bytes().splitlines()
    return [json.loads(line).get('seq', 'gap') for line in lines]


def test_offer_never_blocks_while_the_writer_is_paused(tmp_path):
    gate = paused()
    capture = make(tmp_path, gate=gate, queue_capacity=5)
    try:
        started = time.monotonic()
        for n in range(10_000):
            capture.offer('synthetic', 'test', 0, 'x')
        assert time.monotonic() - started < 2.0
        assert capture.dropped_queue_full == 10_000 - 5
    finally:
        gate.set()
        capture.close()


def test_offer_after_close_is_ignored_and_close_is_idempotent(tmp_path):
    capture = make(tmp_path)
    capture.offer('synthetic', 'test', 0, 'one')
    assert capture.close()
    capture.offer('synthetic', 'test', 0, 'late')
    assert capture.close()
    assert capture.last_assigned_seq == 1
    assert validate_raw_capture(tmp_path).verdict == 'complete'


@pytest.mark.parametrize('args', [
    ('binance', 'test', 0, 'x'),
    ('synthetic', '', 0, 'x'),
    ('synthetic', 'x' * 65, 0, 'x'),
    ('synthetic', 'test', -1, 'x'),
    ('synthetic', 'test', 0, None),
    ('synthetic', 'test', 0, bytearray(b'x')),
])
def test_offer_rejects_programming_errors(tmp_path, args):
    capture = make(tmp_path)
    try:
        with pytest.raises(ValueError):
            capture.offer(*args)
        assert capture.last_assigned_seq == 0
    finally:
        capture.close()


@pytest.mark.parametrize('overrides', [
    dict(queue_capacity=0), dict(queue_bytes=0), dict(max_payload_bytes=True),
    dict(fsync_interval_seconds=0), dict(fsync_interval_seconds=float('inf')),
    dict(retry_interval_seconds=-1),
])
def test_invalid_settings_are_rejected_before_creating_files(tmp_path, overrides):
    with pytest.raises(ValueError):
        make(tmp_path, **overrides)
    assert not (tmp_path / 'raw').exists()


def test_byte_limit_drops_frames_that_would_overflow_it(tmp_path):
    gate = paused()
    capture = make(tmp_path, gate=gate, queue_bytes=10, max_payload_bytes=10)
    for payload in ('aaaa', 'bbbb', 'cccc', b'dd', 'é'):   # 4 + 4 fit; 4 does not; 2 fits; 'é' is 2 bytes
        capture.offer('synthetic', 'test', 0, payload)
    gate.set()
    assert capture.close()
    report = validate_raw_capture(tmp_path)
    assert report.missing_ranges == [dict(first_seq=3, last_seq=3, reason='queue_full'),
                                     dict(first_seq=5, last_seq=5, reason='queue_full')]
    assert [r.payload for r in open_raw_capture_incomplete(tmp_path)] == ['aaaa', 'bbbb', b'dd']
    assert capture.dropped_queue_full == 2


def test_byte_limit_frees_space_as_the_writer_drains(tmp_path):
    gate = paused()
    capture = make(tmp_path, gate=gate, queue_bytes=8, max_payload_bytes=8)
    capture.offer('synthetic', 'test', 0, 'x' * 8)
    capture.offer('synthetic', 'test', 0, 'y')          # no room
    gate.set()
    eventually(capture._queue.empty)
    eventually(lambda: capture._queued_bytes == 0)
    capture.offer('synthetic', 'test', 0, 'z' * 8)      # room again
    assert capture.close()
    assert line_kinds(tmp_path) == [1, 'gap', 3]


def test_oversize_frame_becomes_a_gap_without_content(tmp_path):
    capture = make(tmp_path, max_payload_bytes=4)
    capture.offer('synthetic', 'test', 0, 'ok')
    capture.offer('synthetic', 'test', 0, 'secret-and-too-long')
    capture.offer('synthetic', 'test', 0, 'ééé')           # 6 UTF-8 bytes, 3 characters
    capture.offer('synthetic', 'test', 0, b'1234')
    assert capture.close()
    report = validate_raw_capture(tmp_path)
    assert report.missing_ranges == [dict(first_seq=2, last_seq=2, reason='oversize'),
                                     dict(first_seq=3, last_seq=3, reason='oversize')]
    assert report.dropped == {'oversize': 2, 'queue_full': 0, 'writer_error': 0}
    assert b'secret' not in (tmp_path / 'raw' / 'records.jsonl').read_bytes()


def test_queue_bytes_must_hold_one_maximum_frame(tmp_path):
    with pytest.raises(ValueError, match='queue_bytes'):
        make(tmp_path, queue_bytes=100, max_payload_bytes=101)


def open_raw_capture_incomplete(tmp_path):
    """Messages from a capture that has gaps (open_raw_capture refuses those)."""
    from src.raw_capture.records import parse_line
    lines = (tmp_path / 'raw' / 'records.jsonl').read_bytes().splitlines(keepends=True)
    return [record for record in map(parse_line, lines) if isinstance(record, RawMessage)]
