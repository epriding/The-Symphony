from itertools import count
import json
import threading
import time

import pytest

from src.raw_capture.capture import FileOps, RawCapture
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


def test_frame_refused_by_the_connection_becomes_an_oversize_gap(tmp_path):
    capture = make(tmp_path)
    capture.offer('kalshi', 'public_ws', 1, 'before')
    capture.mark_oversize('kalshi', 'public_ws', 1)
    capture.offer('kalshi', 'public_ws', 2, 'after')
    assert capture.close()
    report = validate_raw_capture(tmp_path)
    assert report.verdict == 'incomplete', report.errors
    assert report.missing_ranges == [dict(first_seq=2, last_seq=2, reason='oversize')]
    assert line_kinds(tmp_path) == [1, 'gap', 3]


def test_mark_oversize_rejects_programming_errors_and_is_ignored_after_close(tmp_path):
    capture = make(tmp_path)
    with pytest.raises(ValueError):
        capture.mark_oversize('nasdaq', 'public_ws', 1)
    assert capture.close()
    capture.mark_oversize('kalshi', 'public_ws', 1)
    assert capture.last_assigned_seq == 0


def test_summary_of_a_clean_capture(tmp_path):
    capture = make(tmp_path)
    for n in range(3):
        capture.offer('synthetic', 'test', 0, str(n))
    assert capture.close()
    assert capture.summary() == dict(status='closed', finalized=True, complete=True, last_assigned_seq=3,
                                     message_count=3, missing_count=0,
                                     dropped={'oversize': 0, 'queue_full': 0, 'writer_error': 0}, error=None)


def test_summary_counts_gaps_like_the_validator(tmp_path):
    gate = paused()
    capture = make(tmp_path, gate=gate, queue_capacity=2, max_payload_bytes=4)
    capture.offer('synthetic', 'test', 0, 'too long')
    for n in range(4):
        capture.offer('synthetic', 'test', 0, str(n))
    gate.set()
    assert capture.close()
    summary, report = capture.summary(), validate_raw_capture(tmp_path)
    # The oversize marker and '0' fill the two slots; '1', '2' and '3' are dropped.
    assert (summary['complete'], summary['missing_count']) == (False, 4)
    assert summary['dropped'] == report.dropped == {'oversize': 1, 'queue_full': 3, 'writer_error': 0}
    assert summary['message_count'] == report.message_count == 1


def test_queue_bytes_must_hold_one_maximum_frame(tmp_path):
    with pytest.raises(ValueError, match='queue_bytes'):
        make(tmp_path, queue_bytes=100, max_payload_bytes=101)


def open_raw_capture_incomplete(tmp_path):
    """Messages from a capture that has gaps (open_raw_capture refuses those)."""
    from src.raw_capture.records import parse_line
    lines = (tmp_path / 'raw' / 'records.jsonl').read_bytes().splitlines(keepends=True)
    return [record for record in map(parse_line, lines) if isinstance(record, RawMessage)]


@pytest.mark.parametrize('overrides', [dict(session_id=None), dict(session_id=''), dict(session_id=7),
                                       dict(code_revision=None)])
def test_invalid_identity_is_rejected_before_creating_files(tmp_path, overrides):
    with pytest.raises(ValueError):
        make(tmp_path, **overrides)
    assert not (tmp_path / 'raw').exists()


class FailingOpen(FileOps):
    def open(self, path, *, create):
        raise OSError('disk full')


def test_failed_open_leaves_nothing_behind_and_allows_a_retry(tmp_path):
    with pytest.raises(OSError, match='disk full'):
        make(tmp_path, file_ops=FailingOpen())
    assert not (tmp_path / 'raw').exists()
    make(tmp_path).close()
    assert validate_raw_capture(tmp_path).verdict == 'complete'


def test_failed_first_manifest_leaves_nothing_behind(tmp_path, monkeypatch):
    def fail(self, **_):
        (self.directory / 'manifest.tmp').write_text('{')   # as if the write died part-way
        raise OSError('no space for manifest')
    monkeypatch.setattr(RawCapture, '_write_manifest', fail)
    with pytest.raises(OSError, match='no space'):
        make(tmp_path)
    assert not (tmp_path / 'raw').exists()


def test_failed_start_keeps_files_it_did_not_create(tmp_path):
    (tmp_path / 'raw').mkdir()
    (tmp_path / 'raw' / 'notes.txt').write_text('keep me')
    with pytest.raises(FileExistsError):
        make(tmp_path)
    assert (tmp_path / 'raw' / 'notes.txt').read_text() == 'keep me'


class CountingOps(FileOps):
    """Real file operations that count fsyncs and split every write in two (short writes)."""
    def __init__(self):
        self.fsyncs = 0

    def write(self, fd, data):
        return super().write(fd, bytes(data[:max(1, len(data) // 2)]))

    def fsync(self, fd):
        self.fsyncs += 1
        super().fsync(fd)


def test_short_writes_are_completed(tmp_path):
    capture = make(tmp_path, file_ops=CountingOps())
    for n in range(50):
        capture.offer('synthetic', 'test', 0, f'message {n} ' * 20)
    assert capture.close()
    assert validate_raw_capture(tmp_path).verdict == 'complete'


def test_idle_writer_does_not_fsync(tmp_path):
    ops = CountingOps()
    capture = make(tmp_path, file_ops=ops, fsync_interval_seconds=0.01)
    capture.offer('synthetic', 'test', 0, 'one')
    eventually(lambda: ops.fsyncs >= 1)
    time.sleep(0.2)             # many intervals pass with nothing new to write
    assert ops.fsyncs == 1
    assert capture.close()
    assert ops.fsyncs == 2      # close syncs the (empty) remainder once


def test_status_hook_reports_close(tmp_path):
    seen = []
    capture = make(tmp_path, on_status=seen.append)
    assert capture.close()
    assert seen == ['closed']


def test_status_hook_errors_are_ignored(tmp_path):
    def broken(status):
        raise RuntimeError('hook bug')
    capture = make(tmp_path, on_status=broken)
    assert capture.close() and capture.status == 'closed'


class BrokenWrite(FileOps):
    def write(self, fd, data):
        raise RuntimeError('bug while writing')


def test_unexpected_error_fails_the_capture(tmp_path):
    seen = []
    capture = make(tmp_path, file_ops=BrokenWrite(), on_status=seen.append)
    capture.offer('synthetic', 'test', 0, 'one')
    eventually(lambda: seen == ['failed'])   # the hook runs after the status and manifest change
    assert capture.status == 'failed' and isinstance(capture.writer_error, RuntimeError)
    capture.offer('synthetic', 'test', 0, 'two')    # still never raises
    assert not capture.close(timeout=1.0)
    report = validate_raw_capture(tmp_path)
    assert report.verdict == 'incomplete' and report.status == 'failed', report.errors


def test_final_manifest_failure_means_not_finalized(tmp_path, monkeypatch):
    capture = make(tmp_path)
    capture.offer('synthetic', 'test', 0, 'one')
    original = RawCapture._write_manifest

    def fail_when_finalizing(self, **changes):
        if changes.get('finalized'):
            raise OSError('no space for manifest')
        original(self, **changes)
    monkeypatch.setattr(RawCapture, '_write_manifest', fail_when_finalizing)
    assert not capture.close()
    assert capture.status == 'failed' and isinstance(capture.writer_error, OSError)
    report = validate_raw_capture(tmp_path)
    assert report.verdict == 'incomplete' and report.status == 'failed', report.errors


# ---- disk errors and recovery ------------------------------------------------

class FaultyOps(FileOps):
    """Real file operations with switches the test flips to make the next calls fail."""
    def __init__(self):
        self.fail_write = self.fail_fsync = self.fail_open = False
        self.fail_fsync_times = 0
        self.opens = 0

    def open(self, path, *, create):
        self.opens += 1
        if self.fail_open and not create:
            raise OSError('cannot reopen')
        return super().open(path, create=create)

    def write(self, fd, data):
        if self.fail_write:
            raise OSError('write failed')
        return super().write(fd, data)

    def fsync(self, fd):
        if self.fail_fsync or self.fail_fsync_times:
            self.fail_fsync_times = max(0, self.fail_fsync_times - 1)
            raise OSError('fsync failed')
        super().fsync(fd)


def faulty(tmp_path, **overrides):
    ops, seen = FaultyOps(), []
    options = dict(file_ops=ops, on_status=seen.append, retry_interval_seconds=0.02,
                   fsync_interval_seconds=0.01)
    options.update(overrides)
    return make(tmp_path, **options), ops, seen


def messages(tmp_path):
    from src.raw_capture.records import parse_line
    lines = (tmp_path / 'raw' / 'records.jsonl').read_bytes().splitlines(keepends=True)
    return [record for record in map(parse_line, lines) if isinstance(record, RawMessage)]


def test_write_error_degrades_then_recovers(tmp_path):
    capture, ops, seen = faulty(tmp_path)
    ops.fail_write = True
    for n in range(1, 4):
        capture.offer('synthetic', 'test', 0, f'lost {n}')
    eventually(lambda: capture.status == 'degraded')
    capture.offer('synthetic', 'test', 0, 'lost 4')    # discarded while degraded; offer still returns
    capture.offer('synthetic', 'test', 0, 'lost 5')
    eventually(capture._queue.empty)
    ops.fail_write = False
    eventually(lambda: capture.status == 'writing')
    capture.offer('synthetic', 'test', 0, 'kept 6')
    assert capture.close()
    report = validate_raw_capture(tmp_path)
    assert report.verdict == 'incomplete' and report.finalized, report.errors
    assert report.missing_ranges == [dict(first_seq=1, last_seq=5, reason='writer_error')]
    assert [m.payload for m in messages(tmp_path)] == ['kept 6']
    assert seen == ['degraded', 'writing', 'closed']


def test_fsync_error_discards_only_data_after_the_checkpoint(tmp_path):
    capture, ops, seen = faulty(tmp_path)
    capture.offer('synthetic', 'test', 0, 'durable 1')
    capture.offer('synthetic', 'test', 0, 'durable 2')
    eventually(lambda: capture._checkpoint['expected'] == 3)    # both are fsynced
    ops.fail_fsync = True
    capture.offer('synthetic', 'test', 0, 'written but never synced 3')
    eventually(lambda: capture.status == 'degraded')
    ops.fail_fsync = False
    eventually(lambda: capture.status == 'writing')
    capture.offer('synthetic', 'test', 0, 'after 4')
    assert capture.close()
    report = validate_raw_capture(tmp_path)
    assert report.missing_ranges == [dict(first_seq=3, last_seq=3, reason='writer_error')], report.errors
    assert [m.payload for m in messages(tmp_path)] == ['durable 1', 'durable 2', 'after 4']
    assert b'never synced' not in (tmp_path / 'raw' / 'records.jsonl').read_bytes()


def test_recovery_retries_until_it_succeeds(tmp_path):
    capture, ops, seen = faulty(tmp_path)
    ops.fail_write = ops.fail_open = True
    capture.offer('synthetic', 'test', 0, 'lost')
    eventually(lambda: capture.status == 'degraded')
    ops.fail_write = False
    opens = ops.opens
    eventually(lambda: ops.opens >= opens + 3)     # several failed reopen attempts
    assert capture.status == 'degraded'
    ops.fail_open = False
    eventually(lambda: capture.status == 'writing')
    capture.offer('synthetic', 'test', 0, 'kept')
    assert capture.close()
    assert [m.payload for m in messages(tmp_path)] == ['kept']
    assert validate_raw_capture(tmp_path).dropped['writer_error'] == 1


def test_drops_during_an_outage_join_the_writer_error_range(tmp_path):
    gate = paused()
    gate.set()
    capture, ops, seen = faulty(tmp_path, _gate=gate, queue_capacity=2)
    ops.fail_write = True
    capture.offer('synthetic', 'test', 0, 'one')
    eventually(lambda: capture.status == 'degraded')
    eventually(capture._queue.empty)
    gate.clear()
    capture.offer('synthetic', 'test', 0, 'a')    # queued
    capture.offer('synthetic', 'test', 0, 'b')    # queued
    capture.offer('synthetic', 'test', 0, 'c')    # dropped: queue full
    gate.set()
    eventually(capture._queue.empty)
    capture.offer('synthetic', 'test', 0, 'd')    # queued, discarded: covers c too
    eventually(capture._queue.empty)
    ops.fail_write = False
    eventually(lambda: capture.status == 'writing')
    assert capture.close()
    report = validate_raw_capture(tmp_path)
    assert report.missing_ranges == [dict(first_seq=1, last_seq=5, reason='writer_error')], report.errors
    assert capture.dropped_queue_full == 1 and report.dropped['queue_full'] == 0


def test_error_that_lasts_through_close_fails_the_capture(tmp_path):
    capture, ops, seen = faulty(tmp_path)
    ops.fail_write = True
    capture.offer('synthetic', 'test', 0, 'lost')
    eventually(lambda: capture.status == 'degraded')
    assert not capture.close(timeout=2.0)
    assert capture.status == 'failed' and isinstance(capture.writer_error, OSError)
    assert seen == ['degraded', 'failed']
    report = validate_raw_capture(tmp_path)
    assert report.verdict == 'incomplete' and report.status == 'failed', report.errors


def test_summary_of_a_failed_capture_names_the_error(tmp_path):
    capture, ops, seen = faulty(tmp_path)
    ops.fail_write = True
    capture.offer('synthetic', 'test', 0, 'lost')
    eventually(lambda: capture.status == 'degraded')
    assert not capture.close(timeout=2.0)
    summary = capture.summary()
    assert (summary['status'], summary['finalized'], summary['complete']) == ('failed', False, False)
    assert summary['missing_count'] == 1 and summary['error'] == 'OSError: write failed'


def test_error_during_the_final_sync_gets_one_recovery_attempt(tmp_path):
    gate = paused()
    capture, ops, seen = faulty(tmp_path, _gate=gate, fsync_interval_seconds=60)
    for n in range(3):
        capture.offer('synthetic', 'test', 0, str(n))
    ops.fail_fsync_times = 1       # the first fsync (at close) fails, the retry succeeds
    gate.set()
    assert capture.close()
    report = validate_raw_capture(tmp_path)
    assert report.finalized and report.verdict == 'incomplete', report.errors
    assert report.missing_ranges == [dict(first_seq=1, last_seq=3, reason='writer_error')]
    assert seen == ['degraded', 'writing', 'closed']


def test_file_shorter_than_its_checkpoint_fails_the_capture(tmp_path):
    capture, ops, seen = faulty(tmp_path)
    capture.offer('synthetic', 'test', 0, 'durable')
    eventually(lambda: capture._checkpoint['expected'] == 2)
    ops.fail_write = ops.fail_open = True
    capture.offer('synthetic', 'test', 0, 'lost')
    eventually(lambda: capture.status == 'degraded')
    (tmp_path / 'raw' / 'records.jsonl').write_bytes(b'')   # durable data vanished
    ops.fail_write = ops.fail_open = False
    eventually(lambda: capture.status == 'failed')
    assert isinstance(capture.writer_error, RuntimeError)
    assert not capture.close(timeout=1.0)


def test_offer_never_blocks_or_raises_while_degraded(tmp_path):
    capture, ops, seen = faulty(tmp_path, queue_capacity=3)
    ops.fail_write = True
    capture.offer('synthetic', 'test', 0, 'x')
    eventually(lambda: capture.status == 'degraded')
    started = time.monotonic()
    for _ in range(10_000):
        capture.offer('synthetic', 'test', 0, 'x')
    assert time.monotonic() - started < 2.0
    ops.fail_write = False
    eventually(lambda: capture.status == 'writing')
    assert capture.close()
    assert validate_raw_capture(tmp_path).finalized


# ---- bounded shutdown ---------------------------------------------------------

def test_close_with_a_full_queue_finishes_in_time(tmp_path):
    gate = paused()
    capture = make(tmp_path, gate=gate, queue_capacity=500)
    for n in range(2000):
        capture.offer('synthetic', 'test', 0, f'{n}')
    gate.set()
    started = time.monotonic()
    assert capture.close(timeout=5.0)
    assert time.monotonic() - started < 5.0
    report = validate_raw_capture(tmp_path)
    assert report.message_count == 500
    assert report.missing_ranges == [dict(first_seq=501, last_seq=2000, reason='queue_full')]


class HangingFsync(FileOps):
    """An fsync that blocks until released, like a disk that stopped responding."""
    def __init__(self):
        self.release = threading.Event()

    def fsync(self, fd):
        self.release.wait()
        super().fsync(fd)


def test_close_returns_false_when_the_writer_is_stuck(tmp_path):
    ops = HangingFsync()
    capture = make(tmp_path, file_ops=ops)
    try:
        capture.offer('synthetic', 'test', 0, 'stuck')
        started = time.monotonic()
        assert not capture.close(timeout=0.2)
        assert time.monotonic() - started < 1.0
        assert validate_raw_capture(tmp_path).verdict == 'incomplete'
    finally:
        ops.release.set()


def test_stuck_writer_that_finishes_later_still_finalizes(tmp_path):
    ops = HangingFsync()
    capture = make(tmp_path, file_ops=ops)
    capture.offer('synthetic', 'test', 0, 'late')
    assert not capture.close(timeout=0.1)
    ops.release.set()
    eventually(lambda: capture.status == 'closed')
    assert validate_raw_capture(tmp_path).verdict == 'complete'
