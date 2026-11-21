from hashlib import sha256
import json

import pytest

from src.raw_capture.records import RawGap, RawMessage
from src.raw_capture.validator import max_line_bytes, open_raw_capture, validate_raw_capture

MAX_PAYLOAD = 4096


def msg(seq, payload=None):
    return RawMessage(seq=seq, venue='synthetic', stream='test', connection_epoch=1,
                      received_wall=1795190000.0 + seq, received_monotonic_ns=1000 + seq,
                      payload=f'{{"n":{seq}}}' if payload is None else payload)


def gap(first, last, reason='queue_full'):
    return RawGap(first_seq=first, last_seq=last, reason=reason, detected_wall=1795190000.5)


def build(tmp_path, records, *, finalized=True, status=None, last_assigned=None, tail=b'', **overrides):
    """Write a raw capture by hand, with a manifest that matches `records` unless overridden."""
    raw = tmp_path / 'raw'
    raw.mkdir(parents=True)
    data = b''.join(record.to_line() for record in records) + tail
    (raw / 'records.jsonl').write_bytes(data)
    gaps = [record for record in records if isinstance(record, RawGap)]
    covered = max([getattr(r, 'seq', 0) for r in records] + [g.last_seq for g in gaps] + [0])
    dropped = {'oversize': 0, 'queue_full': 0, 'writer_error': 0}
    for item in gaps:
        dropped[item.reason] += item.count
    missing = sum(dropped.values())
    manifest = dict(
        format='raw-capture', format_version=1, session_id='s1', code_revision='abc',
        started_wall=1795190000.0, ended_wall=1795190100.0 if finalized else None,
        status=status or ('closed' if finalized else 'writing'),
        finalized=finalized, complete=finalized and missing == 0,
        config=dict(queue_capacity=10, queue_bytes=65536, max_payload_bytes=MAX_PAYLOAD,
                    fsync_interval_seconds=1.0, retry_interval_seconds=1.0),
        max_line_bytes=max_line_bytes(MAX_PAYLOAD),
        message_count=len(records) - len(gaps), gap_count=len(gaps), missing_count=missing,
        dropped=dropped, last_assigned_seq=covered if last_assigned is None else last_assigned,
        records_sha256=sha256(data).hexdigest() if finalized else None,
        records_bytes=len(data) if finalized else None)
    manifest.update(overrides)
    (raw / 'manifest.json').write_text(json.dumps(manifest))
    return tmp_path


def test_complete_capture(tmp_path):
    records = [msg(1), msg(2, b'\x00\xff'), msg(3, 'naïve ✓')]
    report = validate_raw_capture(build(tmp_path, records))
    assert report.verdict == 'complete', report.errors
    assert (report.message_count, report.first_seq, report.last_seq) == (3, 1, 3)
    assert report.checksum == 'ok' and report.missing_ranges == []
    assert list(open_raw_capture(tmp_path)) == records


def test_accepts_the_raw_directory_itself(tmp_path):
    build(tmp_path, [msg(1)])
    assert validate_raw_capture(tmp_path / 'raw').verdict == 'complete'


def test_gaps_make_a_valid_but_incomplete_capture(tmp_path):
    records = [msg(1), gap(2, 4), msg(5), gap(6, 6, 'oversize'), msg(7), gap(8, 9, 'writer_error')]
    report = validate_raw_capture(build(tmp_path, records))
    assert report.verdict == 'incomplete', report.errors
    assert report.finalized and not report.complete
    assert report.missing_ranges == [
        dict(first_seq=2, last_seq=4, reason='queue_full'),
        dict(first_seq=6, last_seq=6, reason='oversize'),
        dict(first_seq=8, last_seq=9, reason='writer_error')]
    assert report.dropped == {'oversize': 1, 'queue_full': 3, 'writer_error': 2}
    with pytest.raises(ValueError, match='incomplete: capture has missing records'):
        open_raw_capture(tmp_path)


def test_unfinalized_capture_with_truncated_tail_is_incomplete_not_invalid(tmp_path):
    partial = msg(3).to_line()[:20]
    report = validate_raw_capture(build(tmp_path, [msg(1), msg(2)], finalized=False, tail=partial))
    assert report.verdict == 'incomplete', report.errors
    assert report.truncated_tail and report.last_seq == 2 and report.checksum == 'unavailable'
    with pytest.raises(ValueError, match='not finalized'):
        open_raw_capture(tmp_path)


def test_failed_status_is_incomplete(tmp_path):
    report = validate_raw_capture(build(tmp_path, [msg(1)], finalized=False, status='failed'))
    assert report.verdict == 'incomplete', report.errors


def corrupt_byte(data):
    return data.replace(b'\\"n\\":2', b'\\"n\\":3')


def remove_line(data):
    lines = data.splitlines(keepends=True)
    return b''.join(lines[:1] + lines[2:])


def reorder_lines(data):
    lines = data.splitlines(keepends=True)
    return b''.join([lines[1], lines[0]] + lines[2:])


@pytest.mark.parametrize('tamper', [corrupt_byte, remove_line, reorder_lines])
def test_tampered_records_are_invalid(tmp_path, tamper):
    build(tmp_path, [msg(1), msg(2), msg(3)])
    path = tmp_path / 'raw' / 'records.jsonl'
    path.write_bytes(tamper(path.read_bytes()))
    report = validate_raw_capture(tmp_path)
    assert report.verdict == 'invalid'
    with pytest.raises(ValueError, match='invalid'):
        open_raw_capture(tmp_path)


@pytest.mark.parametrize('records', [
    [msg(1), msg(1)],                     # duplicate seq
    [msg(1), gap(2, 3), msg(3)],          # message inside a gap
    [msg(1), gap(1, 2), msg(3)],          # gap overlapping a message
    [msg(2)],                             # does not start at 1
    [msg(1), msg(3)],                     # silent hole
])
def test_sequence_violations_are_invalid(tmp_path, records):
    assert validate_raw_capture(build(tmp_path, records)).verdict == 'invalid'


def test_truncated_tail_in_finalized_capture_is_invalid(tmp_path):
    report = validate_raw_capture(build(tmp_path, [msg(1)], tail=b'{"v":1'))
    assert report.verdict == 'invalid'


def test_oversize_line_is_invalid(tmp_path):
    big = msg(1, '\x00' * MAX_PAYLOAD)         # escapes to 6 bytes per character: fits
    assert validate_raw_capture(build(tmp_path, [big])).verdict == 'complete'
    report = validate_raw_capture(build(tmp_path / 'b', [msg(1, 'x' * (7 * MAX_PAYLOAD))]))
    assert report.verdict == 'invalid' and 'max_line_bytes' in report.errors[0]


@pytest.mark.parametrize('overrides', [
    dict(format='paper-capture'),
    dict(format_version=2),
    dict(format_version=True),
    dict(extra_key=1),
    dict(status='running'),
    dict(status='writing'),                      # finalized but not closed
    dict(records_sha256='0' * 64),
    dict(records_bytes=1),
    dict(message_count=5),
    dict(gap_count=1),
    dict(missing_count=1),
    dict(last_assigned_seq=7),                   # trailing drops never recorded
    dict(dropped={'oversize': 0, 'queue_full': 1, 'writer_error': 0}),
    dict(complete=False),
    dict(max_line_bytes=10),
    dict(config=dict(queue_capacity=0, queue_bytes=1, max_payload_bytes=MAX_PAYLOAD,
                     fsync_interval_seconds=1.0, retry_interval_seconds=1.0)),
    dict(ended_wall=None),
])
def test_manifest_mismatches_are_invalid(tmp_path, overrides):
    report = validate_raw_capture(build(tmp_path, [msg(1), msg(2)], **overrides))
    assert report.verdict == 'invalid', overrides


def test_unfinalized_manifest_must_not_claim_a_checksum(tmp_path):
    report = validate_raw_capture(build(tmp_path, [msg(1)], finalized=False, records_sha256='0' * 64))
    assert report.verdict == 'invalid'


def test_missing_files_are_invalid(tmp_path):
    assert validate_raw_capture(tmp_path).verdict == 'invalid'
    build(tmp_path, [msg(1)])
    (tmp_path / 'raw' / 'records.jsonl').unlink()
    assert validate_raw_capture(tmp_path).verdict == 'invalid'


def test_manifest_with_nan_is_invalid(tmp_path):
    build(tmp_path, [msg(1)])
    path = tmp_path / 'raw' / 'manifest.json'
    path.write_text(path.read_text().replace('1795190000.0', 'NaN', 1))
    assert validate_raw_capture(tmp_path).verdict == 'invalid'


def test_iterator_detects_change_after_validation(tmp_path):
    build(tmp_path, [msg(1), msg(2)])
    records = open_raw_capture(tmp_path)
    path = tmp_path / 'raw' / 'records.jsonl'
    path.write_bytes(path.read_bytes().replace(b'\\"n\\":2', b'\\"n\\":9'))
    with pytest.raises(ValueError, match='changed after validation'):
        list(records)


def test_report_serializes_to_json(tmp_path):
    report = validate_raw_capture(build(tmp_path, [msg(1), gap(2, 2)]))
    fields = json.loads(json.dumps(report.to_dict()))
    assert set(fields) == {'format_version', 'status', 'finalized', 'complete', 'message_count', 'first_seq',
                           'last_seq', 'last_assigned_seq', 'missing_ranges', 'dropped', 'checksum',
                           'truncated_tail', 'errors'}
