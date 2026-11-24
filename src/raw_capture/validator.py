"""Check a raw capture directory and report whether it is complete, incomplete or invalid."""
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
import json
import math
import re

from .records import FORMAT_VERSION, GAP_REASONS, RawGap, parse_line

FORMAT_NAME = 'raw-capture'
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
STATUSES = frozenset({'writing', 'degraded', 'closed', 'failed'})
CONFIG_KEYS = frozenset({'queue_capacity', 'queue_bytes', 'max_payload_bytes',
                         'fsync_interval_seconds', 'retry_interval_seconds'})
MANIFEST_KEYS = frozenset({
    'format', 'format_version', 'session_id', 'code_revision', 'started_wall', 'ended_wall',
    'status', 'finalized', 'complete', 'config', 'max_line_bytes', 'message_count', 'gap_count',
    'missing_count', 'dropped', 'last_assigned_seq', 'records_sha256', 'records_bytes'})
SHA256_HEX = re.compile(r'[0-9a-f]{64}')


def max_line_bytes(max_payload_bytes):
    """Worst-case JSON escaping (6 bytes per payload byte) plus the envelope fields."""
    return 6 * max_payload_bytes + 1024


@dataclass
class RawCaptureReport:
    format_version: int | None = None
    status: str | None = None
    finalized: bool = False
    complete: bool = False
    message_count: int = 0
    first_seq: int | None = None
    last_seq: int | None = None
    last_assigned_seq: int | None = None
    missing_ranges: list = field(default_factory=list)
    dropped: dict = field(default_factory=lambda: dict.fromkeys(sorted(GAP_REASONS), 0))
    checksum: str = 'unavailable'
    truncated_tail: bool = False
    errors: list = field(default_factory=list)

    @property
    def verdict(self):
        if self.errors:
            return 'invalid'
        return 'complete' if self.complete else 'incomplete'

    def to_dict(self):
        return dict(vars(self))


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _count(value, name, minimum=0):
    _require(type(value) is int and value >= minimum, f'manifest {name} must be an integer >= {minimum}')


def _positive_number(value, name):
    _require(type(value) in (int, float) and math.isfinite(value) and value > 0,
             f'manifest {name} must be a positive finite number')


def _check_manifest(manifest):
    _require(type(manifest) is dict, 'manifest must be a JSON object')
    _require(manifest.get('format') == FORMAT_NAME, 'manifest format is not raw-capture')
    _require(type(manifest.get('format_version')) is int and manifest['format_version'] == FORMAT_VERSION,
             f'unsupported raw-capture format_version {manifest.get("format_version")!r}')
    _require(set(manifest) == MANIFEST_KEYS, 'manifest has missing or unknown keys')
    _require(type(manifest['session_id']) is str and type(manifest['code_revision']) is str,
             'manifest session_id and code_revision must be strings')
    _positive_number(manifest['started_wall'], 'started_wall')
    if manifest['ended_wall'] is not None:
        _positive_number(manifest['ended_wall'], 'ended_wall')
    _require(manifest['status'] in STATUSES, f'manifest status must be one of {sorted(STATUSES)}')
    _require(type(manifest['finalized']) is bool and type(manifest['complete']) is bool,
             'manifest finalized and complete must be booleans')
    config = manifest['config']
    _require(type(config) is dict and set(config) == CONFIG_KEYS, 'manifest config has missing or unknown keys')
    for name in ('queue_capacity', 'queue_bytes', 'max_payload_bytes'):
        _count(config[name], f'config.{name}', 1)
    for name in ('fsync_interval_seconds', 'retry_interval_seconds'):
        _positive_number(config[name], f'config.{name}')
    _require(manifest['max_line_bytes'] == max_line_bytes(config['max_payload_bytes']),
             'manifest max_line_bytes does not match config.max_payload_bytes')
    for name in ('message_count', 'gap_count', 'missing_count', 'last_assigned_seq'):
        _count(manifest[name], name)
    dropped = manifest['dropped']
    _require(type(dropped) is dict and set(dropped) == GAP_REASONS, 'manifest dropped has missing or unknown keys')
    for reason, value in dropped.items():
        _count(value, f'dropped.{reason}')
    finalized = manifest['finalized']
    _require(finalized == (manifest['status'] == 'closed'), 'manifest is finalized only when status is closed')
    if finalized:
        _require(type(manifest['records_sha256']) is str and SHA256_HEX.fullmatch(manifest['records_sha256']),
                 'finalized manifest needs a records_sha256')
        _count(manifest['records_bytes'], 'records_bytes')
        _require(manifest['ended_wall'] is not None, 'finalized manifest needs ended_wall')
    else:
        _require(manifest['records_sha256'] is None and manifest['records_bytes'] is None,
                 'unfinalized manifest must not claim a checksum')
    _require(manifest['complete'] == (finalized and manifest['missing_count'] == 0),
             'manifest complete must equal finalized with no missing records')


def _reject_constant(name):
    raise ValueError(f'manifest contains non-finite number {name}')


def _read_manifest(directory):
    with (directory / 'manifest.json').open('rb') as source:
        header = source.read(MAX_MANIFEST_BYTES + 1)
    _require(len(header) <= MAX_MANIFEST_BYTES, 'manifest exceeds 16 MiB')
    try:
        manifest = json.loads(header, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f'manifest is not JSON: {error}') from None
    _check_manifest(manifest)
    return manifest


def _scan(path, report, line_limit, finalized):
    """One streaming pass: check coverage and return (digest, bytes, gap count, highest seq covered)."""
    digest = sha256()
    size = 0
    gaps = 0
    expected = 1
    with path.open('rb') as source:
        while line := source.readline(line_limit + 1):
            _require(len(line) <= line_limit, f'record line at byte {size} exceeds max_line_bytes')
            if not line.endswith(b'\n'):
                _require(not finalized, f'truncated record line at byte {size}')
                report.truncated_tail = True
                break
            try:
                record = parse_line(line)
            except ValueError as error:
                raise ValueError(f'invalid record at byte {size}: {error}') from None
            if isinstance(record, RawGap):
                _require(record.first_seq == expected,
                         f'gap starting at seq {record.first_seq} does not follow seq {expected - 1}')
                report.missing_ranges.append(dict(first_seq=record.first_seq, last_seq=record.last_seq,
                                                  reason=record.reason))
                report.dropped[record.reason] += record.count
                gaps += 1
                expected = record.last_seq + 1
            else:
                _require(record.seq == expected, f'message seq {record.seq} where {expected} was expected')
                if report.first_seq is None:
                    report.first_seq = record.seq
                report.last_seq = record.seq
                report.message_count += 1
                expected += 1
            digest.update(line)
            size += len(line)
    return digest, size, gaps, expected - 1


def _raw_directory(directory):
    """Accept a session output directory (containing raw/) or the raw/ directory itself."""
    directory = Path(directory)
    return directory / 'raw' if (directory / 'raw').is_dir() else directory


def validate_raw_capture(directory):
    """Validate `<directory>/raw` (or `directory` itself if it holds the raw files)."""
    directory = _raw_directory(directory)
    report = RawCaptureReport()
    try:
        manifest = _read_manifest(directory)
        report.format_version = manifest['format_version']
        report.status = manifest['status']
        report.finalized = manifest['finalized']
        report.last_assigned_seq = manifest['last_assigned_seq']
        digest, size, gaps, covered = _scan(directory / 'records.jsonl', report,
                                            manifest['max_line_bytes'], manifest['finalized'])
        if manifest['finalized']:
            report.checksum = 'ok' if (digest.hexdigest() == manifest['records_sha256']
                                       and size == manifest['records_bytes']) else 'mismatch'
            _require(report.checksum == 'ok', 'records.jsonl does not match the manifest checksum')
            _require(covered == manifest['last_assigned_seq'],
                     f'records cover seq 1..{covered} but {manifest["last_assigned_seq"]} were assigned')
            missing = sum(item['last_seq'] - item['first_seq'] + 1 for item in report.missing_ranges)
            _require((report.message_count, gaps, missing, report.dropped)
                     == (manifest['message_count'], manifest['gap_count'], manifest['missing_count'],
                         manifest['dropped']),
                     'record counts do not match the manifest')
            report.complete = manifest['complete']
    except (OSError, ValueError) as error:
        report.errors.append(str(error))
        report.complete = False
    return report


def open_raw_capture(directory):
    """Return an iterator over a complete capture's records; raise ValueError otherwise.

    The iterator re-checks the checksum as it reads, so a file changed after
    validation raises at the end instead of passing silently.
    """
    report = validate_raw_capture(directory)
    if report.verdict != 'complete':
        reason = report.errors[0] if report.errors else (
            'capture is not finalized' if not report.finalized else 'capture has missing records')
        raise ValueError(f'raw capture is {report.verdict}: {reason}')
    directory = _raw_directory(directory)
    return _iter_records(directory / 'records.jsonl', _read_manifest(directory))


def _iter_records(path, manifest):
    digest = sha256()
    with path.open('rb') as source:
        while line := source.readline(manifest['max_line_bytes'] + 1):
            record = parse_line(line)
            digest.update(line)
            yield record
    if digest.hexdigest() != manifest['records_sha256']:
        raise ValueError('records.jsonl changed after validation')
