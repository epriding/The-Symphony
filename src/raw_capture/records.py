"""Raw-capture record types and their one-line JSON form (format version 1).

A line is canonical: parsing it and serializing the result must give back the
same bytes. That single rule rejects reordered keys, extra whitespace,
duplicate keys and non-canonical base64 without a separate check for each.
"""
from base64 import b64decode, b64encode
from dataclasses import dataclass
import binascii
import json
import math

FORMAT_VERSION = 1
VENUES = frozenset({'polymarket_us', 'kalshi', 'synthetic'})
GAP_REASONS = frozenset({'queue_full', 'oversize', 'writer_error'})
MAX_STREAM_CHARS = 64


def _integer(value, name, minimum):
    if type(value) is not int or value < minimum:
        raise ValueError(f'{name} must be an integer >= {minimum}')


def _finite(value, name):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f'{name} must be a finite number')


def _serialize(fields):
    text = json.dumps(fields, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False)
    return text.encode('utf-8') + b'\n'


@dataclass(frozen=True, kw_only=True)
class RawMessage:
    seq: int
    venue: str
    stream: str
    connection_epoch: int
    received_wall: float
    received_monotonic_ns: int
    payload: str | bytes

    def __post_init__(self):
        _integer(self.seq, 'seq', 1)
        if self.venue not in VENUES:
            raise ValueError(f'venue must be one of {sorted(VENUES)}')
        if type(self.stream) is not str or not 0 < len(self.stream) <= MAX_STREAM_CHARS:
            raise ValueError(f'stream must be a string of 1 to {MAX_STREAM_CHARS} characters')
        _integer(self.connection_epoch, 'connection_epoch', 0)
        _finite(self.received_wall, 'received_wall')
        _integer(self.received_monotonic_ns, 'received_monotonic_ns', 0)
        if type(self.payload) is str:
            try:
                self.payload.encode('utf-8')
            except UnicodeEncodeError:
                raise ValueError('text payload must be valid UTF-8') from None
        elif type(self.payload) is not bytes:
            raise ValueError('payload must be str or bytes')

    @property
    def encoding(self):
        return 'utf8' if type(self.payload) is str else 'base64'

    def to_line(self):
        payload = self.payload if self.encoding == 'utf8' else b64encode(self.payload).decode('ascii')
        return _serialize(dict(
            v=FORMAT_VERSION, type='message', seq=self.seq, venue=self.venue, stream=self.stream,
            connection_epoch=self.connection_epoch, received_wall=self.received_wall,
            received_monotonic_ns=self.received_monotonic_ns, encoding=self.encoding,
            payload=payload))


@dataclass(frozen=True, kw_only=True)
class RawGap:
    first_seq: int
    last_seq: int
    reason: str
    detected_wall: float

    def __post_init__(self):
        _integer(self.first_seq, 'first_seq', 1)
        _integer(self.last_seq, 'last_seq', self.first_seq)
        if self.reason not in GAP_REASONS:
            raise ValueError(f'reason must be one of {sorted(GAP_REASONS)}')
        _finite(self.detected_wall, 'detected_wall')

    @property
    def count(self):
        return self.last_seq - self.first_seq + 1

    def to_line(self):
        return _serialize(dict(v=FORMAT_VERSION, type='gap', first_seq=self.first_seq,
                               last_seq=self.last_seq, reason=self.reason,
                               detected_wall=self.detected_wall))


MESSAGE_KEYS = frozenset({'v', 'type', 'seq', 'venue', 'stream', 'connection_epoch', 'received_wall',
                          'received_monotonic_ns', 'encoding', 'payload'})
GAP_KEYS = frozenset({'v', 'type', 'first_seq', 'last_seq', 'reason', 'detected_wall'})


def _reject_constant(name):
    raise ValueError(f'non-finite number {name} is not allowed')


def parse_line(line):
    """Parse one complete, canonical line (bytes ending in a newline)."""
    if type(line) is not bytes or not line.endswith(b'\n'):
        raise ValueError('record line must be bytes ending with a newline')
    try:
        fields = json.loads(line.decode('utf-8'), parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f'record line is not UTF-8 JSON: {error}') from None
    if type(fields) is not dict:
        raise ValueError('record line must be a JSON object')
    if fields.get('v') != FORMAT_VERSION or type(fields.get('v')) is not int:
        raise ValueError(f'unsupported record version {fields.get("v")!r}')
    kind = fields.get('type')
    if kind == 'message':
        if set(fields) != MESSAGE_KEYS:
            raise ValueError('message record has missing or unknown keys')
        payload = fields['payload']
        if type(payload) is not str:
            raise ValueError('payload must be a JSON string')
        if fields['encoding'] == 'base64':
            try:
                payload = b64decode(payload, validate=True)
            except binascii.Error:
                raise ValueError('payload is not valid base64') from None
        elif fields['encoding'] != 'utf8':
            raise ValueError('encoding must be utf8 or base64')
        record = RawMessage(seq=fields['seq'], venue=fields['venue'], stream=fields['stream'],
                            connection_epoch=fields['connection_epoch'],
                            received_wall=fields['received_wall'],
                            received_monotonic_ns=fields['received_monotonic_ns'], payload=payload)
    elif kind == 'gap':
        if set(fields) != GAP_KEYS:
            raise ValueError('gap record has missing or unknown keys')
        record = RawGap(first_seq=fields['first_seq'], last_seq=fields['last_seq'],
                        reason=fields['reason'], detected_wall=fields['detected_wall'])
    else:
        raise ValueError(f'unknown record type {kind!r}')
    if record.to_line() != line:
        raise ValueError('record line is not in canonical form')
    return record
