import json

import pytest

from src.raw_capture.records import RawGap, RawMessage, parse_line


def message(**overrides):
    fields = dict(seq=1, venue='kalshi', stream='public_ws', connection_epoch=0,
                  received_wall=1795190587.412, received_monotonic_ns=81234567890123,
                  payload='{"type":"trade"}')
    fields.update(overrides)
    return RawMessage(**fields)


@pytest.mark.parametrize('payload', [
    '{"type":"orderbook_delta","price":"0.62"}',
    'naïve 日本語 ✓ emoji 🚀',
    'quote " backslash \\ slash /',
    'controls \x00 \x01 \t \n \r \x1f \x7f',
    '',
    b'\x00\x01\xfe\xff binary',
    b'',
])
def test_payload_round_trips_exactly(payload):
    record = message(payload=payload)
    line = record.to_line()
    assert line.endswith(b'\n') and line.count(b'\n') == 1
    parsed = parse_line(line)
    assert parsed == record
    assert type(parsed.payload) is type(payload)
    assert parsed.encoding == ('utf8' if isinstance(payload, str) else 'base64')


def test_text_payload_is_stored_readable_not_base64():
    line = message(payload='{"market":"KXBTC"}').to_line()
    fields = json.loads(line)
    assert fields['encoding'] == 'utf8'
    assert fields['payload'] == '{"market":"KXBTC"}'


def test_gap_round_trips():
    gap = RawGap(first_seq=18, last_seq=40, reason='queue_full', detected_wall=1795190588.001)
    assert parse_line(gap.to_line()) == gap
    assert gap.count == 23


@pytest.mark.parametrize('overrides', [
    dict(seq=0), dict(seq=True), dict(seq=1.0),
    dict(venue='binance'),
    dict(stream=''), dict(stream='x' * 65), dict(stream=3),
    dict(connection_epoch=-1),
    dict(received_wall=float('nan')), dict(received_wall=float('inf')), dict(received_wall='1'),
    dict(received_wall=True),
    dict(received_monotonic_ns=-1), dict(received_monotonic_ns=1.5),
    dict(payload=bytearray(b'x')), dict(payload=None), dict(payload='\ud800'),
])
def test_invalid_message_fields_are_rejected(overrides):
    with pytest.raises(ValueError):
        message(**overrides)


@pytest.mark.parametrize('fields', [
    dict(first_seq=0, last_seq=1, reason='queue_full', detected_wall=1.0),
    dict(first_seq=5, last_seq=4, reason='queue_full', detected_wall=1.0),
    dict(first_seq=1, last_seq=1, reason='lost', detected_wall=1.0),
    dict(first_seq=1, last_seq=1, reason='oversize', detected_wall=float('nan')),
])
def test_invalid_gap_fields_are_rejected(fields):
    with pytest.raises(ValueError):
        RawGap(**fields)


def rewrite(line, **changes):
    fields = json.loads(line)
    for key, value in changes.items():
        if value is KeyError:
            del fields[key]
        else:
            fields[key] = value
    return json.dumps(fields, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode() + b'\n'


@pytest.mark.parametrize('mutate', [
    lambda line: line[:-1],                                   # no trailing newline
    lambda line: line.replace(b'"seq":1', b'"seq": 1'),       # non-canonical whitespace
    lambda line: b'{"v":1,' + line[1:].replace(b'"v":1,', b''),  # keys out of order
    lambda line: line.replace(b'}', b',"seq":1}'),            # duplicate key
    lambda line: rewrite(line, v=2),
    lambda line: rewrite(line, v=True),
    lambda line: rewrite(line, type='heartbeat'),
    lambda line: rewrite(line, extra=1),
    lambda line: rewrite(line, venue=KeyError),
    lambda line: rewrite(line, encoding='hex'),
    lambda line: rewrite(line, encoding='base64', payload='not base64!'),
    lambda line: rewrite(line, encoding='base64', payload='AB=='),   # non-canonical padding bits
    lambda line: rewrite(line, payload=7),
    lambda line: line.replace(b'1795190587.412', b'NaN'),
    lambda line: b'\xff' + line,
    lambda line: b'[1]\n',
])
def test_malformed_lines_are_rejected(mutate):
    with pytest.raises(ValueError):
        parse_line(mutate(message().to_line()))


def test_parse_requires_bytes():
    with pytest.raises(ValueError):
        parse_line(message().to_line().decode())
