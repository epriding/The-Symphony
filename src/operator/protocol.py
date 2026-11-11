"""Finite version-1 JSON frames. Public errors never include raw input."""
import json
import math
import unicodedata

from ..paper_trading.runtime_config import retained_bytes

MAX_REQUEST = 64 * 1024
MAX_RESPONSE = 1024 * 1024
DEADLINE = 5.0
CONTROL_OPS = frozenset(('start','pause','resume','stop'))
SERVICE_BUDGET = 16 * 1024 * 1024
# Conservative reservations, not an RSS claim: bounded stream/decode buffers,
# the copied model, and two large response builds/writes can coexist.
STATIC_RESERVE = 9 * 1024 * 1024
QUERY_RESERVE = 7 * 1024 * 1024 // 2
MAX_RESPONSE_OBJECT = 512 * 1024
OPERATIONS = {op: set() for op in ('overview', 'start', 'pause', 'resume', 'stop')}
OPERATIONS.update(strategies={'limit','cursor'}, strategy={'name','collection','limit','cursor'})
OPERATIONS.update(findings={'name','limit','cursor'},
    publish_finding={'name','author','severity','message','evidence'})


class ProtocolError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def invalid(message='Invalid request'):
    raise ProtocolError('invalid_request', message)


def bounded_text(value, limit, *, nullable=False, identifier=False):
    if nullable and value is None:
        return value
    if not isinstance(value, str) or not value:
        invalid('Invalid text field')
    try:
        length=len(value.encode('utf-8'))
    except UnicodeError:
        invalid('Invalid Unicode text')
    if length>limit or (identifier and any(unicodedata.category(c) in ('Cc','Cs') for c in value)):
        invalid('Invalid text field')
    return value


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            invalid('Duplicate JSON field')
        result[key] = value
    return result


def _validate_tree(value, depth=0):
    if depth > 16:
        invalid('JSON nesting exceeds 16 levels')
    if isinstance(value, float) and not math.isfinite(value):
        invalid('Nonfinite numeric value')
    if isinstance(value, dict):
        for child in value.values():
            _validate_tree(child, depth+1)
    elif isinstance(value, list):
        for child in value:
            _validate_tree(child, depth+1)


def decode_request(frame):
    if len(frame) > MAX_REQUEST or not frame.endswith(b'\n'):
        raise ProtocolError('limit_exceeded', 'Request frame exceeds limit or is incomplete')
    try:
        value = json.loads(frame.decode('utf-8'), object_pairs_hook=_pairs,
            parse_constant=lambda _: invalid('Nonfinite numeric value'))
        _validate_tree(value)
    except ProtocolError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        invalid('Malformed UTF-8 JSON')
    if not isinstance(value, dict) or set(value) != {
            'version', 'request_id', 'op', 'service_id', 'session_id', 'params'}:
        invalid('Unexpected request fields')
    if type(value['version']) is not int:
        invalid('Version must be an integer')
    if value['version'] != 1:
        raise ProtocolError('unsupported_version', 'Only protocol version 1 is supported')
    bounded_text(value['request_id'], 128,identifier=True)
    bounded_text(value['service_id'], 256, nullable=True,identifier=True)
    bounded_text(value['session_id'], 256, nullable=True,identifier=True)
    op = value['op']
    if not isinstance(op, str) or op not in OPERATIONS:
        invalid('Unknown operation')
    if not isinstance(value['params'], dict) or set(value['params']) - OPERATIONS[op]:
        invalid('Unexpected operation parameters')
    params=value['params']
    if 'limit' in params and (type(params['limit']) is not int or not 1<=params['limit']<=100):
        invalid('Page limit must be an integer from 1 to 100')
    if 'cursor' in params:
        bounded_text(params['cursor'],2048)
    if op == 'strategy':
        bounded_text(value['params'].get('name'),256)
        if value['params'].get('collection') not in (None,'orders','positions','fills','events'):
            invalid('Unknown strategy collection')
        if not params.get('collection') and set(params)&{'limit','cursor'}:
            invalid('Paging requires a strategy collection')
    if op in ('findings','publish_finding') and 'name' in params:
        bounded_text(params['name'],256)
    if op == 'publish_finding':
        bounded_text(params.get('author'),128)
        bounded_text(params.get('message'),2048)
        if not isinstance(params.get('severity'),str) or params['severity'] not in ('info','warning','error'):
            invalid('Unknown finding severity')
        if 'evidence' in params:
            bounded_text(params['evidence'],2048)
    if retained_bytes(value)>MAX_REQUEST:
        raise ProtocolError('limit_exceeded','Decoded request exceeds memory limit')
    return value


def encode_response(response):
    if retained_bytes(response)>MAX_RESPONSE_OBJECT:
        raise ProtocolError('limit_exceeded','Response object exceeds memory limit; request a smaller page')
    payload=bytearray()
    encoder=json.JSONEncoder(allow_nan=False,ensure_ascii=False,separators=(',',':'))
    try:
        for part in encoder.iterencode(response):
            encoded=part.encode('utf-8')
            if len(payload)+len(encoded)+1>MAX_RESPONSE:
                raise ProtocolError('limit_exceeded','Response exceeds frame limit; request a smaller page')
            payload.extend(encoded)
    except ProtocolError:
        raise
    except (ValueError,UnicodeError,RecursionError):
        raise ProtocolError('internal_error','Observation cannot be represented as finite UTF-8 JSON') from None
    payload.append(10)
    return payload
