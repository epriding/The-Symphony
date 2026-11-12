"""Finite local RPC client. Requests never launch services or retry mutations."""
import asyncio
import json
from pathlib import Path
from uuid import uuid4

from .protocol import (
    CONTROL_OPS, DEADLINE, MAX_REQUEST, MAX_RESPONSE, MAX_RESPONSE_OBJECT,
    OPERATIONS, _pairs, _validate_tree, bounded_text, decode_request, invalid,
)
from ..paper_trading.runtime_config import retained_bytes
from .client_schema import validate_data


class ClientError(Exception):
    def __init__(self, code: str, *, unknown: bool = False):
        super().__init__(code)
        self.code, self.unknown = code, unknown


def decode_reply(raw: bytes, request: dict) -> dict:
    if len(raw)>MAX_RESPONSE or not raw.endswith(b'\n'):
        raise ClientError('invalid_response')
    # Bound nesting and parser container overhead before constructing objects.
    quoted=escaped=False
    depth=nodes=0
    for byte in raw:
        if quoted:
            if escaped:
                escaped=False
            elif byte==92:
                escaped=True
            elif byte==34:
                quoted=False
        elif byte==34:
            quoted=True
        elif byte in (91,123):
            depth+=1
            nodes+=1
        elif byte in (93,125):
            depth-=1
        elif byte in (44,58):
            nodes+=1
        if depth>16 or nodes>16384:
            raise ClientError('invalid_response')
    try:
        value=json.loads(raw.decode('utf-8'),object_pairs_hook=_pairs,parse_constant=invalid)
        _validate_tree(value)
        if not isinstance(value,dict) or retained_bytes(value)>MAX_RESPONSE_OBJECT:
            raise ValueError()
        if type(value['version']) is not int or value['version']!=1:
            raise ValueError()
        if value['request_id']!=request['request_id'] or type(value['ok']) is not bool:
            raise ValueError()
        bounded_text(value['service_id'],256,identifier=True)
        bounded_text(value['session_id'],256,nullable=True,identifier=True)
        if type(value['revision']) is not int or value['revision']<0:
            raise ValueError()
        if type(value['observed_at']) not in (int,float):
            raise ValueError()
        if not value['ok'] and not isinstance(value.get('error'),dict):
            raise ValueError()
        if request['service_id'] is not None:
            if value['service_id']!=request['service_id']:
                raise ValueError()
            if request['op']!='start' and value['session_id']!=request['session_id']:
                # A real conflict response can announce the replacement target.
                if value['ok'] or value.get('error',{}).get('code')!='conflict':
                    raise ValueError()
        if not value['ok']:
            if value['data'] is not None or not isinstance(value['error'],dict):
                raise ValueError()
            if value['error'].get('code') not in (
                    'invalid_request','unsupported_version','not_found','conflict',
                    'busy','limit_exceeded','internal_error'):
                raise ValueError()
            raise ClientError(value['error']['code'])
        data=value['data']
        if not isinstance(data,dict):
            raise ValueError()
        if request['op'] in CONTROL_OPS:
            if data.get('accepted') is not True:
                raise ValueError()
        elif request['op']=='overview':
            if data.get('lifecycle') not in ('idle','starting','ready','degraded','stopping','stopped','failed'):
                raise ValueError()
            if data.get('operator_state') not in ('running','pausing','paused'):
                raise ValueError()
        elif request['op'] in ('strategies','findings') or request['params'].get('collection'):
            if (not isinstance(data.get('items'),list) or len(data['items'])>100
                    or any(not isinstance(row,dict) for row in data['items'])
                    or type(data.get('has_more')) is not bool
                    or data.get('scope') not in ('active','recent')):
                raise ValueError()
            bounded_text(data['next_cursor'],2048,nullable=True)
        validate_data(data,request,value['session_id'])
        return value
    except ClientError:
        raise
    except (ValueError,KeyError,TypeError,UnicodeError,RecursionError,OverflowError):
        raise ClientError('invalid_response') from None


class RuntimeClient:
    def __init__(self, path: str | Path, *, timeout: float = DEADLINE):
        if not 0<timeout<=DEADLINE:
            raise ValueError('Client deadline must be positive and at most five seconds')
        self.path=str(path)
        self.timeout=timeout
        self._writers=set()
        self._busy=set()
        self._closed=False
        self.highwater=0
        self._payloads={}
        self.payload_highwater=0

    @property
    def payload_bytes(self) -> int:
        return sum(self._payloads.values())

    @property
    def connection_count(self) -> int:
        return len(self._writers)

    async def request(self, op: str, *, target: tuple | None = None,
                      params: dict | None = None) -> dict:
        if not isinstance(op,str) or op not in OPERATIONS:
            raise ClientError('invalid_request')
        mutation=op in CONTROL_OPS or op=='publish_finding'
        lane='control' if mutation else 'read'
        if self._closed or lane in self._busy:
            raise ClientError('closed' if self._closed else 'busy')
        if target is None and op!='overview':
            raise ClientError('target_required')
        if (target is not None and (not isinstance(target,tuple) or len(target)!=2)
                or params is not None and (not isinstance(params,dict)
                    or any(type(value) not in (str,int) for value in params.values()))):
            raise ClientError('invalid_request')
        try:
            if target is not None:
                bounded_text(target[0],256,identifier=True)
                bounded_text(target[1],256,nullable=True,identifier=True)
        except ValueError:
            raise ClientError('invalid_request') from None
        request=dict(version=1,request_id=str(uuid4()),op=op,
            service_id=target[0] if target else None,
            session_id=target[1] if target else None,params=dict(params) if params is not None else {})
        try:
            if retained_bytes(request)>MAX_REQUEST:
                raise ValueError()
            raw=(json.dumps(request,allow_nan=False,ensure_ascii=False)+'\n').encode()
            decode_request(raw)
        except (ValueError,TypeError,UnicodeError,RecursionError):
            raise ClientError('invalid_request') from None
        self._busy.add(lane)
        self._payloads[lane]=len(raw)
        writer=None
        sent=False
        try:
            async with asyncio.timeout(self.timeout):
                reader,writer=await asyncio.open_unix_connection(self.path,limit=64*1024)
                self._writers.add(writer)
                self.highwater=max(self.highwater,len(self._writers))
                if self._closed:
                    raise ClientError('closed')
                writer.write(raw)
                sent=True
                await writer.drain()
                reply=bytearray()
                while True:
                    chunk=await reader.read(64*1024)
                    if not chunk or len(reply)+len(chunk)>MAX_RESPONSE:
                        raise ClientError('invalid_response',unknown=mutation)
                    reply.extend(chunk)
                    self._payloads[lane]=len(raw)+len(reply)
                    self.payload_highwater=max(self.payload_highwater,self.payload_bytes)
                    if b'\n' in chunk:
                        if not reply.endswith(b'\n') or reply.count(b'\n')!=1:
                            raise ClientError('invalid_response',unknown=mutation)
                        break
                try:
                    return decode_reply(reply,request)
                except ClientError as error:
                    error.unknown=mutation and error.code in ('invalid_response','internal_error')
                    raise
        except (TimeoutError,OSError,ValueError):
            raise ClientError('connection_lost',unknown=mutation and sent) from None
        finally:
            self._busy.discard(lane)
            self._payloads.pop(lane,None)
            if writer is not None:
                self._writers.discard(writer)
                writer.transport.abort()

    async def close(self) -> None:
        self._closed=True
        for writer in tuple(self._writers):
            writer.transport.abort()
        await asyncio.sleep(0)
