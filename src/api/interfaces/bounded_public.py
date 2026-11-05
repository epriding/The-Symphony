"""Finite, read-only transports for the pinned public-market SDK contract.

No account/order endpoint or global package patch is used. Limits apply before
JSON decoding; compressed responses are refused to avoid unbounded expansion.
"""
import asyncio
from importlib.metadata import version
import json
import re

from polymarket_us import AsyncPolymarketUS
from polymarket_us.auth import create_auth_headers
from polymarket_us.websocket import MarketsWebSocket
import websockets


class PublicCapacityError(RuntimeError):
    """A configured public-input bound was exceeded; stop rather than recover silently."""


def positive_limit(value):
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError('A finite positive public input byte limit is required')
    return value


def _finite_json(data):
    def invalid(value):
        raise ValueError('Nonfinite public JSON number')
    return json.loads(data, parse_constant=invalid)


async def read_json_limited(response, max_bytes):
    limit = positive_limit(max_bytes)
    encoding = response.headers.get('Content-Encoding', 'identity').lower()
    if encoding not in ('', 'identity'):
        raise PublicCapacityError('Compressed public response cannot satisfy the input bound')
    length = response.headers.get('Content-Length')
    if length is not None and int(length) > limit:
        raise PublicCapacityError('Public HTTP response byte capacity exceeded')
    data = bytearray()
    chunks = (response.aiter_bytes(chunk_size=min(8192, limit + 1))
              if hasattr(response, 'aiter_bytes') else
              response.content.iter_chunked(min(8192, limit + 1)))
    async for chunk in chunks:
        if len(data) + len(chunk) > limit:
            raise PublicCapacityError('Public HTTP response byte capacity exceeded')
        data.extend(chunk)
    return _finite_json(data) if data else {}


def verify_sdk():
    if version('polymarket-us') != '0.1.2':
        raise RuntimeError('Bounded public transport requires polymarket-us==0.1.2')


class BoundedPublicHTTP(AsyncPolymarketUS):
    """Keep SDK public resources/serialization; replace only its eager GET transport."""
    def __init__(self, *, max_event_bytes, **kwargs):
        verify_sdk()
        self.max_event_bytes = positive_limit(max_event_bytes)
        for key in ('key_id', 'secret_key'):
            if kwargs.get(key) and len(kwargs[key].encode('utf-8')) > self.max_event_bytes:
                raise ValueError('Public credential input byte capacity exceeded')
        super().__init__(**kwargs)

    async def _request(self, method, path, *, query=None, body=None, authenticated=False):
        allowed = re.fullmatch(r'/v1/(?:markets(?:/[^/]+/(?:book|bbo|settlement))?|market/(?:id|slug)/[^/]+)', path)
        if method != 'GET' or authenticated or body is not None or allowed is None:
            raise ValueError('Only public market GET operations are permitted')
        params = self._build_query_params(query) if query else None
        async with self._http.stream('GET', self.gateway_base_url + path, params=params,
                                     headers={'Accept-Encoding': 'identity'}) as response:
            # Avoid SDK errors containing entire raw response bodies or credentials.
            if not response.is_success:
                raise RuntimeError(f'Public market HTTP status {response.status_code}')
            return await read_json_limited(response, self.max_event_bytes)


class BoundedPolymarketPublicAPI:
    def __init__(self, key_id, secret_key, *, max_event_bytes):
        self.client = BoundedPublicHTTP(key_id=key_id, secret_key=secret_key,
                                        max_event_bytes=max_event_bytes)

    async def get_market_by_slug(self, slug):
        from urllib.parse import quote
        return await self.client.markets.retrieve_by_slug(quote(slug, safe=''))

    async def get_markets(self, active=True, limit=100, offset=0):
        return await self.client.markets.list({'active': active, 'closed': not active,
                                               'limit': limit, 'offset': offset})

    async def get_orderbook(self, slug, outcome='YES'):
        from urllib.parse import quote
        if outcome != 'YES':
            raise ValueError('Public transport uses the canonical YES book')
        result = await self.client.markets.book(quote(slug, safe=''))
        return result.get('marketData', result)

    async def get_settlement(self, slug):
        from urllib.parse import quote
        return await self.client.markets.settlement(quote(slug, safe=''))


class BoundedMarketsWebSocket(MarketsWebSocket):
    def __init__(self, *, max_event_bytes, max_queue, **kwargs):
        verify_sdk()
        self.max_event_bytes = positive_limit(max_event_bytes)
        self.max_queue = positive_limit(max_queue)
        super().__init__(**kwargs)

    async def connect(self):
        headers = create_auth_headers(self.key_id, self.secret_key, 'GET', self.path)
        self._ws = await websockets.connect(self.base_url + self.path,
            additional_headers=headers, max_size=self.max_event_bytes,
            max_queue=self.max_queue, compression=None, close_timeout=2,
            open_timeout=15, write_limit=min(self.max_event_bytes, 32768))
        self._emit('open')
        self._message_task = asyncio.create_task(self._message_loop())

    def _handle_message(self, data):
        if len(data.encode('utf-8') if isinstance(data, str) else data) > self.max_event_bytes:
            raise PublicCapacityError('Public websocket message byte capacity exceeded')
        # Validate finiteness before the SDK's bounded parser/event dispatch.
        _finite_json(data)
        super()._handle_message(data)

    async def _message_loop(self):
        # The SDK swallows arbitrary exceptions into unstructured error strings;
        # retain typed capacity failures for the resource owner's fatal path.
        try:
            async for message in self._ws:
                self._handle_message(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if any(getattr(getattr(error, frame, None), 'code', None) == 1009
                   for frame in ('rcvd', 'sent')):
                error = PublicCapacityError('Public websocket message byte capacity exceeded')
            self._emit('error', error)
        finally:
            self._emit('close')
