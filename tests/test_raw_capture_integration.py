"""Raw capture wired into the venue adapters and the runtime."""
import asyncio
from types import SimpleNamespace

import aiohttp
import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from src.api.interfaces import bounded_public as transport
from src.api.interfaces import kalshi as kalshi_module
from src.api.interfaces.kalshi import KalshiMarketData


class Tap:
    """Stands in for RawCapture and records what the adapters hand it."""
    def __init__(self):
        self.calls = []

    def offer(self, venue, stream, epoch, payload):
        self.calls.append((venue, stream, epoch, payload))

    def mark_oversize(self, venue, stream, epoch):
        self.calls.append((venue, stream, epoch, 'OVERSIZE'))


# ---- Kalshi: the real stream() loop over a fake aiohttp socket ----


def frame(kind, data=None):
    return aiohttp.WSMessage(kind, data, None)


class FakeKalshiSocket:
    def __init__(self, frames, error=None):
        self.frames, self.error, self.sent = frames, error, []

    async def send_json(self, payload):
        self.sent.append(payload)

    def exception(self):
        return self.error

    async def __aiter__(self):
        for item in self.frames:
            yield item


class FakeClientSession:
    """Replaces aiohttp.ClientSession inside the Kalshi module only."""
    socket = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def ws_connect(self, url, **kwargs):
        socket = self.socket

        class Connection:
            async def __aenter__(self):
                return socket

            async def __aexit__(self, *exc):
                return False
        return Connection()


def kalshi_feed(monkeypatch, frames, *, error=None, raw=None):
    FakeClientSession.socket = FakeKalshiSocket(frames, error)
    local = SimpleNamespace(**vars(aiohttp))
    local.ClientSession = FakeClientSession
    monkeypatch.setattr(kalshi_module, 'aiohttp', local)
    api = SimpleNamespace(websocket_headers=lambda: {})
    feed = KalshiMarketData(['m'], api=api, max_event_bytes=4096)
    feed.raw_capture = raw
    return feed


async def until_disconnected(stream):
    """Collect events until the feed reports the connection lost (before its retry sleep)."""
    events = []
    async for envelope in stream:
        events.append(envelope)
        if len(events) > 1 and not getattr(envelope.event, 'connected', True):
            break
    await stream.aclose()
    return events


KALSHI_FRAMES = [
    frame(aiohttp.WSMsgType.TEXT, '{"type":"subscribed","id":1}'),
    frame(aiohttp.WSMsgType.TEXT, '{"type":"orderbook_delta","msg":{"market_ticker":"other"}}'),
    frame(aiohttp.WSMsgType.PING, b''),
    frame(aiohttp.WSMsgType.BINARY, b'\x00\x01binary'),
    frame(aiohttp.WSMsgType.TEXT, 'not json'),
]


@pytest.mark.asyncio
async def test_kalshi_offers_every_data_frame_before_parsing(monkeypatch):
    tap = Tap()
    feed = kalshi_feed(monkeypatch, KALSHI_FRAMES, raw=tap)
    events = await asyncio.wait_for(until_disconnected(feed.stream()), 2)
    assert events[-1].event.reason == 'public_feed_recovery:JSONDecodeError'
    # Every text and binary frame, in order, including the one that failed to parse. No ping.
    assert tap.calls == [
        ('kalshi', 'public_ws', 1, '{"type":"subscribed","id":1}'),
        ('kalshi', 'public_ws', 1, '{"type":"orderbook_delta","msg":{"market_ticker":"other"}}'),
        ('kalshi', 'public_ws', 1, b'\x00\x01binary'),
        ('kalshi', 'public_ws', 1, 'not json'),
    ]


@pytest.mark.asyncio
async def test_kalshi_without_a_tap_behaves_the_same(monkeypatch):
    with_tap = await asyncio.wait_for(until_disconnected(
        kalshi_feed(monkeypatch, KALSHI_FRAMES, raw=Tap()).stream()), 2)
    without = await asyncio.wait_for(until_disconnected(
        kalshi_feed(monkeypatch, KALSHI_FRAMES).stream()), 2)
    assert [(e.event.connected, e.event.reason) for e in with_tap] == \
           [(e.event.connected, e.event.reason) for e in without]


@pytest.mark.asyncio
async def test_kalshi_frame_refused_for_size_is_marked(monkeypatch):
    tap = Tap()
    error = aiohttp.WebSocketError(1009, 'Message size exceeds limit')
    feed = kalshi_feed(monkeypatch, [frame(aiohttp.WSMsgType.TEXT, '{"a":1}'),
                                     frame(aiohttp.WSMsgType.ERROR, error)], error=error, raw=tap)
    stream = feed.stream()
    assert (await anext(stream)).event.connected
    with pytest.raises(transport.PublicCapacityError):
        await anext(stream)
    assert tap.calls == [('kalshi', 'public_ws', 1, '{"a":1}'), ('kalshi', 'public_ws', 1, 'OVERSIZE')]


# ---- Polymarket: the real _message_loop over a fake websockets socket ----


class FakeUSSocket:
    def __init__(self, messages, error=None):
        self.messages, self.error = messages, error

    async def __aiter__(self):
        for message in self.messages:
            yield message
        if self.error is not None:
            raise self.error


def us_websocket(messages, *, error=None, raw=None, epoch=3):
    client = transport.BoundedMarketsWebSocket(max_event_bytes=4096, max_queue=8, raw_capture=raw,
                                               epoch=epoch, key_id='dummy', secret_key='dummy')
    client._ws = FakeUSSocket(messages, error)
    seen = []
    for name in ('market_data', 'heartbeat', 'error'):
        client.on(name, lambda *args, name=name: seen.append(name))
    return client, seen


US_MESSAGES = [
    '{"heartbeat":{}}',
    '{"marketData":{"marketSlug":"m"}}',
    '{"somethingNew":1}',
    b'{"marketData":{"marketSlug":"bytes"}}',
    '{"price": NaN}',
]


@pytest.mark.asyncio
async def test_polymarket_offers_every_frame_through_the_real_message_loop():
    tap = Tap()
    client, seen = us_websocket(US_MESSAGES, raw=tap)
    await client._message_loop()
    assert tap.calls == [('polymarket_us', 'markets_ws', 3, message) for message in US_MESSAGES]
    assert seen == ['heartbeat', 'market_data', 'market_data', 'error']   # NaN frame rejected after the tap


@pytest.mark.asyncio
async def test_polymarket_without_a_tap_behaves_the_same():
    tapped, with_tap = us_websocket(US_MESSAGES, raw=Tap())
    plain, without = us_websocket(US_MESSAGES)
    await tapped._message_loop()
    await plain._message_loop()
    assert with_tap == without


@pytest.mark.asyncio
@pytest.mark.parametrize('received', [False, True])
async def test_polymarket_frame_refused_for_size_is_marked(received):
    close = Close(1009, 'too big')
    error = ConnectionClosedError(close if received else None, None if received else close, None)
    tap = Tap()
    client, seen = us_websocket(['{"heartbeat":{}}'], error=error, raw=tap)
    await client._message_loop()
    assert tap.calls == [('polymarket_us', 'markets_ws', 3, '{"heartbeat":{}}'),
                         ('polymarket_us', 'markets_ws', 3, 'OVERSIZE')]
    assert seen[-1] == 'error'


@pytest.mark.asyncio
async def test_polymarket_other_disconnects_are_not_marked():
    tap = Tap()
    error = ConnectionClosedError(None, Close(1011, 'server error'), None)
    client, _ = us_websocket([], error=error, raw=tap)
    await client._message_loop()
    assert tap.calls == []
