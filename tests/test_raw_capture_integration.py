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


# ---- The runtime: create, connect, close and report ----

import json

from src.bot import BotRunner
from src.paper_trading.clock import VirtualClock
from src.raw_capture import capture as capture_module
from src.raw_capture.validator import open_raw_capture, validate_raw_capture

from bot_v1_support import START, VENUES, ControlledFeed, eventually, strategy_config, valid_config


class Idle:
    async def on_snapshot(self, snapshot):
        return None


class TappedFeed(ControlledFeed):
    """A fixture feed that hands each pushed book to raw capture, as a real adapter would."""
    async def push_book(self, **kwargs):
        raw = getattr(self, 'raw_capture', None)
        if raw is not None:
            raw.offer('synthetic', self.exchange.lower(), 1, json.dumps({'book': self.emitted}))
        await super().push_book(**kwargs)


def raw_section(**overrides):
    value = dict(enabled=True, queue_capacity=100, queue_bytes=262144, fsync_interval_seconds=0.05)
    value.update(overrides)
    return value


async def run_session(directory, raw=None, *, books=3):
    config = valid_config(strategies=[strategy_config('idle', factory='idle')])
    if raw is not None:
        config['raw_capture'] = raw
    clock = VirtualClock(START)
    feeds = [TappedFeed(venue, clock=clock) for venue in VENUES]
    runner = BotRunner(config, output_directory=directory, feeds=feeds, clock=clock,
                       strategy_factories={'idle': lambda context: Idle()})
    task = asyncio.create_task(runner.run())
    await eventually(lambda: task.done() or all(feed.started.is_set() for feed in feeds), task=task)
    for _ in range(books):
        clock.advance(clock() + .05)
        for feed in feeds:
            await feed.push_book()
    for feed in feeds:
        await feed.end()
    summary = await asyncio.wait_for(task, 5)
    return summary, feeds, json.loads((directory / 'outcome.json').read_text())


@pytest.mark.asyncio
async def test_run_with_raw_capture_produces_a_complete_capture(tmp_path):
    summary, feeds, outcome = await run_session(tmp_path / 'out', raw_section())
    report = validate_raw_capture(tmp_path / 'out')
    assert report.verdict == 'complete', report.errors
    records = list(open_raw_capture(tmp_path / 'out'))
    assert len(records) == 6 and {r.stream for r in records} == {'polymarket_us', 'kalshi'}
    assert outcome['clean'] and outcome['raw_capture'] == dict(
        status='closed', finalized=True, complete=True, last_assigned_seq=6, message_count=6,
        missing_count=0, dropped={'oversize': 0, 'queue_full': 0, 'writer_error': 0}, error=None)
    assert summary['runtime']['raw_capture'] == outcome['raw_capture']
    paper = json.loads((tmp_path / 'out' / 'manifest.json').read_text())
    raw = json.loads((tmp_path / 'out' / 'raw' / 'manifest.json').read_text())
    assert paper['schema_version'] == 5
    assert raw['session_id'] == summary['session_id'] and raw['code_revision'] == paper['code_revision']
    assert raw['config']['max_payload_bytes'] == 262144 and raw['config']['fsync_interval_seconds'] == 0.05


@pytest.mark.asyncio
@pytest.mark.parametrize('raw', [None, raw_section(enabled=False)])
async def test_run_without_raw_capture_is_unchanged(tmp_path, raw):
    _, feeds, outcome = await run_session(tmp_path / 'out', raw)
    assert not (tmp_path / 'out' / 'raw').exists()
    assert 'raw_capture' not in outcome
    assert all(not hasattr(feed, 'raw_capture') for feed in feeds)


@pytest.mark.asyncio
async def test_disk_failure_leaves_the_run_clean_and_is_reported(tmp_path, monkeypatch):
    def broken_write(self, fd, data):
        raise OSError('disk full')
    monkeypatch.setattr(capture_module.FileOps, 'write', broken_write)
    summary, _, outcome = await run_session(tmp_path / 'out', raw_section())
    assert outcome['clean'] and outcome['status']['phase'] == 'stopped'
    raw = outcome['raw_capture']
    assert (raw['status'], raw['finalized'], raw['complete']) == ('failed', False, False)
    assert raw['missing_count'] == 6 and raw['error'] == 'OSError: disk full'
    log = (tmp_path / 'out' / 'runtime.log').read_text()
    assert 'raw capture degraded, discarding frames until the disk recovers: OSError: disk full' in log
    assert 'raw capture failed and is incomplete' in log
    assert validate_raw_capture(tmp_path / 'out').verdict == 'incomplete'


@pytest.mark.asyncio
async def test_raw_capture_that_cannot_start_stops_the_run_before_trading(tmp_path, monkeypatch):
    def refuse(self, *args, **kwargs):
        raise OSError('cannot create raw/')
    monkeypatch.setattr(capture_module.RawCapture, '__init__', refuse)
    config = valid_config(strategies=[strategy_config('idle', factory='idle')])
    config['raw_capture'] = raw_section()
    feeds = [TappedFeed(venue) for venue in VENUES]
    runner = BotRunner(config, output_directory=tmp_path / 'out', feeds=feeds,
                       strategy_factories={'idle': lambda context: Idle()})
    with pytest.raises(OSError, match='cannot create raw/'):
        await asyncio.wait_for(runner.run(), 5)
    assert not any(feed.started.is_set() for feed in feeds)
    assert json.loads((tmp_path / 'out' / 'outcome.json').read_text())['clean'] is False


def test_cli_warns_only_when_the_raw_capture_is_incomplete():
    from main import raw_capture_warning
    assert raw_capture_warning({'runtime': {}}) is None
    assert raw_capture_warning({'runtime': {'raw_capture': {'complete': True}}}) is None
    assert raw_capture_warning({'runtime': {'raw_capture': dict(complete=False, status='degraded',
                                                                missing_count=650)}}) == (
        'Warning: raw capture incomplete (status degraded, 650 frames missing); see outcome.json')
