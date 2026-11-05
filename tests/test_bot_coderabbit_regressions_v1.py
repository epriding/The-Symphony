"""Independent offline regressions for the four verified CodeRabbit findings."""
import asyncio
from dataclasses import asdict
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

import src.bot as bot
from src.api.interfaces import bounded_public as transport
from src.api.interfaces import polymarket_us_public as us_public
from src.paper_trading.clock import VirtualClock
from src.paper_trading.history import HistoryStore
from src.paper_trading.models import Delay, PaperExecutionConfig
from src.paper_trading.recording import decode
from src.paper_trading.runtime import _create_public_feeds
from src.paper_trading.session import PaperSession
from src.paper_trading.venue_rules import fixture_venue
from src.utilities.classes import MarketInfo, OrderBookSnapshotEvent, OrderStatus, RiskConfig, TimeInForce, TradeEvent, TradingConfig

from bot_v1_support import (
    START, US, VENUES, ControlledFeed, eventually, strategy_config, valid_config,
)
from test_paper_trading import book, portfolio, record, submit_size, trade


class OfflinePublicAPI:
    """Only replace network ownership; real feed buffering/normalization runs."""

    def __init__(self, *args, **kwargs):
        self.client = self
        self.closed = False

    async def close(self):
        self.closed = True


class OfflineWebSocket:
    instances = []

    def __init__(self, *, max_queue, **kwargs):
        self.max_queue = max_queue
        self.handlers = {}
        self.subscriptions = []
        self.closed = False
        self.instances.append(self)

    def on(self, name, callback):
        self.handlers[name] = callback

    async def connect(self):
        pass

    async def subscribe(self, identity, subscription, markets):
        self.subscriptions.append((identity, subscription, tuple(markets)))

    async def close(self):
        self.closed = True

    def emit(self, name, payload):
        self.handlers[name](payload)


async def offline_us_feed(monkeypatch, markets):
    monkeypatch.setenv("CODERABBIT_TEST_PUBLIC_KEY", "test-only-public-key-sentinel")
    monkeypatch.setenv("CODERABBIT_TEST_PUBLIC_SECRET", "test-only-public-secret-sentinel")
    monkeypatch.setattr(transport, "BoundedPolymarketPublicAPI", OfflinePublicAPI)
    monkeypatch.setattr(transport, "BoundedMarketsWebSocket", OfflineWebSocket)
    monkeypatch.setattr(OfflineWebSocket, "instances", [])
    queues = []

    class ObservedQueue(asyncio.Queue):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            queues.append(self)

    # The feed alone sees this queue observer; other asyncio clients are intact.
    local_asyncio = SimpleNamespace(**vars(asyncio))
    local_asyncio.Queue = ObservedQueue
    monkeypatch.setattr(us_public, "asyncio", local_asyncio)
    limits = valid_config()["runtime"]["limits"]
    feeds = await _create_public_feeds({US: {
        "markets": markets, "read_interval": .01,
        "key_id_env": "CODERABBIT_TEST_PUBLIC_KEY",
        "secret_key_env": "CODERABBIT_TEST_PUBLIC_SECRET",
    }}, limits, timeout=1)
    feed = feeds[0]
    # Metadata has already been verified by the caller when the runner streams.
    feed.identities.update({name: name for name in markets})
    return feed, queues, limits


def initial_us_messages(markets):
    for index, name in enumerate(markets):
        yield "market_data", {"marketSlug": name, "marketId": name,
            "bids": [{"px": ".49", "qty": "10"}],
            "offers": [{"px": ".51", "qty": "10"}]}
        yield "trade", {"marketSlug": name, "marketId": name,
            "price": ".5", "quantity": "1", "tradeId": f"startup-{index}",
            "taker": {"side": "ORDER_SIDE_SELL"}}


@pytest.mark.asyncio
@pytest.mark.parametrize("market_count", [1, 3])
async def test_runtime_us_queues_admit_the_full_subscribed_startup_burst(monkeypatch, market_count):
    markets = [f"selected-{index}" for index in range(market_count)]
    feed, queues, limits = await offline_us_feed(monkeypatch, markets)
    stream = feed.stream()
    try:
        connected = await asyncio.wait_for(anext(stream), 1)
        assert connected.event.connected
        websocket = OfflineWebSocket.instances[0]
        assert len(websocket.subscriptions) == 2 * len(markets)
        startup = list(initial_us_messages(markets))
        # The actual generator is suspended on its connection notification.
        # All subscriptions can emit before its consumer asks for another item.
        for kind, payload in startup:
            websocket.emit(kind, payload)
        received = [await asyncio.wait_for(anext(stream), 1) for _ in startup]
        assert [(type(row.event), row.event.market_id) for row in received] == [
            (event_type, name) for name in markets
            for event_type in (OrderBookSnapshotEvent, TradeEvent)]
        assert all(row.connection_epoch == connected.connection_epoch for row in received)
        assert len(OfflineWebSocket.instances) == 1
        assert queues[0].maxsize == websocket.max_queue == feed.queue_limit
        assert len(startup) <= feed.queue_limit <= limits["memory_budget_bytes"] // limits["max_event_bytes"]
    finally:
        await stream.aclose()
        assert feed.api.closed


@pytest.mark.asyncio
async def test_runtime_us_queue_overflow_remains_fatal_beyond_the_finite_capacity(monkeypatch):
    feed, queues, _ = await offline_us_feed(monkeypatch, ["selected"])
    stream = feed.stream()
    try:
        assert (await asyncio.wait_for(anext(stream), 1)).event.connected
        websocket = OfflineWebSocket.instances[0]
        kind, payload = next(initial_us_messages(["selected"]))
        for _ in range(feed.queue_limit + 1):
            websocket.emit(kind, payload)
        with pytest.raises(transport.PublicCapacityError, match="queue capacity"):
            await asyncio.wait_for(anext(stream), 1)
        assert queues[0].maxsize == websocket.max_queue == feed.queue_limit
        assert len(OfflineWebSocket.instances) == 1
        assert websocket.closed
    finally:
        await stream.aclose()
        await feed.aclose()


class FailingSocket:
    def __init__(self, error):
        self.error = error

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise self.error


@pytest.mark.asyncio
@pytest.mark.parametrize("received", [False, True], ids=["sent-only-1009", "received-1009"])
async def test_native_websocket_1009_emits_a_typed_fatal_capacity_error(received):
    error = ConnectionClosedError(
        Close(1009, "fixture oversized frame") if received else None,
        None if received else Close(1009, "fixture oversized frame"),
        None)
    client = transport.BoundedMarketsWebSocket(
        max_event_bytes=128, max_queue=2, key_id="dummy", secret_key="dummy")
    client._ws = FailingSocket(error)
    errors, closes = [], []
    client.on("error", errors.append)
    client.on("close", lambda: closes.append(True))
    await client._message_loop()
    assert len(errors) == 1
    assert isinstance(errors[0], transport.PublicCapacityError)
    assert closes == [True]


@pytest.mark.asyncio
async def test_noncapacity_native_websocket_error_keeps_its_identity():
    original = ConnectionClosedError(None, Close(1011, "fixture disconnect"), None)
    client = transport.BoundedMarketsWebSocket(
        max_event_bytes=128, max_queue=2, key_id="dummy", secret_key="dummy")
    client._ws = FailingSocket(original)
    errors = []
    client.on("error", errors.append)
    await client._message_loop()
    assert errors == [original]
    assert errors[0] is original


@pytest.mark.asyncio
async def test_native_websocket_loop_reraises_task_cancellation():
    original = asyncio.CancelledError("fixture cancellation")
    client = transport.BoundedMarketsWebSocket(
        max_event_bytes=128, max_queue=2, key_id="dummy", secret_key="dummy")
    client._ws = FailingSocket(original)
    errors, closes = [], []
    client.on("error", errors.append)
    client.on("close", lambda: closes.append(True))
    with pytest.raises(asyncio.CancelledError) as caught:
        await client._message_loop()
    assert caught.value is original
    assert not errors and closes == [True]


async def cancel_race_session(store=None, limits=None):
    config = TradingConfig()
    config.strategy_portfolios = {"one": "PAPER", "two": "PAPER"}
    config.paper_starting_balances = {"PAPER": {"A": 10000}}
    config.risk_configs["PAPER"] = RiskConfig(diversification={"other": 1},
        max_position_exposure=1000, max_total_exposure=20000)
    rules = fixture_venue("A", acknowledgement=Delay(0),
        execution_report=Delay(.1, .02), cancel=Delay(1))
    config.paper_execution = PaperExecutionConfig({"A": rules}, seed=42)
    session = PaperSession(config, VirtualClock(START), history_store=store, limits=limits)
    await session.add_market(MarketInfo("m", "A", "m", {}, True,
        {"YES": True, "NO": True}, tick_size=.01, quantity_step=1,
        min_quantity=1, fee_per_contract=0, resolution_date=START + 3600))
    session.test_index = 0
    await book(session, bids={.48: 100}, asks={.51: 100})
    target = await submit_size(session, 1, price=.49, tif=TimeInForce.GTC)
    await session.advance(START)
    assert session.engine.orders[target].phase == "working"
    assert await session.cancel(target, "A")
    await trade(session, 1, price=.49, at=START + .05, trade_id="fills-before-cancel")
    await session.advance(START + .2)
    assert record(session, target).status == OrderStatus.FILLED
    # Older than the two-order hot window, with all original fill reports done.
    for index in range(3):
        await book(session, asks={.5: 100}, at=START + .25 + index * .2)
        other = await submit_size(session, 1, price=.5)
        await session.advance(START + .4 + index * .2)
        assert record(session, other).status == OrderStatus.FILLED
    assert session.clock() < START + 1
    assert any(event.callback.__name__ == "_cancel_arrive" and event.args == (target,)
               for event in session.engine.scheduler.events)
    return session, target


def scheduled_reports(session, order_id):
    return [(event.timestamp_ns, asdict(event.args[0]))
            for event in sorted(session.engine.scheduler.events)
            if event.callback.__name__ == "_deliver" and event.args[0].order_id == order_id]


@pytest.mark.asyncio
async def test_compaction_retains_a_filled_cancel_target_until_its_real_response_is_delivered(tmp_path):
    limits = dict(valid_config()["runtime"]["limits"],
                  max_hot_history_records=2, max_archive_cache_records=2)
    store = HistoryStore(tmp_path / "cancel-retention", limits=limits)
    try:
        session, target = await cancel_race_session(store, limits)
        await session.engine.compact(force=True)
        assert any(event.callback.__name__ == "_cancel_arrive" and event.args == (target,)
                   for event in session.engine.scheduler.events), "The queued cancel still owes a response"
        assert target in session.engine.orders, "A retained cancel callback needs its terminal order"
        before = session.engine.orders[target].record.revision
        await session.advance(START + 1.001)
        assert session.engine.orders[target].record.revision == before + 1
        assert len(scheduled_reports(session, target)) == 1
        await session.engine.compact(force=True)
        assert target in session.engine.orders, "A delayed cancel response still protects its order"
        await session.advance(START + 1.2)
        assert record(session, target).revision == before + 1
        assert not scheduled_reports(session, target)
        # New completed history makes the old target evictable after delivery.
        for index in range(3):
            await book(session, asks={.5: 100}, at=START + 1.3 + index * .2)
            await submit_size(session, 1, price=.5)
            await session.advance(START + 1.45 + index * .2)
        await session.engine.compact(force=True)
        assert target not in session.engine.orders
        archived = await store.aget("engine_orders", target)
        assert archived["revision"] == before + 1
    finally:
        await store.aclose()


@pytest.mark.asyncio
async def test_compaction_preserves_cancel_rng_report_schedule_and_account_causality(tmp_path):
    limits = dict(valid_config()["runtime"]["limits"],
                  max_hot_history_records=2, max_archive_cache_records=2)
    store = HistoryStore(tmp_path / "cancel-parity", limits=limits)
    try:
        bounded, target = await cancel_race_session(store, limits)
        reference, reference_target = await cancel_race_session()
        assert target == reference_target
        assert bounded.engine.rng.getstate() == reference.engine.rng.getstate()
        before_rng = reference.engine.rng.getstate()
        await bounded.engine.compact(force=True)
        await bounded.advance(START + 1.001)
        await reference.advance(START + 1.001)
        assert reference.engine.rng.getstate() != before_rng, "The genuine cancel arrival must sample its report delay"
        assert bounded.engine.rng.getstate() == reference.engine.rng.getstate(), "Compaction skipped the cancel report RNG draw"
        assert scheduled_reports(bounded, target) == scheduled_reports(reference, target)
        assert len(scheduled_reports(bounded, target)) == 1
        await bounded.advance(START + 1.2)
        await reference.advance(START + 1.2)
        actual, expected = portfolio(bounded), portfolio(reference)
        assert record(bounded, target) == record(reference, target)
        assert actual.balances == expected.balances
        assert actual.positions == expected.positions
        assert actual.open_orders == expected.open_orders
        assert actual.reservations == expected.reservations
    finally:
        await store.aclose()


def huge_strategy_error(monkeypatch):
    secret = "TEST_ONLY_BOUNDARY_CREDENTIAL_836491"
    monkeypatch.setenv("CODERABBIT_TEST_API_KEY", secret)
    prefix = "RuntimeError: "
    # The credential straddles a 1024-byte cut in the unredacted diagnostic.
    padding = "é" * ((1024 - len(prefix.encode("utf-8")) - 24) // 2)
    return RuntimeError(padding + secret + "界 " * 10000), secret


def assert_safe_bounded_reason(reason, secret):
    assert secret not in reason and secret[:8] not in reason
    assert "[REDACTED]" in reason, "Redact the entire credential before cutting the reason"
    assert len(reason.encode("utf-8")) <= 1024
    assert any(marker in reason.lower() for marker in ("truncat", "…", "..."))
    assert reason.startswith("RuntimeError:")


@pytest.mark.asyncio
async def test_strategy_disable_retains_one_bounded_redacted_reason_for_status_control_and_log(tmp_path, monkeypatch):
    config = valid_config(strategies=[strategy_config("sick"), strategy_config("healthy")])
    runner = bot.BotRunner(config, output_directory=tmp_path / "unit",
        feeds=[ControlledFeed(venue) for venue in VENUES],
        strategy_factories={"sick": lambda context: None, "healthy": lambda context: None})
    controls, logs = [], []
    runner._control = lambda action, **payload: controls.append((action, payload))
    runner._logger = SimpleNamespace(error=lambda fmt, *args: logs.append(fmt % args))
    runner._cancel_orders = AsyncMock()
    error, secret = huge_strategy_error(monkeypatch)
    slot = next(value for value in runner.slots if value.entry["name"] == "sick")
    await runner._disable(slot, error)
    assert_safe_bounded_reason(slot.reason, secret)
    assert runner.status["strategies"]["sick"] == {"ready": False, "reason": slot.reason}
    assert controls == [("disabled", {"strategy": "sick", "reason": slot.reason})]
    assert logs == ["strategy sick disabled: " + slot.reason]
    assert not next(value for value in runner.slots if value.entry["name"] == "healthy").disabled


@pytest.mark.asyncio
async def test_oversized_strategy_exception_allows_healthy_runner_and_artifact_finalization(tmp_path, monkeypatch):
    error, secret = huge_strategy_error(monkeypatch)
    calls = []

    class Sick:
        def __init__(self, context):
            pass

        async def on_snapshot(self, snapshot):
            raise error

    class Healthy:
        def __init__(self, context):
            pass

        async def on_snapshot(self, snapshot):
            calls.append(snapshot)
            return []

    config = valid_config(strategies=[strategy_config("sick"), strategy_config("healthy")])
    feeds = [ControlledFeed(venue) for venue in VENUES]
    directory = tmp_path / "runner"
    runner = bot.BotRunner(config, output_directory=directory, feeds=feeds,
        strategy_factories={"sick": Sick, "healthy": Healthy})
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: task.done() or all(feed.started.is_set() for feed in feeds))
        if task.done():
            task.result()
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: task.done() or runner.slots[0].disabled)
        if task.done():
            task.result()
        assert_safe_bounded_reason(runner.status["strategies"]["sick"]["reason"], secret)
        previous_calls = len(calls)
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: task.done() or len(calls) > previous_calls)
        if task.done():
            task.result()
        assert runner.status["strategies"]["healthy"]["ready"]
        runner.request_stop()
        summary = await asyncio.wait_for(asyncio.shield(task), 3)
        assert summary["runtime"]["clean"]
        assert runner.status["phase"] == "stopped"
        outcome = json.loads((directory / "outcome.json").read_text())
        reason = outcome["status"]["strategies"]["sick"]["reason"]
        assert_safe_bounded_reason(reason, secret)
        controls = [decode(row["payload"])
                    for row in map(json.loads, (directory / "inputs.jsonl").read_text().splitlines())
                    if row["kind"] == "runtime_control"]
        disabled = [payload for payload in controls if payload["action"] == "disabled"]
        assert len(disabled) == 1 and disabled[0]["reason"] == reason
        log = (directory / "runtime.log").read_text()
        assert secret not in log and secret[:8] not in log
        assert "strategy sick disabled: " + reason in log
        lines = log.splitlines()
        assert all(len(line.encode("utf-8")) <= 4096 for line in lines)
        disabled_lines = [line for line in lines if line.startswith("ERROR strategy sick disabled: ")]
        assert len(disabled_lines) == 1
        assert len(disabled_lines[0].encode("utf-8")) <= 1152
        assert all(feed.close_count == 1 for feed in feeds)
    finally:
        if not task.done():
            runner.request_stop()
            try:
                await asyncio.wait_for(asyncio.shield(task), 3)
            except BaseException:
                if not task.done():
                    task.cancel()
        await asyncio.gather(task, return_exceptions=True)
