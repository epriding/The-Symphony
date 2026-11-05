"""Offline acceptance for bot-v1 mode, settings, and venue recovery contracts.

Only the real agreed runner is used. Its absent API fails at test execution,
so pre-implementation collection remains useful without skips or fake ledgers.
All observation and task-cleanup waits have finite harness bounds.
"""
import asyncio
from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
import json
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import src.bot as bot_module
from src.api.interfaces import polymarket_US
from src.api.interfaces.base_interfaces import RestClient
from src.api.interfaces.polymarket_US import PolyUsClient
from src.api.polymarket.polyus_rest_api import PolymarketAPI
from src.market_decisions.order_coordinator import OrderCoordinator
from src.paper_trading.clock import VirtualClock
from src.paper_trading.paper_trading_engine import PaperTradingEngine
from src.paper_trading.recording import decode, read_capture
from src.utilities import ingest
from src.utilities.classes import Side
from src.utilities.market_events import PublicConnectionEvent, PublicEventEnvelope

from bot_v1_support import (
    ControlledFeed, KALSHI, START, US, VENUES, book_event, eventually,
    market, strategy_config, valid_config,
)


WAIT = 2.0  # Synthetic harness deadline, not an empirical deployment limit.


@pytest.fixture
def bot_api():
    names = ("BotRunner", "load_bot_config")
    missing = [name for name in names if not hasattr(bot_module, name)]
    assert not missing, f"Confirmed bot-v1 API is not implemented: {missing}"
    return SimpleNamespace(**{name: getattr(bot_module, name) for name in names})


@pytest.fixture(autouse=True)
def offline_transport(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline recovery acceptance attempted network transport")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)


async def bounded(awaitable):
    return await asyncio.wait_for(awaitable, WAIT)


async def dispose(task):
    """Bound cancellation itself, including a faulty runner's cleanup path."""
    if not task.done():
        task.cancel()
    _, pending = await asyncio.wait({task}, timeout=WAIT)
    assert not pending, "runner ignored cancellation beyond the harness deadline"
    if not task.cancelled():
        task.exception()  # Retrieve any failure after a preceding assertion failed.


async def finish(runner, task):
    runner.request_stop()
    return await bounded(asyncio.shield(task))


class RecoveryFeed(ControlledFeed):
    """Reuse the existing feed's lifecycle, with explicit normalized provenance."""
    def __init__(self, exchange, *, clock=None, markets=None, missing_rule=None):
        super().__init__(exchange, clock=clock, markets=markets)
        self.sent = 0
        self.missing_rule = missing_rule

    async def get_market(self, identifier):
        self.metadata_reads += 1
        result = market(self.exchange, identifier)
        return replace(result, **{self.missing_rule: None}) if self.missing_rule else result

    def send(self, event, *, epoch=0, coverage="full", received_at=None):
        self.sent += 1
        now = self.clock() if received_at is None else received_at
        source_id = f"{self.exchange}-recovery-{self.sent}"
        self.inputs.put_nowait(PublicEventEnvelope(
            event=event, received_at=now, available_at=now, ingest_index=0,
            source_id=source_id, connection_epoch=epoch, depth_coverage=coverage,
        ))
        return source_id

    def book(self, *, ask=.5, epoch=0, coverage="full", received_at=None):
        return self.send(book_event(self.exchange, asks={ask: 1000},
                                   timestamp=self.clock()),
                         epoch=epoch, coverage=coverage, received_at=received_at)

    def connection(self, connected, *, reason, epoch=0):
        return self.send(PublicConnectionEvent(
            exchange=self.exchange, timestamp=self.clock(), connected=connected,
            reason=reason,
        ), epoch=epoch)

    async def stream(self):
        source = super().stream()
        try:
            while True:
                try:
                    outer = await bounded(anext(source))
                except StopAsyncIteration:
                    return
                # The shared feed supplies receipt order and once-only closure.
                # Preserve the explicitly supplied epoch/coverage/receipt time.
                yield replace(outer.event, ingest_index=outer.ingest_index,
                              received_monotonic_ns=outer.received_monotonic_ns)
        finally:
            await bounded(source.aclose())


async def start_books(feeds):
    await bounded(asyncio.gather(*(feed.started.wait() for feed in feeds)))
    for feed in feeds:
        feed.book()


def writing_config(tmp_path, config):
    path = tmp_path / "configuration.json"
    path.write_text(json.dumps(config))
    return path


@pytest.mark.parametrize("boundary", ["loader", "runner"])
@pytest.mark.parametrize("case", [
    "observation_with_enabled_strategy", "empty_public", "empty_synthetic",
    "only_disabled_public", "empty_synthetic_observation",
])
def test_invalid_modes_rejected_before_resources_or_factories(
        bot_api, tmp_path, boundary, case):
    config = valid_config(source="public")
    if case == "observation_with_enabled_strategy":
        config["runtime"]["observe_only"] = True
    elif case == "only_disabled_public":
        config["strategies"][0]["enabled"] = False
    else:
        config["strategies"] = []
        config["strategy_portfolios"] = {}
        if case == "empty_synthetic_observation":
            config["runtime"]["source"] = "synthetic"
            config["runtime"]["observe_only"] = True
        else:
            config["runtime"]["source"] = case.removeprefix("empty_")
    feeds = [RecoveryFeed(venue) for venue in VENUES]
    factory = Mock(side_effect=AssertionError("invalid mode instantiated a strategy"))
    with pytest.raises(ValueError):
        if boundary == "loader":
            bot_api.load_bot_config(writing_config(tmp_path, config))
        else:
            bot_api.BotRunner(config, output_directory=tmp_path / "invalid",
                              feeds=feeds, strategy_factories={"one": factory})
    factory.assert_not_called()
    assert not any(feed.started.is_set() or feed.metadata_reads for feed in feeds)


@pytest.mark.parametrize("missing", ["market", "venue"])
def test_required_identity_must_have_a_configured_subscription(
        bot_api, tmp_path, missing):
    config = valid_config()
    config["strategies"][0]["required_markets"] = [
        f"{US}:unsubscribed" if missing == "market" else "UNCONFIGURED:m"
    ]
    feeds = [RecoveryFeed(venue) for venue in VENUES]
    factory = Mock(side_effect=AssertionError("missing subscription activated strategy"))
    with pytest.raises(ValueError):
        bot_api.BotRunner(config, output_directory=tmp_path / "invalid", feeds=feeds,
                          strategy_factories={"one": factory})
    assert not any(feed.started.is_set() or feed.metadata_reads for feed in feeds)
    factory.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("rule", ["tick_size", "quantity_step", "min_quantity"])
async def test_missing_required_metadata_rules_never_activate_decisions(
        bot_api, tmp_path, monkeypatch, rule):
    config = valid_config()
    config["runtime"]["startup_timeout_seconds"] = .08
    config["feeds"][US]["market_rules"]["m"].pop(rule)
    feeds = [RecoveryFeed(US, missing_rule=rule), RecoveryFeed(KALSHI)]
    calls = []

    class MustNotRun:
        def __init__(self, context):
            pass
        async def on_snapshot(self, snapshot):
            calls.append(snapshot)
            return None

    submit = AsyncMock(side_effect=AssertionError("entry with missing metadata"))
    monkeypatch.setattr(OrderCoordinator, "submit", submit)
    try:
        runner = bot_api.BotRunner(config, output_directory=tmp_path / "invalid",
                                  feeds=feeds, strategy_factories={"one": MustNotRun})
    except ValueError:
        assert not calls
        submit.assert_not_called()
        assert not any(feed.started.is_set() for feed in feeds)
        return
    for feed in feeds:
        feed.book()
    task = asyncio.create_task(runner.run())
    try:
        await eventually(task.done, timeout=WAIT)
        with pytest.raises(Exception):
            await bounded(asyncio.shield(task))
        assert runner.status["phase"] == "failed" and runner.status["reason"]
        assert not calls
        submit.assert_not_called()
    finally:
        await dispose(task)


@pytest.mark.asyncio
@pytest.mark.parametrize("inactive_live_risk", [False, True])
async def test_public_strategy_decision_is_paper_only_even_with_inactive_live_risk(
        bot_api, tmp_path, monkeypatch, inactive_live_risk):
    config = valid_config(source="public")
    if inactive_live_risk:
        config["risk"]["LIVE"] = deepcopy(config["risk"]["PAPER"])
    private = AsyncMock(side_effect=AssertionError("paper runner called private account API"))
    monkeypatch.setattr(ingest, "run_websocket_ingestion", private)
    monkeypatch.setattr(OrderCoordinator, "reconcile", private)
    for client in (RestClient, PolyUsClient):
        for name in ("place_order", "cancel_order", "get_positions", "get_open_orders",
                     "get_order", "get_balance"):
            monkeypatch.setattr(client, name, private)
    for name in ("create_order", "cancel_order", "cancel_all_orders", "close_position",
                 "get_orders", "get_order", "get_positions", "get_balance"):
        monkeypatch.setattr(PolymarketAPI, name, private)
    create_private = Mock(side_effect=AssertionError("paper runner bootstrapped private clients"))
    monkeypatch.setattr(polymarket_US, "create_poly_us_clients", create_private)
    for name in ("start", "run"):
        monkeypatch.setattr(polymarket_US.PolyUsWebsocketClient, name, private)

    fills = {}
    original = OrderCoordinator.apply_paper_update

    async def observe(coordinator, record, approved):
        await bounded(original(coordinator, record, approved))
        if record.filled_quantity:
            assert record.portfolio_id == approved.portfolio_id == "PAPER"
            assert record.strategy == approved.strategy == "one"
            fills[record.order_id] = max(fills.get(record.order_id, 0), record.filled_quantity)
    monkeypatch.setattr(OrderCoordinator, "apply_paper_update", observe)

    class EnterOnce:
        def __init__(self, context):
            self.context, self.entered = context, False
        async def on_snapshot(self, snapshot):
            if self.entered:
                return None
            self.entered = True
            return [self.context.new_intent(
                market_id="m", exchange=US, outcome="YES", side=Side.BUY,
                price=.5, estimated_probability=1,
                resolution_date=self.context.now() + 3600,
            )]

    feeds = [RecoveryFeed(venue) for venue in VENUES]
    runner = bot_api.BotRunner(config, output_directory=tmp_path / "paper",
                              feeds=feeds, strategy_factories={"one": EnterOnce})
    task = asyncio.create_task(runner.run())
    try:
        await start_books(feeds)
        await eventually(lambda: bool(fills), timeout=WAIT)
        summary = await finish(runner, task)
        assert summary["submitted"] == 1 and sum(fills.values()) > 0
        paper = summary["accounts"]["PAPER"]
        assert paper["open_positions"] == 1
        assert paper["cash"][US] == pytest.approx(1000 - sum(fills.values()) * .5)
        assert paper["cash"][KALSHI] == 1000
        private.assert_not_called()
        create_private.assert_not_called()
        assert all(feed.closed.is_set() and feed.close_count == 1 for feed in feeds)
    finally:
        await dispose(task)


@pytest.fixture
def applied_public(monkeypatch):
    """A pass-through observer, never a replacement for matching or accounts."""
    applied = set()
    original = PaperTradingEngine.on_public

    async def observe(engine, envelope):
        await bounded(original(engine, envelope))
        applied.add(envelope.source_id)
    monkeypatch.setattr(PaperTradingEngine, "on_public", observe)
    return applied


def venue_strategies(calls, affected=US):
    """Healthy snapshots record an input marker as an application barrier."""
    def factory(context):
        class Observe:
            async def on_snapshot(self, snapshot):
                healthy = next(venue for venue in VENUES if venue != affected)
                own_venue = affected if context.strategy_name == "affected" else healthy
                own = snapshot.orderbooks[f"{own_venue}:m:YES"]
                calls[context.strategy_name].append(min(own.asks))
                return None
        return Observe()
    return {"affected": factory, "healthy": factory}


def venue_config(affected=US):
    healthy = next(venue for venue in VENUES if venue != affected)
    return valid_config(source="public", strategies=[
        strategy_config("affected", required_markets=[f"{affected}:m"]),
        strategy_config("healthy", required_markets=[f"{healthy}:m"]),
    ])


async def healthy_probe(feed, calls, marker):
    before = len(calls["healthy"])
    feed.book(ask=marker)
    await eventually(lambda: len(calls["healthy"]) > before
                     and calls["healthy"][-1] == marker, timeout=WAIT)


@pytest.mark.asyncio
@pytest.mark.parametrize("affected", VENUES)
@pytest.mark.parametrize("loss", ["disconnect", "sequence_gap"])
async def test_ordinary_loss_pauses_only_affected_and_needs_verified_depth(
        bot_api, tmp_path, applied_public, loss, affected):
    clock = VirtualClock(START)
    healthy = next(venue for venue in VENUES if venue != affected)
    feeds = [RecoveryFeed(venue, clock=clock) for venue in (affected, healthy)]
    calls = {"affected": [], "healthy": []}
    runner = bot_api.BotRunner(venue_config(affected), output_directory=tmp_path / loss,
        feeds=feeds, strategy_factories=venue_strategies(calls, affected), clock=clock)
    task = asyncio.create_task(runner.run())
    try:
        await start_books(feeds)
        await eventually(lambda: all(calls.values()) and runner.status["phase"] == "ready")
        # A normalizer's sequence gap takes the adapter's ordinary recovery path,
        # represented as a disconnect; it is not an unhandled producer exception.
        reason = "connection_closed" if loss == "disconnect" else "public_feed_recovery:ValueError"
        event_id = feeds[0].connection(False, reason=reason)
        await eventually(lambda: event_id in applied_public
                         and runner.status["phase"] == "degraded"
                         and bool(runner.status["strategies"]["affected"]["reason"]))
        paused = len(calls["affected"])
        await healthy_probe(feeds[1], calls, .51)
        assert len(calls["affected"]) == paused
        assert not task.done(), "ordinary venue loss caused global shutdown"

        event_id = feeds[0].connection(True, reason="awaiting_resnapshot", epoch=1)
        await eventually(lambda: event_id in applied_public)
        await healthy_probe(feeds[1], calls, .52)
        assert len(calls["affected"]) == paused, "connection alone resumed decisions"
        assert runner.status["phase"] == "degraded"

        event_id = feeds[0].book(ask=.53, epoch=1, coverage="partial")
        await eventually(lambda: event_id in applied_public)
        await healthy_probe(feeds[1], calls, .54)
        assert len(calls["affected"]) == paused, "incomplete depth resumed decisions"

        event_id = feeds[0].book(ask=.55, epoch=1)
        await eventually(lambda: event_id in applied_public
                         and len(calls["affected"]) > paused
                         and calls["affected"][-1] == .55
                         and runner.status["phase"] == "ready")
        summary = await finish(runner, task)
        assert summary["submitted"] == 0
        assert all(feed.close_count == 1 for feed in feeds)
    finally:
        await dispose(task)


@pytest.mark.asyncio
@pytest.mark.parametrize("affected", VENUES)
async def test_clock_staleness_pauses_affected_while_fresh_other_venue_continues(
        bot_api, tmp_path, applied_public, affected):
    clock = VirtualClock(START)
    healthy = next(venue for venue in VENUES if venue != affected)
    feeds = [RecoveryFeed(venue, clock=clock) for venue in (affected, healthy)]
    calls = {"affected": [], "healthy": []}
    runner = bot_api.BotRunner(venue_config(affected), output_directory=tmp_path / "stale",
        feeds=feeds, strategy_factories=venue_strategies(calls, affected), clock=clock)
    task = asyncio.create_task(runner.run())
    try:
        await start_books(feeds)
        await eventually(lambda: all(calls.values()) and runner.status["phase"] == "ready")
        # Both books start at the same controlled time. Refresh only healthy
        # depth before advancing beyond the affected book's five-second limit.
        clock.advance(START + 4)
        await healthy_probe(feeds[1], calls, .51)
        clock.advance(START + 6)
        await eventually(lambda: runner.status["phase"] == "degraded"
                         and bool(runner.status["strategies"]["affected"]["reason"]))
        paused = len(calls["affected"])
        await healthy_probe(feeds[1], calls, .52)
        assert len(calls["affected"]) == paused
        assert not task.done()

        event_id = feeds[0].connection(True, reason="awaiting_resnapshot", epoch=1)
        await eventually(lambda: event_id in applied_public)
        await healthy_probe(feeds[1], calls, .53)
        assert len(calls["affected"]) == paused

        # A newly delivered envelope cannot turn an old receipt into fresh data.
        event_id = feeds[0].book(ask=.54, epoch=1, received_at=START)
        await eventually(lambda: event_id in applied_public)
        await healthy_probe(feeds[1], calls, .55)
        assert len(calls["affected"]) == paused, "old depth became fresh on delivery"

        feeds[0].book(ask=.56, epoch=1)
        await eventually(lambda: len(calls["affected"]) > paused
                         and calls["affected"][-1] == .56
                         and runner.status["phase"] == "ready")
        summary = await finish(runner, task)
        assert summary["submitted"] == 0
    finally:
        await dispose(task)


@pytest.mark.asyncio
async def test_startup_without_required_depth_fails_bounded_without_any_entry(
        bot_api, tmp_path, monkeypatch):
    config = venue_config()
    config["runtime"]["startup_timeout_seconds"] = .08
    calls = {"affected": [], "healthy": []}
    feeds = [RecoveryFeed(venue) for venue in VENUES]
    submit = AsyncMock(side_effect=AssertionError("entry before startup readiness"))
    monkeypatch.setattr(OrderCoordinator, "submit", submit)
    runner = bot_api.BotRunner(config, output_directory=tmp_path / "missing-depth",
                              feeds=feeds, strategy_factories=venue_strategies(calls))
    task = asyncio.create_task(runner.run())
    try:
        await bounded(asyncio.gather(*(feed.started.wait() for feed in feeds)))
        feeds[0].book()
        # Kalshi is connected, but no verified initial depth ever arrives.
        feeds[1].connection(True, reason="awaiting_resnapshot")
        await eventually(task.done, timeout=WAIT)
        with pytest.raises(Exception):
            await bounded(asyncio.shield(task))
        assert runner.status["phase"] == "failed" and runner.status["reason"]
        assert not any(calls.values()), "callbacks activated before all subscriptions were ready"
        submit.assert_not_called()
        assert all(feed.closed.is_set() and feed.close_count == 1 for feed in feeds)
    finally:
        await dispose(task)


@pytest.mark.parametrize("settings", [
    pytest.param([], id="array"),
    pytest.param("not-an-object", id="string"),
    pytest.param(None, id="null"),
    pytest.param({"nested": {"not-json"}}, id="nested-set"),
    pytest.param({"nested": float("nan")}, id="nan"),
    pytest.param({"nested": float("inf")}, id="positive-infinity"),
    pytest.param({"nested": float("-inf")}, id="negative-infinity"),
])
def test_invalid_strategy_settings_rejected_before_activation(bot_api, tmp_path, settings):
    config = valid_config()
    # Assign directly: the shared helper intentionally treats None as default {}.
    config["strategies"][0]["settings"] = settings
    feeds = [RecoveryFeed(venue) for venue in VENUES]
    factory = Mock(side_effect=AssertionError("invalid settings reached factory"))
    with pytest.raises(ValueError):
        bot_api.BotRunner(config, output_directory=tmp_path / "invalid-settings",
                          feeds=feeds, strategy_factories={"one": factory})
    factory.assert_not_called()
    assert not any(feed.started.is_set() or feed.metadata_reads for feed in feeds)


def test_encoded_settings_cannot_exceed_existing_event_limit(bot_api, tmp_path):
    config = valid_config()
    # The characters alone exceed the byte limit under either ASCII-escaped or
    # UTF-8 JSON encoding. No serializer-specific whitespace rule is assumed.
    config["strategies"][0]["settings"] = {
        "payload": "x" * (config["runtime"]["limits"]["max_event_bytes"] + 1)
    }
    feeds = [RecoveryFeed(venue) for venue in VENUES]
    factory = Mock(side_effect=AssertionError("oversized settings reached factory"))
    with pytest.raises(ValueError):
        bot_api.BotRunner(config, output_directory=tmp_path / "oversized-settings",
                          feeds=feeds, strategy_factories={"one": factory})
    factory.assert_not_called()
    assert not any(feed.started.is_set() or feed.metadata_reads for feed in feeds)


def contains_mapping(tree, expected):
    """Find original settings without imposing an unagreed capture config key."""
    if isinstance(tree, dict):
        if tree == expected:
            return True
        if set(tree) in ({"pairs"}, {"decimal"}, {"enum", "value"}, {"type", "fields"}):
            return contains_mapping(decode(tree), expected)
        return any(contains_mapping(value, expected) for value in tree.values())
    if isinstance(tree, (list, tuple)):
        return any(contains_mapping(value, expected) for value in tree)
    if is_dataclass(tree) and not isinstance(tree, type):
        return any(contains_mapping(getattr(tree, field.name), expected) for field in fields(tree))
    return False


@pytest.mark.asyncio
async def test_same_factory_receives_distinct_settings_without_mutating_config_or_capture(
        bot_api, tmp_path):
    originals = {
        "alpha": {"nested": {"threshold": .31}, "labels": ["alpha-original"]},
        "beta": {"nested": {"threshold": .72}, "labels": ["beta-original"]},
    }
    config = valid_config(strategies=[
        strategy_config(name, factory="configured", settings=settings)
        for name, settings in originals.items()
    ])
    preserved = deepcopy(config)
    initial, contexts, called = {}, {}, set()

    class Configured:
        def __init__(self, context):
            contexts[context.strategy_name] = context
            initial[context.strategy_name] = deepcopy(context.settings)
            self.context = context
        async def on_snapshot(self, snapshot):
            name = self.context.strategy_name
            if name == "alpha":
                self.context.settings["nested"]["threshold"] = 999
                if "mutated-only-in-context" not in self.context.settings["labels"]:
                    self.context.settings["labels"].append("mutated-only-in-context")
            called.add(name)
            return None

    feeds = [RecoveryFeed(venue) for venue in VENUES]
    output = tmp_path / "settings"
    runner = bot_api.BotRunner(config, output_directory=output, feeds=feeds,
                              strategy_factories={"configured": Configured})
    task = asyncio.create_task(runner.run())
    try:
        await start_books(feeds)
        await eventually(lambda: called == set(originals))
        assert initial == originals
        assert contexts["alpha"].settings is not contexts["beta"].settings
        assert contexts["beta"].settings == originals["beta"]
        assert config == preserved
        await finish(runner, task)
        manifest, records = read_capture(output)
        found = {name for name, value in originals.items() if contains_mapping(manifest, value)}
        for record in records:
            found.update(name for name, value in originals.items() if contains_mapping(record, value))
        assert found == set(originals), "capture omitted original validated strategy settings"
        assert config == preserved
    finally:
        await dispose(task)


@pytest.mark.asyncio
async def test_aggregate_settings_cannot_escape_the_working_memory_budget(bot_api, tmp_path):
    entries = [strategy_config(f"settings-{index}", factory="configured",
                              settings={"payload": str(index) * 200_000})
               for index in range(4)]
    config = valid_config(strategies=entries)
    config["runtime"]["limits"]["memory_budget_bytes"] = 500_000
    # Four distinct ASCII values alone exceed the total budget; each settings
    # object is independently below max_event_bytes. This is only a lower bound.
    assert all(len(json.dumps(entry["settings"]).encode()) <
               config["runtime"]["limits"]["max_event_bytes"] for entry in entries)
    calls = []

    class MustNotRun:
        def __init__(self, context):
            pass
        async def on_snapshot(self, snapshot):
            calls.append(snapshot)
            return None

    feeds = [RecoveryFeed(venue) for venue in VENUES]
    try:
        runner = bot_api.BotRunner(config, output_directory=tmp_path / "settings-budget",
            feeds=feeds, strategy_factories={"configured": MustNotRun})
    except ValueError:
        assert not any(feed.started.is_set() for feed in feeds)
        return
    for feed in feeds:
        feed.book()
    task = asyncio.create_task(runner.run())
    try:
        await eventually(task.done, timeout=WAIT)
        with pytest.raises(Exception):
            await bounded(asyncio.shield(task))
        assert runner.status["phase"] == "failed" and runner.status["reason"]
        assert not calls, "strategy work activated with settings already beyond total budget"
    finally:
        await dispose(task)
