"""Independent executable acceptance tests for confirmed bot-v1 O03–O06.

Public runner APIs are resolved during test execution so this suite collects
before implementation. Missing behavior fails normally; it is never skipped.
"""
import asyncio
import sys
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import src.bot as bot_module
from src.api.interfaces.polymarket_US import PolyUsClient
from src.market_decisions.order_coordinator import OrderCoordinator
from src.market_decisions.risk_manager import RiskManager
from src.paper_trading.paper_trading_engine import PaperTradingEngine
from src.utilities.classes import OrderStatus, Side

from bot_v1_support import (
    ControlledFeed, KALSHI, US, VENUES, eventually, stop_and_join,
    strategy_config, valid_config,
)


@pytest.fixture
def bot_api():
    required = ("BotRunner", "load_bot_config", "replay_bot")
    missing = [name for name in required if not hasattr(bot_module, name)]
    assert not missing, f"Confirmed bot-v1 API is not implemented: {missing}"
    return SimpleNamespace(**{name: getattr(bot_module, name) for name in required})


class NoAction:
    def __init__(self, context):
        self.context = context
        self.calls = 0

    async def on_snapshot(self, snapshot):
        self.calls += 1
        return None


async def dispose(task, *, timeout=2.0):
    """Cancel cooperative runner work, with a finite teardown observation."""
    if not task.done():
        task.cancel()
    done, pending = await asyncio.wait({task}, timeout=timeout)
    if pending:
        message = "runner did not finish cooperative cancellation before teardown deadline"
        original_error = sys.exception()
        if original_error is not None:
            original_error.add_note(message)
        else:
            raise AssertionError(message)
    else:
        await asyncio.gather(*done, return_exceptions=True)


async def wait_for_feeds(feeds):
    await asyncio.wait_for(asyncio.gather(*(feed.started.wait() for feed in feeds)), 2)


async def ready_books(feeds):
    await wait_for_feeds(feeds)
    for feed in feeds:
        await feed.push_book()


@pytest.mark.parametrize("change", ["unknown_factory", "duplicate_name", "missing_route", "live_route"])
def test_preflight_rejects_invalid_strategy_before_activating_feeds(bot_api, tmp_path, change):
    config = valid_config()
    if change == "unknown_factory":
        config["strategies"][0]["factory"] = "not_registered"
    elif change == "duplicate_name":
        config["strategies"].append(strategy_config())
    elif change == "missing_route":
        config["strategy_portfolios"].clear()
    else:
        config["strategy_portfolios"]["one"] = "LIVE"
    feeds = [ControlledFeed(venue) for venue in VENUES]
    with pytest.raises(ValueError):
        bot_api.BotRunner(config, output_directory=tmp_path / "invalid",
                          feeds=feeds, strategy_factories={"one": NoAction})
    assert not any(feed.started.is_set() or feed.metadata_reads for feed in feeds)


@pytest.mark.asyncio
async def test_readiness_waits_for_both_venues_and_snapshots_are_independent(bot_api, tmp_path):
    config = valid_config(strategies=[strategy_config("mutator"),
                                    strategy_config("observer"),
                                    strategy_config("disabled", enabled=False)])
    calls, contexts, observer_snapshots = [], [], []

    class Mutator:
        def __init__(self, context):
            contexts.append(context)
        async def on_snapshot(self, snapshot):
            calls.append("mutator")
            snapshot.markets.clear()
            snapshot.portfolios["PAPER"].balances[US]["USD"].cash = -1000

    class Observer:
        def __init__(self, context):
            contexts.append(context)
        async def on_snapshot(self, snapshot):
            calls.append("observer")
            observer_snapshots.append(snapshot)

    def disabled_factory(context):
        raise AssertionError("disabled strategies must not be instantiated")

    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot_api.BotRunner(config, output_directory=tmp_path / "run", feeds=feeds,
        strategy_factories={"mutator": Mutator, "observer": Observer, "disabled": disabled_factory})
    status = runner.status
    assert {"phase", "reason", "strategies", "venues"} <= set(status)
    status["phase"] = "tampered"
    assert runner.status["phase"] != "tampered"
    task = asyncio.create_task(runner.run())
    try:
        await wait_for_feeds(feeds)
        await feeds[0].push_book()
        await eventually(lambda: feeds[0].emitted == 1)
        await asyncio.sleep(.02)
        assert not calls, "callbacks ran before all initial subscriptions had depth"
        assert runner.status["phase"] == "starting"
        await feeds[1].push_book()
        await eventually(lambda: len(observer_snapshots) == 1)
        assert calls[:2] == ["mutator", "observer"]
        snapshot = observer_snapshots[0]
        assert {f"{venue}:m" for venue in VENUES} <= set(snapshot.markets)
        assert snapshot.portfolios["PAPER"].balances[US]["USD"].cash == 1000
        for context in contexts:
            assert context.portfolio_id == "PAPER" and callable(context.now)
            assert hasattr(context.random, "random")
            assert not any(hasattr(context, attr) for attr in ("state", "coordinator", "risk", "submit"))
            intent = context.new_intent(market_id="m", exchange=US, outcome="YES",
                side=Side.BUY, price=.5, estimated_probability=.9,
                resolution_date=context.now() + 3600)
            assert intent.strategy == context.strategy_name
            assert abs(intent.created_at - context.now()) < .1
        summary = await stop_and_join(runner, task)
        assert summary["submitted"] == 0
        assert runner.status["phase"] == "stopped"
    finally:
        await dispose(task)


@pytest.mark.asyncio
async def test_one_callback_and_one_latest_notification_while_risk_timers_continue(bot_api, tmp_path, monkeypatch):
    config = valid_config(strategies=[strategy_config("slow"), strategy_config("healthy")])
    config["runtime"]["strategy_timeout_seconds"] = .8
    release, started = asyncio.Event(), asyncio.Event()
    slow_prices, healthy_calls, risk_checks = [], [], []
    active, peak = 0, 0
    original_risk = RiskManager.stop_loss_intents

    def counted_risk(manager, snapshot):
        risk_checks.append(snapshot.timestamp)
        return original_risk(manager, snapshot)
    monkeypatch.setattr(RiskManager, "stop_loss_intents", counted_risk)

    class Slow:
        def __init__(self, context):
            pass
        async def on_snapshot(self, snapshot):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                slow_prices.append(min(snapshot.orderbooks[f"{US}:m:YES"].asks))
                started.set()
                if len(slow_prices) == 1:
                    await release.wait()
            finally:
                active -= 1

    class Healthy:
        def __init__(self, context):
            pass
        async def on_snapshot(self, snapshot):
            healthy_calls.append(min(snapshot.orderbooks[f"{US}:m:YES"].asks))

    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot_api.BotRunner(config, output_directory=tmp_path / "run", feeds=feeds,
                              strategy_factories={"slow": Slow, "healthy": Healthy})
    task = asyncio.create_task(runner.run())
    try:
        await ready_books(feeds)
        await asyncio.wait_for(started.wait(), 2)
        for price in (.51, .52, .53, .54, .55):
            await feeds[0].push_book(asks={price: 1000})
        await eventually(lambda: .55 in healthy_calls and len(risk_checks) >= 2)
        assert len(healthy_calls) >= 2
        assert slow_prices == [.5] and peak == 1
        release.set()
        await eventually(lambda: len(slow_prices) >= 2)
        after_latest_dispatch = len(risk_checks)
        await eventually(lambda: len(risk_checks) >= after_latest_dispatch + 2)
        assert slow_prices == [.5, .55], "old notifications were queued instead of replaced"
        assert peak == 1
        await stop_and_join(runner, task)
    finally:
        release.set()
        await dispose(task)


@pytest.mark.asyncio
async def test_delayed_execution_and_reports_continue_while_waiting_and_risk_keeps_quotes_stale(
        bot_api, tmp_path, monkeypatch, clock):
    config = valid_config()
    config["runtime"]["strategy_timeout_seconds"] = 10.0
    config["risk"]["PAPER"].update(max_quote_age_seconds=1.5, stop_loss_pct=10.0)
    for rules in config["execution"]["venues"].values():
        rules.update(outbound={"seconds": .05}, acknowledgement={"seconds": 0},
                     execution_report={"seconds": 1.0}, max_book_age=1.5)
    origin = clock()
    submitted, blocked, released = asyncio.Event(), asyncio.Event(), asyncio.Event()
    dispatched = asyncio.Event()
    callback_finished, acknowledged, filled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    submissions, dispatches, reports, risk_checks = [], [], [], []
    original_submit = OrderCoordinator.submit
    original_apply = OrderCoordinator.apply_paper_update
    original_risk = RiskManager.stop_loss_intents
    original_log = PaperTradingEngine.log

    async def observe_submission(coordinator, intent):
        result = await original_submit(coordinator, intent)
        submissions.append((intent, result))
        if intent.side == Side.BUY:
            submitted.set()
        return result

    async def observe_real_report(coordinator, record, approved):
        result = await original_apply(coordinator, record, approved)
        snapshot = await coordinator.state.snapshot()
        reports.append((replace(record), snapshot,
                        blocked.is_set() and not released.is_set() and not callback_finished.is_set()))
        if record.side == Side.BUY:
            if record.filled_quantity:
                filled.set()
            elif record.status == OrderStatus.NEW:
                acknowledged.set()
        return result

    def observe_real_dispatch(engine, event, **values):
        result = original_log(engine, event, **values)
        if event == "dispatched":
            dispatches.append((values["order_id"], engine.clock(), values["outbound_delay"]))
            dispatched.set()
        return result

    def observe_real_risk(manager, snapshot):
        intents = original_risk(manager, snapshot)
        if manager.portfolio_id == "PAPER":
            position = snapshot.portfolios["PAPER"].positions.get(f"{US}:m:YES")
            book = snapshot.orderbooks.get(f"{US}:m:YES")
            risk_checks.append((clock(), position.size if position else 0,
                                manager.quote(snapshot, US, "m", "YES", Side.SELL),
                                book.timestamp if book else None, tuple(intents)))
        return intents

    monkeypatch.setattr(OrderCoordinator, "submit", observe_submission)
    monkeypatch.setattr(OrderCoordinator, "apply_paper_update", observe_real_report)
    monkeypatch.setattr(RiskManager, "stop_loss_intents", observe_real_risk)
    monkeypatch.setattr(PaperTradingEngine, "log", observe_real_dispatch)

    class EnterThenWait:
        def __init__(self, context):
            self.context, self.calls = context, 0

        async def on_snapshot(self, snapshot):
            self.calls += 1
            if self.calls == 1:
                return [self.context.new_intent(market_id="m", exchange=US, outcome="YES",
                    side=Side.BUY, price=.5, estimated_probability=1,
                    resolution_date=self.context.now() + 3600)]
            if max(snapshot.orderbooks[f"{US}:m:YES"].bids) != .48:
                return None
            blocked.set()
            try:
                await released.wait()
            finally:
                callback_finished.set()

    feeds = [ControlledFeed(venue, clock=clock) for venue in VENUES]
    runner = bot_api.BotRunner(config, output_directory=tmp_path / "run", feeds=feeds,
                              clock=clock, strategy_factories={"one": EnterThenWait})
    task = asyncio.create_task(runner.run())
    try:
        await ready_books(feeds)
        await asyncio.wait_for(submitted.wait(), 2)
        assert len(submissions) == 1 and submissions[0][1].decision.approved
        assert submissions[0][1].status == "pending"
        # A fresh public update starts the next callback before any nonzero
        # outbound/report delay can elapse on this manually controlled clock.
        await feeds[0].push_book(bids={.48: 1000})
        await asyncio.wait_for(blocked.wait(), 2)
        assert not acknowledged.is_set() and not filled.is_set()
        quiet_input_counts = tuple(feed.emitted for feed in feeds)

        # Risk and engine workers may progress in either order. Observe the
        # actual dispatch before advancing beyond its outbound delay; a risk
        # tick alone does not establish when arrival became due.
        clock.now = origin + .1
        await eventually(lambda: any(row[0] >= origin + .1 for row in risk_checks))
        await asyncio.wait_for(dispatched.wait(), 2)
        assert len(dispatches) == 1
        order_id, dispatch_time, outbound_delay = dispatches[0]
        assert order_id in submissions[0][1].submission_ids
        clock.now = max(clock(), dispatch_time + outbound_delay + .05)
        await asyncio.wait_for(acknowledged.wait(), 2)
        assert not filled.is_set(), "a future private fill report was delivered early"
        assert not released.is_set() and not callback_finished.is_set()

        # Its delayed private report must also be serviced during this await,
        # before readiness expires. All observations use real account updates.
        acknowledgement_time = next(snapshot.timestamp for record, snapshot, _ in reports
                                    if record.status == OrderStatus.NEW and not record.filled_quantity)
        report_delay = config["execution"]["venues"][US]["execution_report"]["seconds"]
        report_time = max(clock(), acknowledgement_time + report_delay + .05)
        assert report_time < origin + config["risk"]["PAPER"]["max_quote_age_seconds"]
        clock.now = report_time
        await asyncio.wait_for(filled.wait(), 2)
        await eventually(lambda: any(row[1] > 0 and row[2] == .48 for row in risk_checks))
        fill_reports = [row for row in reports if row[0].filled_quantity]
        assert fill_reports and all(during_wait for _, _, during_wait in reports)
        record, snapshot, _ = fill_reports[-1]
        quantity = record.filled_quantity
        assert quantity > 0 and snapshot.timestamp >= report_time
        account = snapshot.portfolios["PAPER"]
        assert account.positions[f"{US}:m:YES"].size == quantity
        assert account.balances[US]["USD"].cash == pytest.approx(1000 - .5 * quantity)
        assert not released.is_set() and not callback_finished.is_set()

        # End the await before expiring readiness: O04 does not prescribe
        # whether an in-flight callback is cancelled when its venue goes stale.
        released.set()
        await asyncio.wait_for(callback_finished.wait(), 2)
        clock.now = origin + 2
        await eventually(lambda: any(row[0] >= origin + 2 and row[1] > 0 for row in risk_checks))
        clock.now = origin + 2.1
        await eventually(lambda: any(row[0] >= origin + 2.1 and row[1] > 0 for row in risk_checks))
        owned_stale_checks = [row for row in risk_checks if row[0] >= origin + 2 and row[1] > 0]
        assert len(owned_stale_checks) >= 2
        assert all(quote is None and (timestamp is None or timestamp == origin) and not intents
                   for _, _, quote, timestamp, intents in owned_stale_checks)
        assert all(intent.side == Side.BUY for intent, _ in submissions)
        assert tuple(feed.emitted for feed in feeds) == quiet_input_counts
        summary = await stop_and_join(runner, task)
        assert summary["submitted"] == summary["full_fills"] == 1
        assert summary["accounts"]["PAPER"]["open_positions"] == 1
        assert summary["accounts"]["PAPER"]["cash"][US] == pytest.approx(1000 - .5 * quantity)
    finally:
        released.set()
        await dispose(task)


@pytest.mark.asyncio
async def test_periodic_stop_loss_reduces_owned_inventory_without_repeated_oversell(bot_api, tmp_path, monkeypatch):
    config = valid_config()
    config["risk"]["PAPER"]["stop_loss_pct"] = 10.0
    bought, sold = asyncio.Event(), asyncio.Event()
    cumulative = {Side.BUY: {}, Side.SELL: {}}
    original_apply = OrderCoordinator.apply_paper_update

    async def observe_real_report(coordinator, record, approved):
        await original_apply(coordinator, record, approved)
        per_order = cumulative[record.side]
        per_order[record.order_id] = max(per_order.get(record.order_id, 0), record.filled_quantity)
        if record.filled_quantity:
            (bought if record.side == Side.BUY else sold).set()
    monkeypatch.setattr(OrderCoordinator, "apply_paper_update", observe_real_report)

    class EnterOnce:
        def __init__(self, context):
            self.context, self.entered = context, False
        async def on_snapshot(self, snapshot):
            if self.entered:
                return None
            self.entered = True
            return [self.context.new_intent(market_id="m", exchange=US, outcome="YES",
                side=Side.BUY, price=.5, estimated_probability=1,
                resolution_date=self.context.now() + 3600)]

    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot_api.BotRunner(config, output_directory=tmp_path / "run", feeds=feeds,
                              strategy_factories={"one": EnterOnce})
    task = asyncio.create_task(runner.run())
    try:
        await ready_books(feeds)
        await asyncio.wait_for(bought.wait(), 2)
        await feeds[0].push_book(bids={.40: 1000})
        await asyncio.wait_for(sold.wait(), 2)
        await asyncio.sleep(.05)  # More than two configured quiet-market risk intervals.
        summary = await stop_and_join(runner, task)
        buy_quantity = sum(cumulative[Side.BUY].values())
        sell_quantity = sum(cumulative[Side.SELL].values())
        assert buy_quantity > 0 and sell_quantity == buy_quantity
        assert summary["accounts"]["PAPER"]["open_positions"] == 0
        assert summary["accounts"]["PAPER"]["cash"][US] == pytest.approx(1000 - buy_quantity * .1)
    finally:
        await dispose(task)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["exception", "timeout", "foreign_result"])
async def test_failed_strategy_is_disabled_without_partial_result_submission(bot_api, tmp_path, failure):
    config = valid_config(strategies=[strategy_config("bad"), strategy_config("healthy")])
    config["runtime"]["strategy_timeout_seconds"] = .04
    healthy_calls = []

    class Bad:
        def __init__(self, context):
            self.context = context
        async def on_snapshot(self, snapshot):
            if failure == "exception":
                raise RuntimeError("synthetic callback failure")
            if failure == "timeout":
                await asyncio.Event().wait()
            intent = self.context.new_intent(market_id="m", exchange=US, outcome="YES",
                side=Side.BUY, price=.5, estimated_probability=1,
                resolution_date=self.context.now() + 3600)
            return [intent, replace(intent, strategy="foreign", intent_id=intent.intent_id + "-foreign")]

    class Healthy:
        def __init__(self, context):
            pass
        async def on_snapshot(self, snapshot):
            healthy_calls.append(snapshot)

    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot_api.BotRunner(config, output_directory=tmp_path / "run", feeds=feeds,
                              strategy_factories={"bad": Bad, "healthy": Healthy})
    task = asyncio.create_task(runner.run())
    try:
        await ready_books(feeds)
        await eventually(lambda: runner.status["phase"] == "degraded")
        assert runner.status["strategies"]["bad"]["reason"]
        before = len(healthy_calls)
        await feeds[0].push_book()
        await eventually(lambda: len(healthy_calls) > before)
        summary = await stop_and_join(runner, task)
        assert summary["submitted"] == 0, "a partially invalid callback result submitted an earlier intent"
    finally:
        await dispose(task)


@pytest.mark.asyncio
async def test_all_strategies_disabled_is_a_failure_not_observation(bot_api, tmp_path):
    class Broken:
        def __init__(self, context):
            pass
        async def on_snapshot(self, snapshot):
            raise RuntimeError("synthetic all-strategy failure")

    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot_api.BotRunner(valid_config(), output_directory=tmp_path / "run",
                              feeds=feeds, strategy_factories={"one": Broken})
    task = asyncio.create_task(runner.run())
    try:
        await ready_books(feeds)
        await eventually(task.done)
        with pytest.raises(Exception):
            await task
        assert runner.status["phase"] == "failed" and runner.status["reason"]
        assert all(feed.closed.is_set() and feed.close_count == 1 for feed in feeds)
    finally:
        await dispose(task)


@pytest.mark.asyncio
async def test_public_feed_failure_stops_runner_and_preserves_private_account_boundary(bot_api, tmp_path, monkeypatch):
    forbidden = AsyncMock(side_effect=AssertionError("v1 called a private account/execution API"))
    monkeypatch.setattr(OrderCoordinator, "reconcile", forbidden)
    for name in ("place_order", "cancel_order", "get_positions", "get_open_orders", "get_balance"):
        monkeypatch.setattr(PolyUsClient, name, forbidden)
    config = valid_config(source="public")
    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot_api.BotRunner(config, output_directory=tmp_path / "run", feeds=feeds,
                              strategy_factories={"one": NoAction})
    task = asyncio.create_task(runner.run())
    try:
        await ready_books(feeds)
        await eventually(lambda: runner.status["phase"] == "ready")
        await feeds[0].push(RuntimeError("synthetic public producer failure"))
        await eventually(task.done)
        with pytest.raises(Exception):
            await task
        assert runner.status["phase"] == "failed" and runner.status["reason"]
        assert all(feed.closed.is_set() for feed in feeds)
        forbidden.assert_not_called()
    finally:
        await dispose(task)


@pytest.mark.asyncio
async def test_repeated_stop_and_synthetic_eof_close_owned_streams_once(bot_api, tmp_path):
    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot_api.BotRunner(valid_config(), output_directory=tmp_path / "run",
                              feeds=feeds, strategy_factories={"one": NoAction})
    task = asyncio.create_task(runner.run())
    try:
        await ready_books(feeds)
        await eventually(lambda: runner.status["phase"] == "ready")
        runner.request_stop()
        runner.request_stop()
        summary = await stop_and_join(runner, task)
        assert runner.status["phase"] == "stopped"
        assert summary["submitted"] == 0
        assert all(feed.closed.is_set() and feed.close_count == 1 for feed in feeds)
    finally:
        await dispose(task)

    eof_feeds = [ControlledFeed(venue) for venue in VENUES]
    for feed in eof_feeds:
        await feed.push_book()
        await feed.end()
    eof_runner = bot_api.BotRunner(valid_config(), output_directory=tmp_path / "eof",
                                  feeds=eof_feeds, strategy_factories={"one": NoAction})
    eof_task = asyncio.create_task(eof_runner.run())
    try:
        result = await asyncio.wait_for(asyncio.shield(eof_task), 2)
        assert eof_runner.status["phase"] == "stopped" and result["submitted"] == 0
        assert all(feed.close_count == 1 for feed in eof_feeds)
    finally:
        await dispose(eof_task)
