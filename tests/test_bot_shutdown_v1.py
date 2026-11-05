"""Independent O05/O06 acceptance checks with genuine paper obligations.

All feeds are offline fixtures, including when ``source='public'`` selects the
ordinary live-paper shutdown policy. Observers delegate to the actual engine
and coordinator; they do not supply executions, accounts or a runner substitute.
"""
import asyncio
from collections import deque
from copy import deepcopy

import pytest

import src.bot as bot_module
from src.market_decisions.order_coordinator import OrderCoordinator
from src.paper_trading.clock import RealClock
from src.paper_trading.paper_trading_engine import PaperTradingEngine
from src.utilities.classes import OrderStatus, Side, TimeInForce, TradeEvent

from bot_v1_support import (
    ControlledFeed, KALSHI, US, VENUES, eventually, strategy_config, valid_config,
)


@pytest.fixture
def runner_type():
    # Resolve the agreed API at execution time: an absent runtime is a normal
    # red test, rather than a collection error, skip or surrogate implementation.
    assert hasattr(bot_module, "BotRunner"), "Confirmed bot-v1 BotRunner is not implemented"
    return bot_module.BotRunner


def shutdown_config(*, strategies=None, grace=.6):
    config = valid_config(source="public", strategies=strategies)
    config["runtime"]["shutdown_grace_seconds"] = grace
    config["risk"]["PAPER"].update(max_position_exposure=.5,
                                   max_total_exposure=10,
                                   stop_loss_pct=100)
    for rules in config["execution"]["venues"].values():
        for name in ("outbound", "acknowledgement", "execution_report", "cancel"):
            rules[name] = {"seconds": .005}
    return config


def buy(context, venue, *, resting=True):
    return context.new_intent(
        market_id="m", exchange=venue, outcome="YES", side=Side.BUY,
        price=.49 if resting else .50, estimated_probability=1,
        resolution_date=context.now() + 3600,
        tif=TimeInForce.GTC if resting else TimeInForce.FOK,
    )


class RealPaperObserver:
    """Observe actual reports/cancel requests, retaining only fixture-size data.

    Capturing the real coordinator permits a final immutable state snapshot
    after run() raises. The public runner has no agreed failure-summary getter;
    neither a new getter nor a particular Reporter implementation is assumed.
    """
    def __init__(self, monkeypatch):
        self.coordinator = None
        self.reports = deque(maxlen=32)
        self.cancel_requests = deque(maxlen=16)
        self.refuse_terminal_reports = False
        self.refused_reports = deque(maxlen=16)
        original_apply = OrderCoordinator.apply_paper_update
        original_cancel = PaperTradingEngine.enqueue_cancel

        async def observe_report(coordinator, record, approved):
            self.coordinator = coordinator
            if self.refuse_terminal_reports and record.status == OrderStatus.CANCELED:
                self.refused_reports.append(deepcopy(record))
                raise RuntimeError("synthetic unavailable paper-report delivery")
            result = await original_apply(coordinator, record, approved)
            self.reports.append(deepcopy(record))
            return result

        def observe_cancel(engine, order_id):
            result = original_cancel(engine, order_id)
            if result:
                self.cancel_requests.append((order_id, engine.clock()))
            return result

        monkeypatch.setattr(OrderCoordinator, "apply_paper_update", observe_report)
        monkeypatch.setattr(PaperTradingEngine, "enqueue_cancel", observe_cancel)

    def report(self, strategy, status):
        return next((record for record in reversed(self.reports)
                     if record.strategy == strategy and record.status == status), None)

    def cancel_time(self, order_id):
        return next((timestamp for identifier, timestamp in self.cancel_requests
                     if identifier == order_id), None)

    async def snapshot(self):
        assert self.coordinator is not None, "No actual paper report reached the coordinator"
        return await asyncio.wait_for(self.coordinator.state.snapshot(), 1)


async def observe(predicate, task, *, timeout=2):
    """A stopped runner cannot leave a positive observation waiting forever."""
    await eventually(lambda: predicate() or task.done(), timeout=timeout)
    if task.done():
        task.result()
        pytest.fail("Runner completed before the required paper observation")
    assert predicate()


async def start_books(feeds):
    await asyncio.wait_for(asyncio.gather(*(feed.started.wait() for feed in feeds)), 2)
    # A BUY at .49 joins an empty price level. A later eligible SELL print
    # therefore has no fictional displayed queue ahead to consume first.
    await asyncio.wait_for(asyncio.gather(*(
        feed.push_book(bids={.48: 1000}, asks={.50: 1000}) for feed in feeds
    )), 2)


async def dispose(task):
    """Finite teardown, including when the runner fails before startup."""
    if not task.done():
        task.cancel()
    done, pending = await asyncio.wait({task}, timeout=1)
    assert not pending, "Runner ignored cooperative cancellation during bounded test cleanup"
    for completed in done:
        if not completed.cancelled():
            completed.exception()  # Retrieve failures after an earlier assertion.


def assert_streams_closed_once(feeds):
    assert all(feed.closed.is_set() and feed.close_count == 1 for feed in feeds)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["exception", "timeout"])
async def test_disabling_strategy_cancels_its_existing_order_while_healthy_work_continues(
        runner_type, tmp_path, monkeypatch, failure):
    """O05/AC07: isolation includes orders submitted before the bad callback."""
    config = shutdown_config(strategies=[strategy_config("bad"), strategy_config("healthy")])
    config["runtime"]["strategy_timeout_seconds"] = .05
    probe = RealPaperObserver(monkeypatch)
    fail_now = asyncio.Event()
    healthy_snapshots = deque(maxlen=8)
    bad_failures = []

    class Bad:
        def __init__(self, context):
            self.context, self.sent = context, False

        async def on_snapshot(self, snapshot):
            if not self.sent:
                self.sent = True
                return [buy(self.context, US)]
            if fail_now.is_set():
                bad_failures.append(failure)
                if failure == "exception":
                    raise RuntimeError("synthetic failure after a working paper order")
                await asyncio.Event().wait()

    class Healthy:
        def __init__(self, context):
            self.context, self.sent = context, False

        async def on_snapshot(self, snapshot):
            healthy_snapshots.append(snapshot)
            if not self.sent:
                self.sent = True
                return [buy(self.context, KALSHI)]

    clock = RealClock()
    feeds = [ControlledFeed(venue, clock=clock) for venue in VENUES]
    runner = runner_type(config, output_directory=tmp_path / failure, feeds=feeds,
                         clock=clock, strategy_factories={"bad": Bad, "healthy": Healthy})
    task = asyncio.create_task(runner.run())
    try:
        await start_books(feeds)
        await observe(lambda: probe.report("bad", OrderStatus.NEW) is not None and
                      probe.report("healthy", OrderStatus.NEW) is not None, task)
        bad_order = probe.report("bad", OrderStatus.NEW)
        healthy_order = probe.report("healthy", OrderStatus.NEW)
        assert bad_order.remaining_quantity > 0 and healthy_order.remaining_quantity > 0

        fail_now.set()
        await asyncio.wait_for(feeds[0].push_book(bids={.48: 1000}), 1)
        await observe(lambda: runner.status["phase"] == "degraded" and
                      probe.report("bad", OrderStatus.CANCELED) is not None, task)
        assert runner.status["strategies"]["bad"]["reason"]
        assert probe.cancel_time(bad_order.order_id) is not None
        assert probe.cancel_time(healthy_order.order_id) is None

        # This book can only appear in a callback after disable/cancel completed.
        await asyncio.wait_for(feeds[1].push_book(bids={.48: 1000}, asks={.51: 1000}), 1)
        await observe(lambda: healthy_snapshots and
                      min(healthy_snapshots[-1].orderbooks[f"{KALSHI}:m:YES"].asks) == .51,
                      task)
        account = healthy_snapshots[-1].portfolios["PAPER"]
        assert not any(order.strategy == "bad" for order in account.pending_orders)
        assert any(order.order_id == healthy_order.order_id
                   for order in account.open_orders.values())
        assert not account.positions and not account.reservations
        assert bad_failures == [failure], "A disabled callback was dispatched again"

        runner.request_stop()
        summary = await asyncio.wait_for(asyncio.shield(task), 2)
        assert runner.status["phase"] == "stopped"
        assert summary["submitted"] == 2 and summary["counts"] == {"CANCELED": 2}
        assert summary["accounts"]["PAPER"]["open_orders"] == 0
        assert summary["accounts"]["PAPER"]["reserved_cash"] == 0
        assert summary["accounts"]["PAPER"]["cash"] == {US: 1000, KALSHI: 1000}
        assert_streams_closed_once(feeds)
    finally:
        await dispose(task)


@pytest.mark.asyncio
async def test_requested_public_stop_keeps_fill_winning_cancel_race_and_held_inventory(
        runner_type, tmp_path, monkeypatch):
    """O06/AC11: grace services a real feed fill without implicit liquidation."""
    config = shutdown_config(grace=1)
    config["execution"]["venues"][US]["cancel"] = {"seconds": .4}
    probe = RealPaperObserver(monkeypatch)
    calls = []

    class EnterOnce:
        def __init__(self, context):
            self.context, self.sent = context, False

        async def on_snapshot(self, snapshot):
            calls.append(snapshot.timestamp)
            if not self.sent:
                self.sent = True
                return [buy(self.context, KALSHI, resting=False), buy(self.context, US)]

    clock = RealClock()
    feeds = [ControlledFeed(venue, clock=clock) for venue in VENUES]
    runner = runner_type(config, output_directory=tmp_path / "race", feeds=feeds,
                         clock=clock, strategy_factories={"one": EnterOnce})
    task = asyncio.create_task(runner.run())
    try:
        await start_books(feeds)
        await observe(lambda: any(record.exchange == KALSHI and record.status == OrderStatus.FILLED
                                  for record in probe.reports) and
                      any(record.exchange == US and record.status == OrderStatus.NEW
                          for record in probe.reports), task)
        working = next(record for record in probe.reports
                       if record.exchange == US and record.status == OrderStatus.NEW)
        held = next(record for record in probe.reports
                    if record.exchange == KALSHI and record.status == OrderStatus.FILLED)
        before = await probe.snapshot()
        assert len(before.portfolios["PAPER"].positions) == 1
        assert working.remaining_quantity > 0 and held.filled_quantity > 0

        runner.request_stop()
        runner.request_stop()
        await observe(lambda: probe.cancel_time(working.order_id) is not None, task)
        assert runner.status["phase"] == "stopping"
        calls_at_cancel = len(calls)
        cancel_time = probe.cancel_time(working.order_id)
        # A legitimate, uniquely identified public print is supplied *after*
        # the actual cancellation request, while its configured delay runs.
        await asyncio.wait_for(feeds[0].push(TradeEvent(
            "m", "YES", clock(), US, .49, working.remaining_quantity,
            Side.SELL, "legitimate-stop-race-print",
        )), 1)
        summary = await asyncio.wait_for(asyncio.shield(task), 3)
        filled = next((record for record in probe.reports
                       if record.order_id == working.order_id and record.status == OrderStatus.FILLED), None)
        assert filled is not None, "The due fill/report was discarded during stop grace"
        assert filled.filled_quantity == working.size
        assert summary["diagnostics"].get("fill_after_cancel_request", 0) == 1
        assert cancel_time is not None and len(calls) == calls_at_cancel
        assert runner.status["phase"] == "stopped"
        assert summary["submitted"] == 2 and summary["full_fills"] == 2
        assert summary["counts"] == {"FILLED": 2}
        assert all(record.side == Side.BUY for record in probe.reports)
        account = summary["accounts"]["PAPER"]
        assert account["open_positions"] == 2
        assert account["open_orders"] == account["pending_reservations"] == 0
        assert account["reserved_cash"] == 0
        assert account["cash"][US] == pytest.approx(1000 - filled.filled_quantity * .49)
        assert account["cash"][KALSHI] == pytest.approx(1000 - held.filled_quantity * .50)
        final = (await probe.snapshot()).portfolios["PAPER"]
        assert final.positions[f"{US}:m:YES"].size == filled.filled_quantity
        assert final.positions[f"{KALSHI}:m:YES"].size == held.filled_quantity
        assert_streams_closed_once(feeds)
    finally:
        await dispose(task)


@pytest.mark.asyncio
@pytest.mark.parametrize("unresolved", ["cancel_delay", "report_delivery"])
async def test_incomplete_public_stop_fails_and_retains_real_obligations(
        runner_type, tmp_path, monkeypatch, unresolved):
    """O06/AC12: finite cleanup cannot relabel a request as confirmation."""
    config = shutdown_config(grace=.08)
    if unresolved == "cancel_delay":
        config["execution"]["venues"][US]["cancel"] = {"seconds": 10}
    probe = RealPaperObserver(monkeypatch)

    class RestOnce:
        def __init__(self, context):
            self.context, self.sent = context, False

        async def on_snapshot(self, snapshot):
            if not self.sent:
                self.sent = True
                return [buy(self.context, US)]

    clock = RealClock()
    feeds = [ControlledFeed(venue, clock=clock) for venue in VENUES]
    runner = runner_type(config, output_directory=tmp_path / unresolved, feeds=feeds,
                         clock=clock, strategy_factories={"one": RestOnce})
    task = asyncio.create_task(runner.run())
    try:
        await start_books(feeds)
        await observe(lambda: probe.report("one", OrderStatus.NEW) is not None, task)
        working = probe.report("one", OrderStatus.NEW)
        assert working.remaining_quantity == working.size > 0
        probe.refuse_terminal_reports = unresolved == "report_delivery"
        runner.request_stop()
        runner.request_stop()
        await eventually(task.done, timeout=2)
        with pytest.raises(Exception):
            task.result()
        assert runner.status["phase"] == "failed" and runner.status["reason"]
        assert probe.cancel_time(working.order_id) is not None

        account = (await probe.snapshot()).portfolios["PAPER"]
        retained = account.open_orders[f"{US}:{working.order_id}"]
        assert retained.status == OrderStatus.NEW
        assert retained.remaining_quantity == working.size
        assert sum(order.commitment for order in account.pending_orders) == pytest.approx(
            working.size * working.price)
        assert account.balances[US]["USD"].cash == 1000
        assert not account.positions
        engine = probe.coordinator.paper_engine
        actual = engine.get_order(working.order_id)
        if unresolved == "cancel_delay":
            assert actual.status == OrderStatus.NEW
            assert not probe.refused_reports
        else:
            assert probe.refused_reports, "The fixture never exercised terminal-report failure"
            assert actual.status == OrderStatus.CANCELED
            assert any(record.order_id == working.order_id and record.status == OrderStatus.CANCELED
                       for record, approved in engine.outbox), "An undelivered report was discarded"
        assert_streams_closed_once(feeds)
    finally:
        await dispose(task)
