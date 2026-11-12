"""Public-contract acceptance tests for bounded v1 history and replay.

The workload is a synthetic fixture, not an exchange-throughput or production
memory target. No private archive API, file layout or storage engine is assumed.
The runtime API is intentionally resolved inside tests so this test-first suite
collects while the feature is still absent, and then fails honestly.
"""
import asyncio
from decimal import Decimal
import json
import sys
import tracemalloc

import pytest

import src.bot as bot
from src.paper_trading.clock import VirtualClock
from src.utilities.classes import (
    OrderStatus, Side, TimeInForce, TradeEvent, TERMINAL_ORDER_STATUSES,
)

from bot_v1_support import (
    START, US, KALSHI, VENUES, ControlledFeed, eventually,
    strategy_config, valid_config,
)


class RoundTrips:
    """A bounded fixture strategy driven solely by its agreed snapshot/context."""

    def __init__(self, cycles):
        self.target = cycles
        self.context = None
        self.phase = "round_trips"
        self.cycles = {venue: 0 for venue in VENUES}
        self.leg = {venue: "buy" for venue in VENUES}
        self.quantity_bought = {venue: Decimal(0) for venue in VENUES}
        self.calls = 0
        self.terminal_history_highwater = 0
        self.active_highwater = 0
        self.position_highwater = 0
        self.last_cash = {}
        self.last_open_positions = 0
        self.last_pending = 0
        self.passive_intent_id = None
        self.passive_observed = False
        self.passive_bid_evidence = None
        self.passive_filled = None
        self.passive_status = None
        self.passive_remaining = None
        self.passive_price = None
        self.passive_tif = None
        self.last_us_book_time = None

    def factory(self, context):
        self.context = context
        return self

    @property
    def complete(self):
        return all(count == self.target for count in self.cycles.values())

    def _intent(self, venue, **changes):
        values = dict(market_id="m", exchange=venue, outcome="YES",
                      side=Side.BUY, price=.50, estimated_probability=1.0,
                      resolution_date=self.context.now() + 3600)
        values.update(changes)
        return self.context.new_intent(**values)

    async def on_snapshot(self, snapshot):
        self.calls += 1
        account = snapshot.portfolios[self.context.portfolio_id]
        terminal = sum(order.status in TERMINAL_ORDER_STATUSES
                       for order in account.order_history.values())
        self.terminal_history_highwater = max(self.terminal_history_highwater,
                                              terminal)
        self.active_highwater = max(self.active_highwater,
                                    len(account.open_orders) + len(account.reservations))
        self.position_highwater = max(self.position_highwater, len(account.positions))
        self.last_cash = {venue: Decimal(str(balances["USD"].cash))
                          for venue, balances in account.balances.items()}
        self.last_open_positions = len(account.positions)
        self.last_pending = len(account.open_orders) + len(account.reservations)
        book = snapshot.orderbooks.get(f"{US}:m:YES")
        if book is not None:
            self.last_us_book_time = book.timestamp

        if self.phase == "passive":
            if self.passive_intent_id is None:
                intent = self._intent(US, price=.49, tif=TimeInForce.GTC)
                self.passive_intent_id = intent.intent_id
                return [intent]
            for order in account.open_orders.values():
                if order.intent_id == self.passive_intent_id:
                    self.passive_observed = True
                    self.passive_filled = order.filled_quantity or 0
                    self.passive_status = order.status
                    self.passive_remaining = order.remaining_quantity
                    self.passive_price = order.price
                    self.passive_tif = order.tif
                    self.passive_bid_evidence = dict(book.bids) if book else None
            return []

        results = []
        for venue in VENUES:
            if self.cycles[venue] >= self.target:
                continue
            position = account.positions.get(f"{venue}:m:YES")
            pending = any(item.exchange == venue for item in account.pending_orders)
            if self.leg[venue] == "await_buy" and position is not None and not pending:
                self.quantity_bought[venue] += Decimal(str(position.size))
                self.leg[venue] = "sell"
            if self.leg[venue] == "await_sell" and position is None and not pending:
                self.cycles[venue] += 1
                self.leg[venue] = "buy"
            if self.cycles[venue] >= self.target:
                continue
            if self.leg[venue] == "buy" and position is None and not pending:
                results.append(self._intent(venue))
                self.leg[venue] = "await_buy"
            elif self.leg[venue] == "sell" and position is not None and not pending:
                results.append(self._intent(venue, side=Side.SELL, price=.48,
                                            estimated_probability=None,
                                            resolution_date=None, reduce_only=True))
                self.leg[venue] = "await_sell"
        return results


def retention_config():
    config = valid_config(strategies=[strategy_config("retention")])
    # Allocation tracing and durable SQLite work are part of this scenario;
    # retention/accounting, rather than subsecond host I/O, is the invariant.
    config['runtime']['startup_timeout_seconds'] = 2
    config['runtime']['shutdown_grace_seconds'] = 2
    config['runtime']['strategy_timeout_seconds'] = 2
    config["risk"]["PAPER"].update(max_position_exposure=.50,
                                   max_total_exposure=10,
                                   max_quote_age_seconds=5,
                                   stop_loss_pct=100)
    for rules in config["execution"]["venues"].values():
        for name in ("outbound", "acknowledgement", "execution_report", "cancel"):
            rules[name] = {"seconds": 0}
    config["runtime"]["limits"].update(
        max_hot_history_records=4, max_archive_cache_records=4,
        max_active_orders=8, max_positions=4,
        memory_budget_bytes=64 * 1024 * 1024,
    )
    return config


async def pump(clock, feeds, scenario, task, *, print_id=None):
    """One bounded input wave, waiting for a real strategy observation."""
    before = scenario.calls
    clock.advance(clock() + .05)
    if print_id is not None:
        await feeds[US].push(TradeEvent("m", "YES", clock(), US,
                                       .49, 1, Side.SELL, print_id))
    for venue in VENUES:
        await feeds[venue].push_book(bids={.48: 1000}, asks={.50: 1000})
    await eventually(lambda: scenario.calls > before or task.done(), timeout=2)
    if task.done():
        task.result()
        pytest.fail("Synthetic stream ended before the fixture workload completed")


async def run_workload(directory, *, cycles=24, duplicate_probe=False):
    runner_type = bot.BotRunner
    config = retention_config()
    clock = VirtualClock(START)
    feeds = {venue: ControlledFeed(venue, clock=clock) for venue in VENUES}
    scenario = RoundTrips(cycles)
    runner = runner_type(config, output_directory=directory,
                         feeds=list(feeds.values()), clock=clock,
                         strategy_factories={"retention": scenario.factory})
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: all(feed.started.is_set() for feed in feeds.values()), task=task)
        await pump(clock, feeds, scenario, task)
        # Record a reliable old print while there is no passive order. Later
        # unique prints and completed orders exceed the tiny hot-history window.
        await pump(clock, feeds, scenario, task, print_id="old-public-print")
        for step in range(cycles * 16 + 32):
            if scenario.complete and not scenario.last_open_positions and not scenario.last_pending:
                break
            await pump(clock, feeds, scenario, task,
                       print_id=f"later-public-print-{step}")
        else:
            pytest.fail("Round-trip workload did not complete within its bounded fixture inputs")

        if duplicate_probe:
            scenario.phase = "passive"
            for _ in range(16):
                await pump(clock, feeds, scenario, task)
                if scenario.passive_observed:
                    break
            assert scenario.passive_observed, "The probe order must actually be working"
            assert scenario.passive_status == OrderStatus.NEW
            assert scenario.passive_filled == 0
            assert scenario.passive_remaining > 0
            assert scenario.passive_price == .49
            assert scenario.passive_tif == TimeInForce.GTC
            assert scenario.passive_bid_evidence is not None
            assert .49 not in scenario.passive_bid_evidence
            assert max(scenario.passive_bid_evidence) < .49
            # At .49 this BUY joined an empty price level, so this eligible
            # one-contract SELL would fill it if the old print were reused.
            await pump(clock, feeds, scenario, task, print_id="old-public-print")
            for _ in range(4):
                await pump(clock, feeds, scenario, task)

        for feed in feeds.values():
            await feed.end()
        summary = await asyncio.wait_for(asyncio.shield(task), 5)
        return config, scenario, summary
    finally:
        if not task.done():
            original_error = sys.exception()
            runner.request_stop()
            try:
                await asyncio.wait_for(asyncio.shield(task), 2)
            except BaseException as cleanup_error:
                if original_error is None:
                    raise
                original_error.add_note(f"workload stop cleanup raised {type(cleanup_error).__name__}")
            finally:
                if not task.done():
                    task.cancel()
                    done, pending = await asyncio.wait({task}, timeout=2)
                    if pending:
                        message = "runner did not finish cooperative cancellation before workload cleanup deadline"
                        active_error = original_error or sys.exception()
                        if active_error is not None:
                            active_error.add_note(message)
                        else:
                            raise AssertionError(message)
                    else:
                        await asyncio.gather(*done, return_exceptions=True)


def execution_totals(directory):
    """Read the existing public execution artifact in constant fixture space."""
    count = 0
    notional = {venue: Decimal(0) for venue in VENUES}
    with (directory / "executions.jsonl").open() as source:
        for line in source:
            execution = json.loads(line)
            count += 1
            direction = -1 if execution["side"] == "BUY" else 1
            notional[execution["exchange"]] += direction * Decimal(execution["notional"])
    return count, notional


@pytest.mark.asyncio
async def test_archived_round_trips_bound_visible_history_and_preserve_all_totals(tmp_path):
    """AC13: tiny hot limits cannot erase session totals or retain every order."""
    config, scenario, summary = await run_workload(tmp_path / "capture")
    expected_orders = 2 * len(VENUES) * scenario.target
    limits = config["runtime"]["limits"]
    # Allow at most active-order/cleanup headroom in a concurrently observed
    # snapshot; do not prescribe an archive batch size or private store design.
    assert scenario.terminal_history_highwater <= (
        limits["max_hot_history_records"] + limits["max_active_orders"]
    )
    assert expected_orders > 4 * limits["max_hot_history_records"]
    assert scenario.active_highwater <= limits["max_active_orders"]
    assert scenario.position_highwater <= limits["max_positions"]
    assert summary["submitted"] == expected_orders
    assert summary["full_fills"] == expected_orders
    account = summary["accounts"]["PAPER"]
    assert account["open_positions"] == account["open_orders"] == 0
    assert account["pending_reservations"] == 0
    assert Decimal(account["fees"]) == 0
    for venue in VENUES:
        expected_cash = Decimal(1000) - Decimal(".02") * scenario.quantity_bought[venue]
        assert Decimal(str(account["cash"][venue])) == expected_cash
    count, cash_changes = execution_totals(tmp_path / "capture")
    assert count == expected_orders
    for venue in VENUES:
        assert Decimal(str(account["cash"][venue])) == Decimal(1000) + cash_changes[venue]


@pytest.mark.asyncio
async def test_old_public_print_cannot_fill_new_empty_ahead_order_after_history_window(tmp_path):
    """AC14: an old trade ID stays consumed across completed-history turnover."""
    _, scenario, summary = await run_workload(tmp_path / "capture", cycles=12,
                                             duplicate_probe=True)
    assert scenario.passive_observed and scenario.passive_filled == 0
    assert summary["submitted"] == 2 * len(VENUES) * scenario.target + 1
    assert summary["full_fills"] == 2 * len(VENUES) * scenario.target
    assert summary["accounts"]["PAPER"]["open_positions"] == 0
    assert summary["accounts"]["PAPER"]["open_orders"] == 1
    count, _ = execution_totals(tmp_path / "capture")
    assert count == 2 * len(VENUES) * scenario.target


@pytest.mark.asyncio
async def test_archived_capture_replay_preserves_totals_with_a_finite_fixture_allocation_guard(
        tmp_path, delayed_history_startup):
    """AC13/AC14: replay/report totals span archived work without a new strategy."""
    replay = bot.replay_bot
    was_tracing = tracemalloc.is_tracing()
    if not was_tracing:
        tracemalloc.start()
    before, _ = tracemalloc.get_traced_memory()
    tracemalloc.reset_peak()
    try:
        config, _, original = await run_workload(tmp_path / "capture", cycles=32)
        reproduced = await replay(tmp_path / "capture", tmp_path / "replay")
        _, peak = tracemalloc.get_traced_memory()
        assert peak - before <= config["runtime"]["limits"]["memory_budget_bytes"]
        for key in ("submitted", "full_fills", "partial_fills", "zero_fills", "fees", "accounts"):
            assert reproduced[key] == original[key]
        original_count, original_cash = execution_totals(tmp_path / "capture")
        replay_count, replay_cash = execution_totals(tmp_path / "replay")
        assert replay_count == original_count and replay_cash == original_cash
    finally:
        if not was_tracing:
            tracemalloc.stop()
