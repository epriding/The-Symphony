"""Additional O03/O04 boundaries absent from the frozen acceptance files."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path

import pytest

import src.bot as bot
from src.market_decisions.order_coordinator import OrderCoordinator
from src.utilities.classes import OrderStatus, Side

from bot_v1_support import ControlledFeed, VENUES, US, eventually, strategy_config, valid_config


@pytest.mark.parametrize("key", [
    "max_strategies", "max_markets", "max_active_orders", "max_positions",
    "max_intents_per_callback", "max_hot_history_records",
    "max_archive_cache_records", "max_event_bytes", "memory_budget_bytes",
])
@pytest.mark.parametrize("invalid", [True, 1.5, float("inf")])
def test_capacity_limits_require_finite_integer_values(tmp_path, key, invalid):
    config = valid_config(strategies=[], source="public", observe_only=True)
    config["runtime"]["limits"][key] = invalid
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        bot.load_bot_config(path)
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("defect", [
    "zero_quote_age", "drawdown_above_100", "boolean_cash", "negative_cash", "unknown_cash_venue",
])
def test_invalid_ledger_configuration_rejected_before_any_resource_or_factory(tmp_path, defect):
    config = valid_config()
    if defect == "zero_quote_age":
        config["risk"]["PAPER"]["max_quote_age_seconds"] = 0
    elif defect == "drawdown_above_100":
        config["risk"]["PAPER"]["max_drawdown_pct"] = 101
    elif defect == "unknown_cash_venue":
        config["starting_balances"]["PAPER"]["UNCONFIGURED_VENUE"] = 1
    else:
        config["starting_balances"]["PAPER"][US] = (True if defect == "boolean_cash" else -1)
    path = tmp_path / "invalid-ledger.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        bot.load_bot_config(path)
    calls = []

    def never_construct(context):
        calls.append(context)
        pytest.fail("Invalid ledger configuration reached a strategy factory")

    feeds = [ControlledFeed(venue) for venue in VENUES]
    with pytest.raises(ValueError):
        bot.BotRunner(config, output_directory=tmp_path / "never-created", feeds=feeds,
                      strategy_factories={"one": never_construct})
    assert not calls
    assert not any(feed.started.is_set() or feed.metadata_reads for feed in feeds)
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["foreign_tail", "too_many", "tuple", "non_intent",
    "nan_probability", "boolean_close_fraction", "string_tif", "infinite_expiry"])
async def test_entire_invalid_result_is_rejected_before_first_submission(tmp_path, monkeypatch, defect):
    config = valid_config()
    config["runtime"]["limits"]["max_intents_per_callback"] = 2
    submitted = []
    called = asyncio.Event()
    original = OrderCoordinator.submit

    async def observe(coordinator, intent):
        submitted.append(intent)
        return await original(coordinator, intent)

    monkeypatch.setattr(OrderCoordinator, "submit", observe)

    class InvalidResult:
        def __init__(self, context):
            self.context = context

        async def on_snapshot(self, snapshot):
            called.set()
            first = self.context.new_intent(
                market_id="m", exchange=US, outcome="YES", side=Side.BUY,
                price=.5, estimated_probability=1,
                resolution_date=self.context.now() + 3600)
            if defect == "foreign_tail":
                return [first, replace(first, intent_id="foreign", strategy="other")]
            if defect == "too_many":
                return [first, replace(first, intent_id="two"), replace(first, intent_id="three")]
            if defect == "tuple":
                return (first,)
            changes = {"nan_probability": {"estimated_probability": float("nan")},
                       "boolean_close_fraction": {"close_fraction": True},
                       "string_tif": {"tif": "FOK"},
                       "infinite_expiry": {"expires_at": float("inf")}}
            if defect in changes:
                return [first, replace(first, intent_id="malformed-tail", **changes[defect])]
            return [first, {"intent_id": "not-an-intent"}]

    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot.BotRunner(config, output_directory=tmp_path / defect,
                           feeds=feeds, strategy_factories={"one": InvalidResult})
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: task.done() or all(feed.started.is_set() for feed in feeds))
        if task.done():
            task.result()
        for feed in feeds:
            await feed.push_book()
        await asyncio.wait_for(called.wait(), 2)
        await eventually(task.done, timeout=2)
        with pytest.raises(Exception):
            task.result()
        assert runner.status["phase"] == "failed"
        assert runner.status["strategies"]["one"]["reason"]
        assert not submitted, "A valid prefix escaped before the invalid callback tail was checked"
        assert all(feed.close_count == 1 for feed in feeds)
    finally:
        if not task.done():
            runner.request_stop()
            done, pending = await asyncio.wait({task}, timeout=2)
            if pending:
                task.cancel()
                done, pending = await asyncio.wait({task}, timeout=2)
                assert not pending, "Runner ignored bounded test cleanup"
            await asyncio.gather(*done, return_exceptions=True)
        else:
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_callback_ignoring_cooperative_cancel_fails_owner_after_finite_grace(
        tmp_path, monkeypatch):
    config = valid_config(strategies=[strategy_config("stalled"), strategy_config("healthy")])
    config["runtime"].update(strategy_timeout_seconds=.03, shutdown_grace_seconds=.25)
    entered, cancelled, release, finished = (asyncio.Event() for _ in range(4))
    callbacks, submitted = [], []
    original = OrderCoordinator.submit

    async def observe(coordinator, intent):
        submitted.append(intent)
        return await original(coordinator, intent)

    monkeypatch.setattr(OrderCoordinator, "submit", observe)

    class Stalled:
        def __init__(self, context):
            self.context = context

        async def on_snapshot(self, snapshot):
            callbacks.append(asyncio.current_task())
            entered.set()
            # This coroutine remains cooperative with the event loop but
            # deliberately refuses cancellation until the fixture releases it.
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
            finished.set()
            return [self.context.new_intent(
                market_id="m", exchange=US, outcome="YES", side=Side.BUY,
                price=.5, estimated_probability=1,
                resolution_date=self.context.now() + 3600)]

    class Healthy:
        def __init__(self, context):
            pass

        async def on_snapshot(self, snapshot):
            return None

    feeds = [ControlledFeed(venue) for venue in VENUES]
    output = tmp_path / "cancellation-grace"
    runner = bot.BotRunner(config, output_directory=output, feeds=feeds,
        strategy_factories={"stalled": Stalled, "healthy": Healthy})
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: task.done() or all(feed.started.is_set() for feed in feeds))
        if task.done():
            task.result()
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: task.done() or entered.is_set())
        if task.done():
            task.result()
            pytest.fail("Runner completed before the cancellation fixture callback entered")
        await eventually(lambda: task.done() or cancelled.is_set())
        assert cancelled.is_set(), "The callback was never asked to cancel"
        # Every strategy has not failed: the owner must independently notice
        # that one disabled callback exceeded its finite cancellation allowance.
        await eventually(task.done, timeout=2)
        with pytest.raises(Exception):
            task.result()
        assert runner.status["phase"] == "failed"
        assert runner.status["strategies"]["stalled"]["reason"]
        assert not submitted and not finished.is_set()
        assert all(feed.close_count == 1 for feed in feeds)
        assert json.loads((output / "outcome.json").read_text())["clean"] is False
        assert json.loads((output / "manifest.json").read_text())["finalized"] is False
        # Releasing the old callback after owner failure cannot resurrect it or
        # cause the valid late result to submit an order.
        release.set()
        done, pending = await asyncio.wait(callbacks, timeout=1)
        assert not pending and finished.is_set()
        await asyncio.gather(*done, return_exceptions=True)
        assert not submitted
    finally:
        release.set()
        if callbacks:
            done, pending = await asyncio.wait(callbacks, timeout=1)
            assert not pending, "The fixture failed to release its cooperative callback"
            await asyncio.gather(*done, return_exceptions=True)
        if not task.done():
            runner.request_stop()
            done, pending = await asyncio.wait({task}, timeout=2)
            assert not pending, "Runner exceeded bounded cleanup after callback release"
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_failing_determined_report_drain_preserves_every_untouched_report(monkeypatch):
    from src.paper_trading.models import Delay
    from src.paper_trading.scheduler import Scheduler
    from src.paper_trading.venue_rules import fixture_venue
    from test_paper_trading import START, book, make_session, submit_size

    session = await make_session(rules={"A": fixture_venue(
        "A", acknowledgement=Delay(.1), execution_report=Delay(1))})
    original = session.coordinator.apply_paper_update
    fail = True

    async def unavailable(record, approved):
        nonlocal fail
        if fail:
            fail = False
            raise RuntimeError("dummy-first-determined-report-unavailable")
        return await original(record, approved)

    try:
        await book(session, asks={.5: 100})
        identifiers = [await submit_size(session, 1, price=.5) for _ in range(2)]
        await session.advance(START + .05)
        assert all(session.engine.get_order(identifier).status == OrderStatus.FILLED
                   for identifier in identifiers)
        reports = [event for event in session.engine.scheduler.events
                   if event.priority == Scheduler.REPORT]
        assert len(reports) >= 2
        account = session.state.portfolios["PAPER"]
        assert not account.positions and account.reservations
        monkeypatch.setattr(session.coordinator, "apply_paper_update", unavailable)
        with pytest.raises(RuntimeError, match="first-determined-report-unavailable"):
            await session.finish()
        retained = [event for event in session.engine.scheduler.events
                    if event.priority == Scheduler.REPORT]
        assert sorted(retained) == sorted(reports), "Failed drain lost a determined or untouched report"
        assert len(session.engine.outbox) == 1
        assert account.balances["A"]["USD"].cash == 10000
        assert not account.positions and account.reservations

        # Retrying both retained scheduler delivery and its ambiguous outbox
        # copy must deliver all fills exactly once through the real coordinator.
        summary = await session.finish()
        assert summary["submitted"] == summary["full_fills"] == 2
        assert len(session.engine.executions) == 2
        assert account.balances["A"]["USD"].cash == 9999
        assert account.positions["A:m:YES"].size == 2
        assert not account.reservations and not session.engine.outbox
        assert not any(event.priority == Scheduler.REPORT
                       for event in session.engine.scheduler.events)
    finally:
        monkeypatch.setattr(session.coordinator, "apply_paper_update", original)
        if not session.closed:
            await session.finish()


@pytest.mark.asyncio
async def test_reported_log_cleanup_failure_cannot_leave_a_clean_capture(tmp_path, monkeypatch):
    from src.utilities.logger import BoundedLogHandler

    config = valid_config(strategies=[], source="public", observe_only=True)
    output = tmp_path / "log-cleanup-failure"
    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = bot.BotRunner(config, output_directory=output, feeds=feeds)
    original = BoundedLogHandler.close
    closed = []

    def failed_owned_close(handler, *args, **kwargs):
        result = original(handler, *args, **kwargs)
        if Path(handler._file.name) == output / "runtime.log" and not closed:
            closed.append(handler)
            raise OSError("dummy-owned-log-final-cleanup-failure")
        return result

    monkeypatch.setattr(BoundedLogHandler, "close", failed_owned_close)
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: task.done() or all(feed.started.is_set() for feed in feeds))
        if task.done():
            task.result()
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: task.done() or runner.status["phase"] == "ready")
        if task.done():
            task.result()
        runner.request_stop()
        await eventually(task.done)
        with pytest.raises(OSError, match="owned-log-final-cleanup-failure"):
            task.result()
        assert len(closed) == 1 and not closed[0]._worker.is_alive()
        assert all(feed.close_count == 1 for feed in feeds)
        assert runner.status["phase"] == "failed"
        assert json.loads((output / "outcome.json").read_text())["clean"] is False
        assert json.loads((output / "manifest.json").read_text())["finalized"] is False
        summary = json.loads((output / "summary.json").read_text())
        assert summary["runtime"]["clean"] is False
    finally:
        if not task.done():
            runner.request_stop()
            done, pending = await asyncio.wait({task}, timeout=2)
            assert not pending, "Runner exceeded cleanup after an owned logger failure"
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_second_public_feed_construction_failure_closes_first_unstarted_http_owner(
        tmp_path, monkeypatch):
    import src.api.interfaces.bounded_public as bounded
    import src.api.interfaces.kalshi as kalshi

    config = valid_config(strategies=[], source="public", observe_only=True)
    names = {US: ("BOT_TEST_US_KEY", "BOT_TEST_US_SECRET"),
             "KALSHI": ("BOT_TEST_KALSHI_KEY", "BOT_TEST_KALSHI_PATH")}
    for venue, (key_name, second_name) in names.items():
        config["feeds"][venue]["key_id_env"] = key_name
        config["feeds"][venue]["secret_key_env" if venue == US else "private_key_env"] = second_name
        monkeypatch.setenv(key_name, "dummy-public-fixture-key")
        monkeypatch.setenv(second_name, "dummy-unopened-fixture-value")
    assert list(config["feeds"]) == [US, "KALSHI"]
    opened, attempts = [], []

    class CountingAPI:
        def __init__(self, *args, **kwargs):
            self.client, self.close_count = self, 0
            opened.append(self)

        async def close(self):
            self.close_count += 1

        async def get_market_by_slug(self, **kwargs):
            pytest.fail("Metadata was read before public feed construction completed")

    def fail_second(*args, **kwargs):
        attempts.append("second-feed")
        raise OSError("dummy-second-public-feed-construction-failure")

    monkeypatch.setattr(bounded, "BoundedPolymarketPublicAPI", CountingAPI)
    monkeypatch.setattr(kalshi, "KalshiMarketData", fail_second)
    output = tmp_path / "partial-public-construction"
    runner = bot.BotRunner(config, output_directory=output)
    with pytest.raises(OSError, match="second-public-feed-construction-failure"):
        await asyncio.wait_for(runner.run(), 2)
    assert attempts == ["second-feed"] and len(opened) == 1
    assert opened[0].close_count == 1, "Unstarted first feed retained its owned HTTP transport"
    assert runner.status["phase"] == "failed"
    assert json.loads((output / "outcome.json").read_text())["clean"] is False
    assert json.loads((output / "manifest.json").read_text())["finalized"] is False
