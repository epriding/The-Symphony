"""Slow real archive initialization still gives owned feed streams a lifecycle."""
import asyncio
import json
import time

import pytest

from src.bot import BotRunner
from src.market_decisions.order_coordinator import OrderCoordinator
from src.paper_trading.history import HistoryStore

from bot_v1_support import VENUES, valid_config
from test_bot_recovery_v1 import RecoveryFeed


@pytest.mark.asyncio
async def test_archive_startup_delay_exceeding_startup_deadline_closes_started_feeds_once(
        tmp_path, monkeypatch):
    config = valid_config()
    config["runtime"]["startup_timeout_seconds"] = .08
    callbacks, submissions, initialization_seconds = [], [], []
    original_initialize = HistoryStore._initialize

    def delayed_real_initialization(store, connection):
        began = time.monotonic()
        # This runs on the genuine archive worker before real SQLite setup.
        # Startup is still charged this time; the deadline must not restart.
        time.sleep(.12)
        result = original_initialize(store, connection)
        initialization_seconds.append(time.monotonic() - began)
        return result

    class MustRemainInactive:
        def __init__(self, context):
            pass

        async def on_snapshot(self, snapshot):
            callbacks.append(snapshot)
            pytest.fail("A strategy ran without initial verified depth")

    async def must_not_submit(coordinator, intent):
        submissions.append(intent)
        pytest.fail("An entry was submitted without initial verified depth")

    monkeypatch.setattr(HistoryStore, "_initialize", delayed_real_initialization)
    monkeypatch.setattr(OrderCoordinator, "submit", must_not_submit)
    # Preserve the ordinary recovery fixture's awaited stream delegation and
    # actual ControlledFeed initialization/finally closure behavior.
    feeds = [RecoveryFeed(venue) for venue in VENUES]
    output = tmp_path / "delayed-startup"
    runner = BotRunner(config, output_directory=output, feeds=feeds,
        strategy_factories={"one": MustRemainInactive})
    began = time.monotonic()
    task = asyncio.create_task(runner.run())
    try:
        with pytest.raises(RuntimeError, match="startup timed out"):
            await asyncio.wait_for(asyncio.shield(task), 2)
        assert len(initialization_seconds) == 1 and initialization_seconds[0] >= .12
        assert time.monotonic() - began < 2
        assert runner.status["phase"] == "failed"
        assert "startup timed out" in runner.status["reason"]
        assert not callbacks and not submissions
        assert all(feed.started.is_set() for feed in feeds), "Created collectors were canceled before entering their streams"
        assert all(feed.closed.is_set() and feed.close_count == 1 for feed in feeds)
        assert all(producer.done() for producer in runner._tasks)
        assert not runner._store._thread.is_alive()
        assert not runner._logger.handlers
        assert json.loads((output / "outcome.json").read_text())["clean"] is False
        assert json.loads((output / "manifest.json").read_text())["finalized"] is False
    finally:
        if not task.done():
            task.cancel()
            _, pending = await asyncio.wait({task}, timeout=2)
            assert not pending, "Startup failure ignored bounded fixture cleanup"
        await asyncio.gather(task, return_exceptions=True)
