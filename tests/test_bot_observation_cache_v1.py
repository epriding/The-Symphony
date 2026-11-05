"""Successful-observation reuse cannot hide newly available markout work."""
from contextlib import asynccontextmanager
from decimal import Decimal
import json
import sqlite3

import pytest

from src.paper_trading.clock import VirtualClock
from src.paper_trading.history import HistoryStore
from src.paper_trading.models import PaperExecutionConfig
from src.paper_trading.session import PaperSession
from src.paper_trading.venue_rules import fixture_venue
from src.utilities.classes import MarketInfo, RiskConfig, TradingConfig

from bot_v1_support import START, valid_config
from test_bot_report_batches_v1 import MarkCommitFailureConnection, durable_marks
from test_paper_trading import book, submit_size


@asynccontextmanager
async def observation_input(directory, *, page_size=1, horizons=(.1,),
        connection_factory=sqlite3.Connection, park_execution=True):
    limits = valid_config()["runtime"]["limits"]
    limits.update(max_hot_history_records=16, max_archive_cache_records=8)
    store = HistoryStore(directory, limits=limits, page_size=page_size,
        connection_factory=connection_factory)
    config = TradingConfig()
    config.strategy_portfolios = {"one": "PAPER"}
    config.paper_starting_balances = {"PAPER": {"A": 10000}}
    config.risk_configs["PAPER"] = RiskConfig(diversification={"other": 1},
        max_position_exposure=1000, max_total_exposure=10000)
    config.paper_execution = PaperExecutionConfig({"A": fixture_venue("A")},
        markout_horizons=horizons)
    session = PaperSession(config, VirtualClock(START), audit_directory=directory,
        history_store=store, limits=limits)
    session.test_index = 0
    execution = None
    try:
        await session.add_market(MarketInfo("m", "A", "m", {}, True,
            {"YES": True, "NO": True}, tick_size=.01, quantity_step=1,
            min_quantity=1, fee_per_contract=0, resolution_date=START + 3600))
        await book(session, bids={.49: 100}, asks={.5: 100})
        await submit_size(session, 1, price=.5)
        await session.advance(START)
        assert len(session.engine.executions) == 1
        execution = session.engine.executions[0]
        if park_execution:
            # Hold the genuinely matched immutable execution at the reporter's
            # input boundary. This lets the test expose new hot/archive input
            # without changing books, receipt cutoff, time or account truth.
            session.engine.executions.remove(execution)
            await book(session, bids={.6: 100}, asks={.62: 100}, at=START + .12)
            await session.reporter.observe_async()
        yield session, store, execution
    finally:
        if execution is not None and not session.closed:
            if execution not in session.engine.executions and not await store.acontains(
                    "execution_ids", execution.execution_id):
                session.engine.executions.append(execution)
        try:
            if not session.closed:
                await session.finish(directory)
        finally:
            await store.aclose()


def mark_rows(directory):
    return [json.loads(line) for line in (directory / "markouts.jsonl").read_text().splitlines()]


def assert_mark(row, execution, *, horizon=.1, observed_at=START + .12, dollars=".11"):
    assert row["execution_id"] == execution.execution_id
    assert row["order_id"] == execution.order_id
    assert row["horizon"] == horizon and row["missing"] is False
    assert row["observed_at"] == observed_at
    assert Decimal(row["dollars"]) == Decimal(dollars)


async def finish_and_check(session, directory, count):
    summary = await session.finish(directory)
    rows = mark_rows(directory)
    assert summary["executions"] == summary["full_fills"] == 1
    assert summary["markouts"]["records"] == len(rows) == count
    assert summary["missing_markouts"] == 0
    assert summary["accounts"]["PAPER"]["cash"]["A"] == 9999.5
    return rows


@pytest.mark.asyncio
@pytest.mark.parametrize("visibility", ["hot", "archived"])
async def test_unchanged_book_observes_newly_visible_real_execution(tmp_path, visibility):
    async with observation_input(tmp_path) as (session, store, execution):
        assert durable_marks(tmp_path) == {"pending": 0, "completed": 0, "identities": 0}
        session.engine.executions.append(execution)
        if visibility == "archived":
            await session.engine.compact(force=True)
            assert not session.engine.executions
            assert durable_marks(tmp_path) == {"pending": 1, "completed": 0, "identities": 0}
        # Same book, receipt cutoff, quotes and clock as the prior successful
        # observation. The only new input is actual hot or committed fill work.
        await session.reporter.observe_async()
        assert durable_marks(tmp_path) == {"pending": 0, "completed": 1, "identities": 1}
        assert_mark(mark_rows(tmp_path)[0], execution)
        await book(session, bids={.7: 100}, asks={.72: 100}, at=START + .22)
        rows = await finish_and_check(session, tmp_path, 1)
        assert_mark(rows[0], execution)


@pytest.mark.asyncio
async def test_new_receipt_cutoff_with_same_quotes_then_changed_quotes_preserves_first_horizons(tmp_path):
    async with observation_input(tmp_path, horizons=(.1, .2), park_execution=False) as (session, store, execution):
        await session.advance(START + .12)
        assert durable_marks(tmp_path)["completed"] == 0, "Quiet time invented a later public quote"
        # Identical prices now have a genuinely later receipt cutoff.
        await book(session, bids={.49: 100}, asks={.5: 100}, at=START + .12)
        assert durable_marks(tmp_path)["completed"] == 1
        assert_mark(mark_rows(tmp_path)[0], execution, dollars="-.005")
        await session.advance(START + .22)
        assert durable_marks(tmp_path)["completed"] == 1
        await book(session, bids={.6: 100}, asks={.62: 100}, at=START + .22)
        rows = await finish_and_check(session, tmp_path, 2)
        by_horizon = {row["horizon"]: row for row in rows}
        assert_mark(by_horizon[.1], execution, dollars="-.005")
        assert_mark(by_horizon[.2], execution, horizon=.2, observed_at=START + .22)


@pytest.mark.asyncio
async def test_force_flush_commits_partial_page_even_after_identical_successful_observation(tmp_path):
    async with observation_input(tmp_path, page_size=3) as (session, store, execution):
        session.engine.executions.append(execution)
        await session.reporter.observe_async()
        await session.reporter.observe_async()
        assert len(session.reporter.markouts) == 1 < store.page_size
        assert durable_marks(tmp_path)["completed"] == 0
        await session.reporter.observe_async(force=True)
        assert not session.reporter.markouts
        assert durable_marks(tmp_path) == {"pending": 0, "completed": 1, "identities": 1}
        await session.reporter.observe_async()
        rows = await finish_and_check(session, tmp_path, 1)
        assert_mark(rows[0], execution)


@pytest.mark.asyncio
async def test_failed_commit_retries_the_identical_observation_without_a_new_book(tmp_path):
    async with observation_input(tmp_path,
            connection_factory=MarkCommitFailureConnection) as (session, store, execution):
        session.engine.executions.append(execution)
        MarkCommitFailureConnection.instances[-1].fail_next_commit = True
        with pytest.raises(sqlite3.OperationalError, match="actual markout batch commit"):
            await session.reporter.observe_async()
        assert durable_marks(tmp_path) == {"pending": 0, "completed": 0, "identities": 0}
        assert len(session.reporter.markouts) == 1
        await session.reporter.observe_async()
        assert durable_marks(tmp_path) == {"pending": 0, "completed": 1, "identities": 1}
        rows = await finish_and_check(session, tmp_path, 1)
        assert_mark(rows[0], execution)


@pytest.mark.asyncio
async def test_archive_commit_after_observation_worker_before_resume_is_seen_next_time(tmp_path, monkeypatch):
    async with observation_input(tmp_path) as (session, store, execution):
        original_run = store.run
        interleaved = False

        async def archive_before_observer_resumes(operation):
            nonlocal interleaved
            observed_result = await original_run(operation)
            if not interleaved:
                interleaved = True
                # The observation's SQLite callback has completed. Before its
                # awaiting caller resumes, the actual engine commits its real
                # execution plus pending horizon and retires the hot execution.
                session.engine.executions.append(execution)
                await session.engine.compact(force=True)
            return observed_result

        monkeypatch.setattr(store, "run", archive_before_observer_resumes)
        await session.reporter.observe_async(force=True)
        monkeypatch.setattr(store, "run", original_run)
        assert interleaved and not session.engine.executions
        assert durable_marks(tmp_path) == {"pending": 1, "completed": 0, "identities": 0}
        # The visible hot tuple and all book inputs equal the completed
        # observation's inputs. Its later queued archive commit remains unseen.
        await session.reporter.observe_async()
        assert durable_marks(tmp_path) == {"pending": 0, "completed": 1, "identities": 1}
        rows = await finish_and_check(session, tmp_path, 1)
        assert_mark(rows[0], execution)
