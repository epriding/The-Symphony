"""Durable markout batches retain the first determined observation on retry."""
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
from test_paper_trading import book, submit_size


class MarkCommitFailureConnection(sqlite3.Connection):
    instances = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_next_commit = False
        self.__class__.instances.append(self)

    def commit(self):
        if self.fail_next_commit:
            self.fail_next_commit = False
            raise sqlite3.OperationalError("injected actual markout batch commit failure")
        return super().commit()


async def marked_session(directory, page_size, *, connection_factory=sqlite3.Connection,
        hot_records=1, cache_records=4):
    limits = valid_config()["runtime"]["limits"]
    limits.update(max_hot_history_records=hot_records, max_archive_cache_records=cache_records)
    store = HistoryStore(directory, limits=limits, page_size=page_size,
        connection_factory=connection_factory)
    config = TradingConfig()
    config.strategy_portfolios = {"one": "PAPER"}
    config.paper_starting_balances = {"PAPER": {"A": 10000}}
    config.risk_configs["PAPER"] = RiskConfig(diversification={"other": 1},
        max_position_exposure=1000, max_total_exposure=10000)
    config.paper_execution = PaperExecutionConfig({"A": fixture_venue("A")},
        markout_horizons=(.1, .2))
    session = PaperSession(config, VirtualClock(START), audit_directory=directory,
        history_store=store, limits=limits)
    session.test_index = 0
    await session.add_market(MarketInfo("m", "A", "m", {}, True,
        {"YES": True, "NO": True}, tick_size=.01, quantity_step=1,
        min_quantity=1, fee_per_contract=0, resolution_date=START + 3600))
    await book(session, asks={.5: 100})
    await submit_size(session, 1, price=.5)
    await session.advance(START)
    await session.engine.compact()
    return session, store


def durable_marks(directory):
    with sqlite3.connect(directory / "history.sqlite3") as connection:
        return {
            "pending": connection.execute("SELECT COUNT(*) FROM pending_marks").fetchone()[0],
            "completed": connection.execute("SELECT COUNT(*) FROM records WHERE stream='markouts'").fetchone()[0],
            "identities": connection.execute("SELECT COUNT(*) FROM entries WHERE namespace='markout_ids'").fetchone()[0],
        }


@pytest.mark.asyncio
async def test_partial_markout_batch_preserves_first_quote_then_summary_commits_tail(tmp_path):
    session, store = await marked_session(tmp_path, page_size=3)
    try:
        await book(session, bids={.6: 100}, asks={.62: 100}, at=START + .11)
        await session.reporter.observe_async()
        assert len(session.reporter.markouts) == 1 < store.page_size
        assert durable_marks(tmp_path) == {"pending": 2, "completed": 0, "identities": 0}
        await book(session, bids={.7: 100}, asks={.72: 100}, at=START + .21)
        await session.reporter.observe_async()
        assert len(session.reporter.markouts) == 2 < store.page_size
        assert durable_marks(tmp_path) == {"pending": 2, "completed": 0, "identities": 0}
        summary = await session.reporter.summary_async(session.state)
        assert session.reporter.markouts == {}
        assert durable_marks(tmp_path) == {"pending": 0, "completed": 2, "identities": 2}
        assert summary["markouts"]["records"] == 2 and summary["missing_markouts"] == 0
        await session.finish(tmp_path)
        rows = [json.loads(line) for line in (tmp_path / "markouts.jsonl").read_text().splitlines()]
        by_horizon = {row["horizon"]: row for row in rows}
        assert len(rows) == 2 and set(by_horizon) == {.1, .2}
        assert by_horizon[.1]["observed_at"] == START + .11
        assert Decimal(by_horizon[.1]["dollars"]) == Decimal(".11")
        assert by_horizon[.2]["observed_at"] == START + .21
        assert Decimal(by_horizon[.2]["dollars"]) == Decimal(".21")
    finally:
        if not session.closed:
            await session.finish()
        await store.aclose()


@pytest.mark.asyncio
async def test_actual_failed_markout_batch_commit_keeps_pending_evidence_and_retry_quote(tmp_path):
    session, store = await marked_session(tmp_path, page_size=2,
        connection_factory=MarkCommitFailureConnection)
    try:
        MarkCommitFailureConnection.instances[-1].fail_next_commit = True
        with pytest.raises(sqlite3.OperationalError, match="actual markout batch commit"):
            await book(session, bids={.6: 100}, asks={.62: 100}, at=START + .21)
        assert len(session.reporter.markouts) == store.page_size == 2
        assert durable_marks(tmp_path) == {"pending": 2, "completed": 0, "identities": 0}
        # The retry sees a different market. Already determined observations
        # must retain the first price/time despite the rolled-back transaction.
        await book(session, bids={.7: 100}, asks={.72: 100}, at=START + .31)
        await session.reporter.observe_async()
        await session.reporter.observe_async()
        assert session.reporter.markouts == {}
        assert durable_marks(tmp_path) == {"pending": 0, "completed": 2, "identities": 2}
        summary = await session.finish(tmp_path)
        rows = [json.loads(line) for line in (tmp_path / "markouts.jsonl").read_text().splitlines()]
        assert len(rows) == summary["markouts"]["records"] == 2
        assert {row["horizon"] for row in rows} == {.1, .2}
        assert all(not row["missing"] and row["observed_at"] == START + .21 for row in rows)
        assert all(Decimal(row["dollars"]) == Decimal(".11") for row in rows)
    finally:
        if not session.closed:
            await session.finish()
        await store.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("page_size", [1, 3])
async def test_hot_unarchived_execution_keeps_its_first_eligible_markout_quote(tmp_path, page_size):
    session, store = await marked_session(tmp_path, page_size=page_size, hot_records=16, cache_records=8)
    try:
        assert len(session.engine.executions) == 1, "Fixture execution was already evicted"
        await book(session, bids={.6: 100}, asks={.62: 100}, at=START + .12)
        await session.engine.compact()
        await session.reporter.observe_async()
        await book(session, bids={.7: 100}, asks={.72: 100}, at=START + .22)
        await session.engine.compact()
        await session.reporter.observe_async()
        summary = await session.finish(tmp_path)
        rows = [json.loads(line) for line in (tmp_path / "markouts.jsonl").read_text().splitlines()]
        by_horizon = {row["horizon"]: row for row in rows}
        assert len(rows) == summary["markouts"]["records"] == 2
        assert summary["missing_markouts"] == 0 and all(not row["missing"] for row in rows)
        assert by_horizon[.1]["observed_at"] == START + .12
        assert Decimal(by_horizon[.1]["dollars"]) == Decimal(".11")
        assert by_horizon[.2]["observed_at"] == START + .22
        assert Decimal(by_horizon[.2]["dollars"]) == Decimal(".21")
    finally:
        if not session.closed:
            await session.finish()
        await store.aclose()
