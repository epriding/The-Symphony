"""Complete bounded-mode details across groups, missing marks and corrections."""
from collections import Counter
from dataclasses import replace
from decimal import Decimal
import importlib
import json

import pytest

from src.paper_trading.clock import VirtualClock
from src.paper_trading.models import PaperExecutionConfig
from src.paper_trading.session import PaperSession
from src.paper_trading.venue_rules import fixture_venue
from src.utilities.classes import MarketInfo, OrderIntentBatch, RiskConfig, TradingConfig

from bot_v1_support import START, valid_config
from test_paper_trading import book, signal, submit_size


@pytest.mark.asyncio
async def test_detail_streams_include_every_group_timing_mark_and_one_cost_correction(tmp_path):
    limits = valid_config()["runtime"]["limits"]
    limits.update(max_hot_history_records=1, max_archive_cache_records=2)
    store = importlib.import_module("src.paper_trading.history").HistoryStore(tmp_path, limits=limits)
    config = TradingConfig()
    config.strategy_portfolios = {"one": "PAPER", "two": "PAPER"}
    config.paper_starting_balances = {"PAPER": {"A": 10000, "B": 10000}}
    config.risk_configs["PAPER"] = RiskConfig(diversification={"other": 1},
        max_position_exposure=1000, max_total_exposure=20000)
    config.paper_execution = PaperExecutionConfig(
        {venue: fixture_venue(venue) for venue in ("A", "B")}, seed=42,
        markout_horizons=(.1, .5))
    session = PaperSession(config, VirtualClock(START), audit_directory=tmp_path,
                           history_store=store, limits=limits)
    session.test_index = 0
    try:
        for venue in ("A", "B"):
            await session.add_market(MarketInfo("m", venue, "m", {}, True,
                {"YES": True, "NO": True}, tick_size=.01, quantity_step=1,
                min_quantity=1, fee_per_contract=0, resolution_date=START + 3600))
        await book(session, asks={.5: 100})
        await book(session, asks={.5: 1}, exchange="B")
        batch = OrderIntentBatch(
            legs=[signal(session, price=.5, estimated_probability=None),
                  signal(session, exchange="B", price=.5, estimated_probability=None)],
            quantity_ratios=[1, 1], capital_ceiling=10,
            execution_policy="paper_independent_legs", execution_deadline=START + .15)
        submitted = await session.submit_batch(batch)
        assert submitted.status == "pending" and len(submitted.submission_ids) == 2
        await session.advance(START + .2)
        target = session.engine.get_order(submitted.submission_ids[0])
        assert target.filled_quantity == 10
        await session.engine.compact()
        # One future A book determines only A's .1-second horizon. Its .5
        # horizon and the following B fill receive no future observation.
        await book(session, bids={.6: 100}, asks={.61: 100}, at=START + .2)
        await session.engine.compact()
        await session.reporter.observe_async()
        await book(session, asks={.5: 100}, exchange="B", at=START + .2)
        await submit_size(session, 1, exchange="B", price=.5)
        await session.advance(START + .2)
        await session.engine.compact()
        assert f"A:{target.order_id}" not in session.state.portfolios["PAPER"].order_history
        correction = replace(target, average_fill_price=.4, cumulative_notional="4.0",
                             fees_paid=.2, revision=target.revision + 1)
        await session.coordinator.apply_order_update(correction)
        await session.coordinator.apply_order_update(correction)
        summary = await session.finish(tmp_path)

        references = {reference["kind"]: reference for reference in summary["history"]["details"]}
        assert set(references) == {"executions", "markouts", "order_timings", "groups"}
        rows = {}
        for kind, reference in references.items():
            with (tmp_path / reference["path"]).open() as source:
                rows[kind] = [json.loads(line) for line in source]
            assert reference["records"] == len(rows[kind])
        executions = [row for row in rows["executions"] if row["kind"] == "execution"]
        corrections = [row for row in rows["executions"] if row["kind"] == "correction"]
        assert len(executions) == summary["executions"] == 2
        assert len(corrections) == 1
        assert corrections[0]["target_order_id"] == target.order_id
        assert Decimal(corrections[0]["notional_delta"]) == -1
        assert Decimal(corrections[0]["fee_delta"]) == Decimal(".2")
        assert Decimal(str(summary["fees"])) == Decimal(".2")
        assert summary["submitted"] == 3 and summary["full_fills"] == 2
        assert len(rows["order_timings"]) == 3
        assert len({(row["exchange"], row["order_id"]) for row in rows["order_timings"]}) == 3
        groups = rows["groups"]
        assert len(groups) == 1 and groups[0]["batch_id"] == batch.batch_id
        assert groups[0]["failed"] is True
        assert sorted(Decimal(leg["filled_quantity"]) for leg in groups[0]["legs"]) == [0, 10]
        marks = rows["markouts"]
        assert len(marks) == len(executions) * len(config.paper_execution.markout_horizons)
        assert Counter((mark["execution_id"], mark["horizon"]) for mark in marks) == Counter(
            (execution["execution_id"], horizon)
            for execution in executions for horizon in config.paper_execution.markout_horizons)
        assert sum(bool(mark["missing"]) for mark in marks) == summary["missing_markouts"] == 3
        completed = [mark for mark in marks if not mark["missing"]]
        assert len(completed) == 1 and completed[0]["horizon"] == .1
        account = summary["accounts"]["PAPER"]
        assert account["cash"]["A"] == pytest.approx(10000 - 4 - .2)
        assert account["cash"]["B"] == 9999.5
    finally:
        if not session.closed:
            await session.finish()
        store.close()
