"""Real coordinator corrections after terminal history has left active RAM."""
from copy import deepcopy
from dataclasses import replace
import importlib
import sqlite3

import pytest

from conftest import ImmediateFillPaperEngine
from src.market_decisions.order_coordinator import OrderCoordinator
from src.utilities.classes import OrderRecord, OrderStatus, Side
from src.utilities.trading_state import TradingState

from bot_v1_support import valid_config
from test_bot_history_v1 import CommitFailureConnection


@pytest.mark.asyncio
@pytest.mark.parametrize("close_fraction", [0, .5, 1])
async def test_archived_buy_correction_reprices_open_partial_and_exited_lots(
        tmp_path, state, intent, clock, close_fraction):
    history_type = importlib.import_module("src.paper_trading.history").HistoryStore
    limits = valid_config()["runtime"]["limits"]
    limits.update(max_hot_history_records=2, max_archive_cache_records=2)
    store = history_type(tmp_path, limits=limits)
    archived = TradingState(state.config, clock, history_store=store, limits=limits)
    archived.markets = deepcopy(state.markets)
    archived.orderbooks = deepcopy(state.orderbooks)
    coordinator = OrderCoordinator(archived, paper_engine=ImmediateFillPaperEngine())
    try:
        buy = (await coordinator.submit(intent)).records[0]
        sold = 0
        archived.orderbooks["A:m:YES"].bids = {.6: 10000}
        if close_fraction:
            result = await coordinator.submit(replace(
                intent, intent_id="target-exit", side=Side.SELL, reduce_only=True,
                close_fraction=close_fraction))
            sold = result.records[0].size

        # Turn over unrelated actual orders while the target position may remain
        # open. Waiting for all inventory to close would hide unbounded history.
        archived.markets["A:x"] = replace(archived.markets["A:m"], market_id="x")
        archived.orderbooks["A:x:YES"] = replace(
            archived.orderbooks["A:m:YES"], market_id="x", bids={.6: 10000})
        for index in range(8):
            entry = replace(intent, market_id="x", strategy="second", intent_id=f"later-{index}")
            assert (await coordinator.submit(entry)).records
            exit_signal = replace(entry, intent_id=f"later-exit-{index}",
                                  side=Side.SELL, reduce_only=True,
                                  estimated_probability=None, resolution_date=None)
            assert (await coordinator.submit(exit_signal)).records
        await archived.compact_history()
        portfolio = archived.portfolios["PAPER"]
        assert f"A:{buy.order_id}" not in portfolio.order_history
        assert len(portfolio.order_history) <= limits["max_hot_history_records"]
        cash_before = portfolio.balances["A"]["USD"].cash
        realized_before = portfolio.realized_pnl

        correction = replace(buy, strategy="", average_fill_price=.4, fees_paid=.2,
                             cumulative_notional=str(buy.size * .4))
        await coordinator.apply_order_update(correction)
        assert portfolio.balances["A"]["USD"].cash == pytest.approx(cash_before + buy.size * .1 - .2)
        assert portfolio.realized_pnl == pytest.approx(realized_before + sold * .1 - .2)
        if close_fraction == 1:
            assert "A:m:YES" not in portfolio.positions
            assert not portfolio.strategy_positions["paper"]
        else:
            for position in (portfolio.positions["A:m:YES"],
                             portfolio.strategy_positions["paper"]["A:m:YES"]):
                assert position.size == buy.size - sold
                assert position.entry_price == pytest.approx(.4)
        once = (portfolio.balances["A"]["USD"].cash, portfolio.realized_pnl,
                deepcopy(portfolio.positions), deepcopy(portfolio.strategy_positions))
        await archived.compact_history()
        await coordinator.apply_order_update(correction)
        twice = (portfolio.balances["A"]["USD"].cash, portfolio.realized_pnl,
                 portfolio.positions, portfolio.strategy_positions)
        assert twice == once, "Archived cumulative correction was accounted more than once"
        assert (await coordinator.submit(intent)).decision.reason == "duplicate_intent"
    finally:
        store.close()


@pytest.mark.asyncio
async def test_failed_actual_archive_commit_retains_hot_fill_and_duplicate_evidence(tmp_path, state, clock):
    history_type = importlib.import_module("src.paper_trading.history").HistoryStore
    limits = valid_config()["runtime"]["limits"]
    limits.update(max_hot_history_records=1, max_archive_cache_records=1)
    store = history_type(tmp_path, limits=limits,
                         connection_factory=CommitFailureConnection)
    archived = TradingState(state.config, clock, history_store=store, limits=limits)
    coordinator = OrderCoordinator(archived, paper_engine=ImmediateFillPaperEngine())
    first = OrderRecord("old", "m", "A", "YES", Side.BUY, 2, .5,
                        OrderStatus.FILLED, strategy="paper", portfolio_id="PAPER",
                        filled_quantity=2, average_fill_price=.5, fees_paid=0,
                        cumulative_notional="1.0", intent_id="old-intent")
    second = replace(first, order_id="new", intent_id="new-intent")
    try:
        await coordinator.apply_order_update(first)
        CommitFailureConnection.instances[-1].fail_next_commit = True
        with pytest.raises(sqlite3.OperationalError, match="actual commit"):
            await coordinator.apply_order_update(second)
        portfolio = archived.portfolios["PAPER"]
        assert {"A:old", "A:new"} <= set(portfolio.order_history)
        assert portfolio.positions["A:m:YES"].size == 4
        assert portfolio.balances["A"]["USD"].cash == 998
        # Duplicate delivery after the fault may retry archival, never the fill.
        await coordinator.apply_order_update(second)
        assert portfolio.positions["A:m:YES"].size == 4
        assert portfolio.balances["A"]["USD"].cash == 998
        assert len(portfolio.order_history) <= 1
    finally:
        store.close()
