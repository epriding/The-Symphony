"""A failed detail export cannot repeat already committed accounting evidence."""
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
import sqlite3

import pytest

from src.market_decisions.order_coordinator import OrderCoordinator
from src.paper_trading.history import HistoryStore
from src.utilities.classes import OrderRecord, OrderStatus, Side, TradingConfig
from src.utilities.trading_state import TradingState

from bot_v1_support import START, valid_config


class FailFirstDetailFlush:
    def __init__(self, output):
        self.output = output
        self.fail = True

    def __getattr__(self, name):
        return getattr(self.output, name)

    def flush(self):
        if self.fail:
            self.fail = False
            raise OSError("injected actual postcommit execution detail flush failure")
        return self.output.flush()


def committed_fills(directory):
    with sqlite3.connect(directory / "history.sqlite3") as connection:
        return connection.execute("SELECT COUNT(*) FROM records WHERE stream='fills:PAPER'").fetchone()[0]


@pytest.mark.asyncio
async def test_detail_flush_failure_then_archive_retry_cannot_duplicate_fills_or_later_correction(tmp_path):
    limits = valid_config()["runtime"]["limits"]
    limits.update(max_hot_history_records=16, max_archive_cache_records=8)
    store = HistoryStore(tmp_path, limits=limits)
    config = TradingConfig()
    config.strategy_portfolios = {"one": "PAPER"}
    config.paper_starting_balances = {"PAPER": {"A": 10000}}
    state = TradingState(config, lambda: START, history_store=store, limits=limits)
    coordinator = OrderCoordinator(state)
    first = OrderRecord("target", "m", "A", "YES", Side.BUY, 2, .5,
        OrderStatus.FILLED, strategy="one", portfolio_id="PAPER", filled_quantity=2,
        average_fill_price=.5, fees_paid=0, cumulative_notional="1.0",
        intent_id="target-intent", revision=1)
    second = replace(first, order_id="other", size=1, filled_quantity=1,
        cumulative_notional=".5", intent_id="other-intent")
    try:
        await coordinator.apply_order_update(first)
        await state.compact_history()
        await coordinator.apply_order_update(replace(first, average_fill_price=.4,
            cumulative_notional=".8", revision=2))
        await coordinator.apply_order_update(second)
        store._detail_outputs["executions"] = FailFirstDetailFlush(store._detail_outputs["executions"])
        with pytest.raises(OSError, match="actual postcommit execution detail flush"):
            await state.compact_history()
        assert committed_fills(tmp_path) == 2
        account = state.portfolios["PAPER"]
        assert account.positions["A:m:YES"].size == 3
        before_retry = (account.balances["A"]["USD"].cash, account.realized_pnl,
            deepcopy(account.positions), deepcopy(account.strategy_positions))
        await state.compact_history()
        assert committed_fills(tmp_path) == 2, "Retry appended a fill whose accounting transaction already committed"
        assert (account.balances["A"]["USD"].cash, account.realized_pnl,
            account.positions, account.strategy_positions) == before_retry
        final = replace(first, average_fill_price=.3, cumulative_notional=".6", revision=3)
        await coordinator.apply_order_update(final)
        await state.compact_history()
        assert committed_fills(tmp_path) == 2
        for position in (account.positions["A:m:YES"], account.strategy_positions["one"]["A:m:YES"]):
            assert position.size == 3
            assert position.entry_price == pytest.approx(1.1 / 3)
        assert account.balances["A"]["USD"].cash == pytest.approx(9998.9)
        assert account.realized_pnl == 0
        once = (account.balances["A"]["USD"].cash, account.realized_pnl,
            deepcopy(account.positions), deepcopy(account.strategy_positions))
        await coordinator.apply_order_update(final)
        await state.compact_history()
        assert (account.balances["A"]["USD"].cash, account.realized_pnl,
            account.positions, account.strategy_positions) == once
        rows = [json.loads(line) for line in (tmp_path / "executions.jsonl").read_text().splitlines()]
        assert len(rows) == 2 and all(row["kind"] == "correction" for row in rows)
        assert {row["revision"] for row in rows} == {2, 3}
        assert all(row["target_order_id"] == "target" and row["strategy"] == "one" and
            Decimal(row["notional_delta"]) == Decimal("-.2") and Decimal(row["fee_delta"]) == 0
            for row in rows)
    finally:
        await store.aclose()
