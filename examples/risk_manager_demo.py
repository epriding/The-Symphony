"""Run with: venv/bin/python -m examples.risk_manager_demo

All balances, quotes, markets, and fills are simulated. No credentials or network.
"""
import asyncio
from dataclasses import replace
import time

from src.market_decisions.order_coordinator import OrderCoordinator
from src.strategies.base_strategy import Strategy
from src.utilities.classes import (
    MarketInfo, OrderBook, OrderIntent, OrderIntentBatch, RiskConfig, Side,
    TradingConfig,
)
from src.utilities.trading_state import TradingState


class ExampleStrategy(Strategy):
    async def on_snapshot(self, snapshot):
        return [OrderIntent(
            market_id="demo", exchange="VENUE_A", outcome="YES", side=Side.BUY,
            strategy=self.name, price=.5, estimated_probability=.7,
            resolution_date=snapshot.timestamp + 3600,
            created_at=snapshot.timestamp,
        )]


async def main():
    config = TradingConfig()
    config.strategy_portfolios = {"example": "PAPER"}
    config.paper_starting_balances = {"PAPER": {"VENUE_A": 1000, "VENUE_B": 500}}
    config.risk_configs["PAPER"] = RiskConfig(
        diversification={"other": 1.0}, max_total_exposure=500,
        max_position_exposure=250,
    )
    state = TradingState(config)
    coordinator = OrderCoordinator(state)
    for exchange in ("VENUE_A", "VENUE_B"):
        await state.update_trading_state(MarketInfo(
            "demo", exchange, "demo-market", {}, True, {"YES": True, "NO": True},
            category="other", tick_size=.01, quantity_step=1, min_quantity=1,
            fee_per_contract=.005, resolution_date=time.time() + 3600,
        ))
        for outcome in ("YES", "NO"):
            await state.update_orderbook(OrderBook("demo", exchange, outcome,
                bids={.5: 10000}, asks={.5: 10000}, timestamp=time.time()))
    strategy = ExampleStrategy("example", state, state.risk_managers["PAPER"], coordinator)
    signals = await strategy.on_snapshot(await state.snapshot())
    entry = (await coordinator.submit_many(signals))[0]
    print(f"Entry: {entry.status}, {entry.records[0].size} contracts, limited by {entry.decision.limiting_budget}")
    duplicate = await coordinator.submit(signals[0])
    print(f"Duplicate: {duplicate.decision.reason}")
    batch = OrderIntentBatch(
        legs=[replace(signals[0], intent_id="group-a", estimated_probability=None),
              replace(signals[0], exchange="VENUE_B", intent_id="group-b", estimated_probability=None)],
        quantity_ratios=[1, 2], capital_ceiling=60,
        execution_policy="paper_all_or_none",
    )
    grouped = await coordinator.submit_batch(batch)
    print(f"Group: {grouped.status}, sizes {[record.size for record in grouped.records]}")
    close = replace(signals[0], intent_id="close", side=Side.SELL,
                    reduce_only=True, estimated_probability=None, resolution_date=None)
    exit_result = await coordinator.submit(close)
    print(f"Exit on VENUE_A: {exit_result.status}")
    snapshot = await state.snapshot()
    paper = snapshot.portfolios["PAPER"]
    for exchange, currencies in paper.balances.items():
        print(f"{exchange} paper cash: ${currencies['USD'].cash:.2f}")
    print(f"Paper equity: ${state.risk_managers['PAPER'].equity(snapshot):.2f}")
    print(f"Realized P&L including fees: ${paper.realized_pnl:.2f}")
    print(f"Outstanding orders/reservations: {len(paper.open_orders)}/{len(paper.reservations)}")


if __name__ == "__main__":
    asyncio.run(main())
