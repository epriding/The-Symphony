from dataclasses import replace
from copy import deepcopy
import pytest

from src.utilities.classes import (
    AccountBalance, MarketInfo, OrderBook, OrderIntent, OrderRecord, OrderStatus,
    RiskConfig, Side, TradingConfig,
)
from src.utilities.trading_state import TradingState


class Clock:
    def __init__(self):
        self.now = 1_800_000_000.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def state(clock):
    config = TradingConfig()
    config.paper_trading = {"paper": True, "second": True, "live": False}
    config.paper_starting_balances = {"PAPER": {"A": 1000.0}}
    config.risk_configs = {
        mode: RiskConfig(diversification={"other": 1.0}, max_position_exposure=1000)
        for mode in ("PAPER", "LIVE")
    }
    state = TradingState(config, clock)
    market = MarketInfo("m", "A", "test", {}, True, {"YES": True, "NO": True},
                        tick_size=.01, quantity_step=1, min_quantity=1,
                        fee_per_contract=0, resolution_date=clock() + 3600)
    state.markets["A:m"] = market
    for outcome in ("YES", "NO"):
        state.orderbooks[f"A:m:{outcome}"] = OrderBook("m", "A", outcome, {.5: 10000}, {.5: 10000}, clock())
    return state


@pytest.fixture
def intent(clock):
    return OrderIntent(market_id="m", exchange="A", side=Side.BUY, outcome="YES",
                       price=.5, strategy="paper", estimated_probability=.8,
                       created_at=clock(), resolution_date=clock() + 3600)


class FakeClient:
    def __init__(self, clock, exchange="A", status=OrderStatus.NEW, filled=0):
        self.clock = clock
        self.exchange = exchange
        self.status = status
        self.filled = filled
        self.placed = []
        self.orders = {}
        self.positions = []
        self.balances = {"USD": AccountBalance(1000, 1000, clock())}
        self.error = None
        self.cancel_status = OrderStatus.CANCELED

    async def place_order(self, order):
        self.placed.append(order)
        if self.error:
            raise self.error
        record = OrderRecord(str(len(self.placed)), order.market_id, self.exchange,
                             order.outcome, order.side, order.size, order.price,
                             self.status, tif=order.tif, filled_quantity=self.filled,
                             average_fill_price=order.price)
        self.orders[record.order_id] = record
        return deepcopy(record)

    async def get_open_orders(self):
        return deepcopy([o for o in self.orders.values() if o.remaining_quantity])

    async def get_order(self, order_id):
        return deepcopy(self.orders.get(order_id))

    async def get_positions(self):
        return deepcopy(self.positions)

    async def get_balance(self):
        return deepcopy(self.balances)

    async def cancel_order(self, order_id, market_id):
        self.orders[order_id] = replace(self.orders[order_id], status=self.cancel_status)
