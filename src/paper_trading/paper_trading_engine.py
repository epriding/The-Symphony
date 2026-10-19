from dataclasses import dataclass, field
from ..utilities.trading_state import TradingState
from ..utilities.classes import OrderIntent, MarketInfo, OrderRecord, OrderStatus
import uuid

class PaperTradingEngine:
    """Simulates fills, P&L, and positions for testing without live trading."""

    def __init__(self, state: TradingState):
        self.state = state

    async def simulate_fill(self, order: OrderIntent, market: MarketInfo) -> OrderRecord:
        # make paper trading more accurate
        # naive fill: assume full fill at requested price
        # TODO: slippage/fees, partial fills based on real orderbook depth
        fill_price = order.price

        return OrderRecord(
            order_id=str(uuid.uuid4()),
            market_id=order.market_id,
            exchange=order.exchange,
            outcome=order.outcome,
            side=order.side,
            size=order.size,
            price=fill_price,
            status=OrderStatus.FILLED,
            strategy=order.strategy,
            tif=order.tif,
            filled_quantity=order.size,
        )

    async def update_pnl(self) -> None:
        raise NotImplementedError
