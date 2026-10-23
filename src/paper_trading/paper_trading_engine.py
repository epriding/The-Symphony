"""Immediate fills against a supplied executable quote; no depth simulation."""
from uuid import uuid4
from ..utilities.classes import (
    ApprovedOrder, MarketInfo, OrderRecord, OrderStatus, Side, TimeInForce,
)


class PaperTradingEngine:
    async def simulate_fill(self, order: ApprovedOrder, market: MarketInfo, quote: float) -> OrderRecord:
        if quote is None or (
            (order.side == Side.BUY and quote > order.price)
            or (order.side == Side.SELL and quote < order.price)
        ):
            raise ValueError("No executable quote within approval bound")
        if order.tif not in (TimeInForce.FOK, TimeInForce.IOC):
            raise ValueError("Paper resting orders are not implemented; use FOK or IOC")
        return OrderRecord(
            order_id=str(uuid4()), market_id=order.market_id, exchange=order.exchange,
            outcome=order.outcome, side=order.side, size=order.size, price=order.price,
            status=OrderStatus.FILLED, strategy=order.strategy, tif=order.tif,
            filled_quantity=order.size, portfolio_id=order.portfolio_id,
            category=order.category, intent_id=order.intent_id, batch_id=order.batch_id,
            fee_per_contract=order.fee_per_contract, average_fill_price=quote,
            fees_paid=order.size * order.fee_per_contract,
        )
