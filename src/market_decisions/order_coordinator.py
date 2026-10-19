from  ..api.interfaces.base_interfaces import RestClient
from ..market_decisions.risk_manager import RiskManager
from ..utilities.trading_state import TradingState
from ..utilities.classes import OrderIntent, OrderRecord, Position, OrderStatus, Side
from ..paper_trading.paper_trading_engine import PaperTradingEngine
from ..utilities.logger import setup_logger
from typing import Optional
import asyncio

logger = setup_logger(__name__)

class OrderCoordinator:
    """Serializes all order actions and maintains local correctness."""

    def __init__(self, client: RestClient, risk: RiskManager, state: TradingState, paper_engine: PaperTradingEngine):
        self.client = client
        self.risk = risk
        self.state = state
        self.paper_engine = paper_engine
        self.lock = asyncio.Lock()

    async def submit(self, order: OrderIntent) -> Optional[OrderRecord]:
        async with self.lock:
            if not self.risk.check_trade_allowed(order=order):
                return None

            if self.state.paper_trading[order.strategy]:
                market = self.state.markets[f"{order.exchange}:{order.market_id}"]
                record = await self.paper_engine.simulate_fill(order, market)
                await self._apply_fill(record)
            else:
                record = await self.client.place_order(order)

            if record is not None:
                await self.state.update_open_order(record)
            return record

    def _merge_position(self, existing: Optional[Position], order: OrderRecord) -> Optional[Position]:
        """Net YES and NO contract exposure into one market position."""
        incoming_sign = self._signed_order_size(order) / order.size if order.size else 0.0
        incoming_outcome = "YES" if incoming_sign > 0 else "NO"
        # Store prices in the units of the resulting net outcome. Selling either
        # outcome is equivalent to buying its complement.
        incoming_price = order.price if order.side == Side.BUY else 1.0 - order.price
        if existing is None:
            return Position(
                market_id=order.market_id,
                exchange=order.exchange,
                outcome=incoming_outcome,
                size=order.size,
                entry_price=incoming_price,
                side=Side.BUY,
                strategy=order.strategy,
            )
        existing_signed_size = self.state._signed_position_size(existing)
        incoming_signed_size = self._signed_order_size(order)
        if existing_signed_size * incoming_signed_size > 0:
            total_size = existing.size + order.size
            avg_price = (
                existing.entry_price * existing.size + incoming_price * order.size
            ) / total_size
            existing.size = total_size
            existing.outcome = incoming_outcome
            existing.side = Side.BUY
            existing.entry_price = avg_price
            return existing

        remaining = existing.size - order.size
        if remaining > 1e-12:
            existing.size = remaining
            return existing
        if abs(remaining) <= 1e-12:
            return None
        return Position(
            market_id=order.market_id,
            exchange=order.exchange,
            outcome=incoming_outcome,
            size=abs(remaining),
            entry_price=incoming_price,
            side=Side.BUY,
            strategy=order.strategy,
        )

    @staticmethod
    def _signed_order_size(order: OrderRecord) -> float:
        direction = 1.0 if order.outcome.upper() == "YES" else -1.0
        if order.side == Side.SELL:
            direction *= -1.0
        return direction * order.size


    async def _apply_fill(self, order: OrderRecord) -> None:
        """Apply the quantity in ``size`` as one fill delta.

        ``filled_quantity`` is cumulative order state and must not be added to
        positions directly; callers pass only the newly filled amount in size.
        """
        #if the strategy is not set, it automatically skips it just for safety
        if not order.strategy:
            logger.warning("Order %s has no strategy set, skipping _apply_fill", order.order_id)
            return

        if self.state.paper_trading.get(order.strategy, False):
            key = self.state._position_key(order.market_id, order.exchange, order.strategy)

            result = self._merge_position(self.state.positions.get(key), order)
            if result is None:
                await self.state.remove_position(order.market_id, order.exchange, order.strategy)
            else:
                await self.state.update_position(result)

        strategy_key = self.state._strategy_position_key(order.market_id, order.exchange)
        strategy_existing = self.state.strategy_positions.get(order.strategy, {}).get(strategy_key)
        strategy_result = self._merge_position(strategy_existing, order)

        if strategy_result is None:
            await self.state.remove_strategy_position(order.strategy, order.market_id, order.exchange)
        else:
            await self.state.update_strategy_position(strategy_result)

    async def apply_fill(self, order: OrderRecord) -> None:
        """Apply an exchange-reported fill using the same accounting as submissions."""
        async with self.lock:
            await self._apply_fill(order)

    async def cancel(self, order_id: str) -> None:
        raise NotImplementedError

    async def reconcile(self) -> None:
        async with self.lock:
            live_positions = await self.client.get_positions()
            live_orders = await self.client.get_open_orders()
            live_orders_by_keys = {f"{o.exchange}:{o.order_id}": o for o in live_orders}

            for k, o in self.state.open_orders.items():
                if self.state.paper_trading.get(o.strategy, False):
                    continue
                live_o = live_orders_by_keys.get(k)
                if live_o is None:
                    # The open-orders endpoint omits completed/canceled orders.
                    # Ask the exchange for this exact order before attributing fills.
                    live_o = await self.client.get_order(o.order_id)
                    if live_o is None:
                        logger.warning(
                            "Order %s is no longer open and Polymarket could not retrieve it; "
                            "no fill will be attributed from position changes",
                            o.order_id,
                        )
                        continue

                # For orders still open, cumQuantity is the authoritative fill
                # counter. The same cumulative counter is checked on individually
                # retrieved terminal orders, avoiding position-change guesses.
                previous_filled = o.filled_quantity or 0.0
                current_filled = live_o.filled_quantity
                if current_filled is None or current_filled <= previous_filled + 1e-9:
                    continue

                fill_size = current_filled - previous_filled
                await self._apply_fill(OrderRecord(
                    order_id=o.order_id, market_id=o.market_id, exchange=o.exchange,
                    outcome=o.outcome, side=o.side, size=fill_size, price=live_o.price,
                    status=OrderStatus.FILLED, strategy=o.strategy, tif=o.tif,
                    filled_quantity=current_filled,
                ))

            await self.state.reconcile(live_positions, live_orders)
