from __future__ import annotations
from typing import Dict, Any, List, TYPE_CHECKING
import asyncio
from .classes import (
    Position, OrderRecord, MarketInfo, Snapshot, OrderBook,
    LiteOrderBookSnapshotEvent,
)
import time

if TYPE_CHECKING:
    # only needed for type hints — avoids the runtime circular import with risk_manager.py
    from ..market_decisions.risk_manager import RiskManager



class TradingState:
    """Single owner of all mutable shared trading state."""

    def __init__(self, risk_manager: RiskManager):
        self.markets: Dict[str, MarketInfo] = {}
        self.orderbooks: Dict[str, OrderBook] = {}
        self.market_quotes: Dict[str, LiteOrderBookSnapshotEvent] = {}
        self._market_price_timestamps: Dict[str, float] = {}
        self.positions: Dict[str, Position] = {}
        self.strategy_positions: Dict[str, Dict[str, Position]] = {}  # strategy -> {key: Position}
        self.open_orders: Dict[str, OrderRecord] = {}
        self.balance: Dict[str, float] = {"USD": 0.0}
        self.risk_manager: RiskManager = risk_manager
        self.paper_trading: Dict[str, bool] = risk_manager.config.paper_trading
        self.lock = asyncio.Lock()

    def _position_key(self, market_id: str, exchange: str, strategy: str) -> str:
        mode = "PAPER" if self.paper_trading.get(strategy, False) else "LIVE"
        return f"{mode}:{exchange}:{market_id}"

    @staticmethod
    def _strategy_position_key(market_id: str, exchange: str) -> str:
        return f"{exchange}:{market_id}"

    @staticmethod
    def _signed_position_size(position: Position) -> float:
        direction = 1.0 if position.outcome.upper() == "YES" else -1.0
        if position.side.value == "SELL":
            direction *= -1.0
        return direction * position.size

    async def update_position(self, position: Position) -> None:
        """Adds or overwrites a single position. Key encodes paper/live mode
        so paper and live fills for the same market never merge."""
        async with self.lock:
            key = self._position_key(position.market_id, position.exchange, position.strategy)
            self.positions[key] = position

    async def remove_position(self, market_id: str, exchange: str, strategy: str) -> None:
        """Removes a position, e.g. when fully closed. Needs strategy now to
        know whether to remove the paper or live key."""
        async with self.lock:
            key = self._position_key(market_id, exchange, strategy)
            self.positions.pop(key, None)

    async def update_strategy_position(self, position: Position) -> None:
        async with self.lock:
            key = self._strategy_position_key(position.market_id, position.exchange)
            self.strategy_positions.setdefault(position.strategy, {})[key] = position

    async def remove_strategy_position(self, strategy: str, market_id: str, exchange: str) -> None:
        async with self.lock:
            key = self._strategy_position_key(market_id, exchange)
            strategy_positions = self.strategy_positions.get(strategy)
            if strategy_positions is None:
                return
            strategy_positions.pop(key, None)
            if not strategy_positions:
                self.strategy_positions.pop(strategy, None)
   
    async def update_trading_state(self, market: MarketInfo) -> None:
        async with self.lock:
           self.markets[f'{market.exchange}:{market.market_id}'] = market

    async def update_orderbook(self, orderbook: OrderBook) -> None:
        async with self.lock:
            key = f"{orderbook.exchange}:{orderbook.market_id}:{orderbook.outcome}"
            existing = self.orderbooks.get(key)
            if existing is None or orderbook.timestamp >= existing.timestamp:
                self.orderbooks[key] = orderbook

    async def update_market_quote(self, quote: LiteOrderBookSnapshotEvent) -> None:
        async with self.lock:
            key = f"{quote.exchange}:{quote.market_id}:{quote.outcome}"
            existing = self.market_quotes.get(key)
            if existing is not None and existing.timestamp > quote.timestamp:
                return
            self.market_quotes[key] = quote
            if quote.last_price is not None:
                self._market_price_timestamps[key] = quote.timestamp
                market = self.markets.get(f"{quote.exchange}:{quote.market_id}")
                if market is not None:
                    market.prices[quote.outcome] = quote.last_price

    async def update_market_price(
        self, market_id: str, exchange: str, outcome: str, price: float,
        timestamp: float,
    ) -> None:
        async with self.lock:
            key = f"{exchange}:{market_id}:{outcome}"
            if self._market_price_timestamps.get(key, float("-inf")) > timestamp:
                return
            self._market_price_timestamps[key] = timestamp
            market = self.markets.get(f"{exchange}:{market_id}")
            if market is not None:
                market.prices[outcome] = price

    async def update_open_order(self, order: OrderRecord) -> None:
        """Adds or overwrites a single open order."""
        async with self.lock:
            self.open_orders[f"{order.exchange}:{order.order_id}"] = order

    async def apply_order_snapshot(self, exchange: str, orders: List[OrderRecord], complete: bool) -> None:
        """Replace live orders for an exchange only after a complete snapshot."""
        if not complete:
            return
        async with self.lock:
            prefix = f"{exchange}:"
            retained = {
                key: order for key, order in self.open_orders.items()
                if not key.startswith(prefix) or self.paper_trading.get(order.strategy, False)
            }
            snapshot_orders = {}
            for order in orders:
                key = f"{order.exchange}:{order.order_id}"
                existing = self.open_orders.get(key)
                if existing is not None:
                    order.strategy = existing.strategy
                    if order.filled_quantity is None:
                        order.filled_quantity = existing.filled_quantity
                snapshot_orders[key] = order
            self.open_orders = {**retained, **snapshot_orders}

    async def remove_open_order(self, order_id: str, exchange: str) -> None:
        """Removes an order, e.g. once filled or cancelled."""
        async with self.lock:
            key = f"{exchange}:{order_id}"
            self.open_orders.pop(key, None)

    async def update_live_position(self, position: Position) -> None:
        """Replace the exchange aggregate position, separate from strategy attribution."""
        async with self.lock:
            key = f"LIVE:{position.exchange}:{position.market_id}"
            existing = self.positions.get(key)
            if existing is not None:
                position.strategy = existing.strategy
            self.positions[key] = position

    async def remove_live_position(self, market_id: str, exchange: str) -> None:
        async with self.lock:
            self.positions.pop(f"LIVE:{exchange}:{market_id}", None)

    async def apply_live_position_snapshot(self, positions: List[Position], exchange: str) -> None:
        """Replace positions for one exchange while retaining paper and other exchange state."""
        async with self.lock:
            retained = {
                key: position for key, position in self.positions.items()
                if not key.startswith(f"LIVE:{exchange}:")
            }
            replacements = {}
            for position in positions:
                key = f"LIVE:{position.exchange}:{position.market_id}"
                existing = self.positions.get(key)
                if existing is not None:
                    position.strategy = existing.strategy
                replacements[key] = position
            self.positions = {**retained, **replacements}

    async def replace_balances(self, balances: Dict[str, float]) -> None:
        async with self.lock:
            self.balance = dict(balances)

    async def update_balance(self, currency: str, balance: float) -> None:
        async with self.lock:
            self.balance[currency] = balance

    async def reconcile(self, positions: List[Position], orders: List[OrderRecord]) -> None:
        async with self.lock:
            paper_positions = {
                k: v for k, v in self.positions.items() if k.startswith("PAPER:")
            }

            live_positions = {}
            for p in positions:
                key = f"LIVE:{p.exchange}:{p.market_id}"
                strategy_key = self._strategy_position_key(p.market_id, p.exchange)
                existing = self.positions.get(key)

                if existing is not None:
                    p.strategy = existing.strategy
                else:
                    pass
                    # orphan position, log critical

                strategies_size = sum(
                    self._signed_position_size(positions_by_key[strategy_key])
                    for strategy, positions_by_key in self.strategy_positions.items()
                    if not self.paper_trading.get(strategy, False) and strategy_key in positions_by_key
                )
                if abs(strategies_size - self._signed_position_size(p)) > 1e-9:
                    pass
                    # log warning: drift for {strategy_key}

                live_positions[key] = p

            self.positions = {**paper_positions, **live_positions}

            # --- orders ---
            paper_orders = {
                k: v for k, v in self.open_orders.items()
                if self.paper_trading.get(v.strategy, False)
            }

            live_orders = {}
            for o in orders:
                key = f"{o.exchange}:{o.order_id}"
                existing = self.open_orders.get(key)

                if existing is not None:
                    o.strategy = existing.strategy  # carry over local attribution
                else:
                    pass
                    # order exists on exchange but not locally tracked
                    # log warning: untracked order found — placed outside this bot? crashed before local write?

                live_orders[key] = o

            self.open_orders = {**paper_orders, **live_orders}

    async def get_cached_orderbook(self, market_id: str, outcome: str, exchange: str) -> OrderBook:
        async with self.lock:
            key = f"{exchange}:{market_id}:{outcome}"
            orderbook = self.orderbooks.get(key)

            if orderbook is None:
                raise KeyError(f"No orderbook cached for {key}")

            return orderbook

    async def snapshot(self) -> Snapshot:
        current_snapshot = Snapshot(
            timestamp=time.time(),
            markets=self.markets,
            orderbooks=self.orderbooks,
            positions=self.positions,
            open_orders=self.open_orders,
            balance=self.balance,
            exposure=self.risk_manager.get_exposure(self),
            market_quotes=self.market_quotes,

        )

        return current_snapshot
