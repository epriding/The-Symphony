"""Single owner of public market data and isolated portfolio ledgers.

Methods prefixed with _ are synchronous helpers called with lock held.
Network calls never run while this lock is held.
"""
import asyncio
from copy import deepcopy
from dataclasses import dataclass, replace
import time
import math

from .classes import (
    AccountBalance, ApprovedOrder, MarketInfo, OrderBook, OrderRecord, OrderStatus,
    PortfolioState, Position, Side, Snapshot, TERMINAL_ORDER_STATUSES, TradingConfig,
)
from ..market_decisions.risk_manager import RiskManager, position_key


@dataclass(frozen=True)
class _FillAmounts:
    """Newly executed quantity/cost, plus cumulative totals for deduplication."""
    quantity: float
    price: float
    total_notional: float
    total_fees: float
    new_fees: float


class TradingState:
    def __init__(self, config: TradingConfig, clock=time.time):
        self.config = config
        self.clock = clock
        self.markets = {}
        self.orderbooks = {}
        self.market_quotes = {}
        self._market_price_timestamps = {}
        self.lock = asyncio.Lock()
        self.portfolios = {}
        self.risk_managers = {}
        # Capture routing once: changing configuration cannot reclassify inventory.
        self.strategy_portfolios = {
            name: "PAPER" if paper else "LIVE"
            for name, paper in config.paper_trading.items()
        }
        self.strategy_portfolios.update(config.strategy_portfolios)
        for portfolio_id, risk_config in config.risk_configs.items():
            paper = portfolio_id != "LIVE"
            self.portfolios[portfolio_id] = PortfolioState(portfolio_id, paper)
            self.risk_managers[portfolio_id] = RiskManager(deepcopy(risk_config), portfolio_id, clock)
        if "LIVE" not in self.portfolios:
            raise ValueError("A LIVE portfolio configuration is required")
        if any(value not in self.portfolios for value in self.strategy_portfolios.values()):
            raise ValueError("Every strategy route must refer to a configured portfolio")
        for portfolio_id, exchanges in config.paper_starting_balances.items():
            portfolio = self.portfolios[portfolio_id]
            if not portfolio.paper:
                raise ValueError("Starting balances are only for paper portfolios")
            for exchange, cash in exchanges.items():
                portfolio.balances[exchange] = {
                    "USD": AccountBalance(
                        buying_power=cash, cash=cash, timestamp=clock(),
                        buying_power_includes_holds=False,
                    )
                }
                portfolio.positions_ready.add(exchange)
                portfolio.orders_ready.add(exchange)
            portfolio.equity_high_water = sum(exchanges.values())

    def portfolio_for_strategy(self, strategy):
        return self.strategy_portfolios.get(strategy)

    def _snapshot(self):
        return Snapshot(
            timestamp=self.clock(),
            markets=deepcopy(self.markets),
            orderbooks=deepcopy(self.orderbooks),
            portfolios=deepcopy(self.portfolios),
            market_quotes=deepcopy(self.market_quotes),
        )

    async def snapshot(self) -> Snapshot:
        async with self.lock:
            self._refresh_performance()
            return self._snapshot()

    def _refresh_performance(self):
        snapshot = self._snapshot()
        for portfolio_id, manager in self.risk_managers.items():
            equity = manager.equity(snapshot)
            portfolio = self.portfolios[portfolio_id]
            if equity is not None:
                portfolio.equity_high_water = max(portfolio.equity_high_water, equity)

    async def cash_flow(self, portfolio_id, exchange, amount):
        """Explicit deposit/withdrawal for paper; live cash comes from reconciliation.

        Always declare external flows so a deposit is not mistaken for profit or
        a withdrawal for drawdown. Live calls adjust only the performance baseline.
        """
        if isinstance(amount, bool) or not math.isfinite(amount):
            raise ValueError("Invalid cash flow")
        async with self.lock:
            portfolio = self.portfolios[portfolio_id]
            if portfolio.paper:
                balance = portfolio.balances[exchange]["USD"]
                available = self.risk_managers[portfolio_id].available_cash(portfolio, exchange)
                if amount < -available:
                    raise ValueError("Withdrawal exceeds unreserved cash")
                balance.cash += amount
                balance.buying_power += amount
            portfolio.net_cash_flows += amount
            portfolio.equity_high_water = max(0, portfolio.equity_high_water + amount)

    async def update_trading_state(self, market: MarketInfo):
        async with self.lock:
            overrides = self.config.market_overrides.get(f"{market.exchange}:{market.market_id}", {})
            self.markets[f"{market.exchange}:{market.market_id}"] = replace(market, **overrides)

    async def update_orderbook(self, orderbook: OrderBook):
        async with self.lock:
            key = position_key(orderbook.exchange, orderbook.market_id, orderbook.outcome)
            existing = self.orderbooks.get(key)
            if existing is None or orderbook.timestamp >= existing.timestamp:
                self.orderbooks[key] = deepcopy(orderbook)
                self._refresh_performance()

    async def update_market_quote(self, quote):
        async with self.lock:
            key = position_key(quote.exchange, quote.market_id, quote.outcome)
            existing = self.market_quotes.get(key)
            if existing is None or quote.timestamp >= existing.timestamp:
                self.market_quotes[key] = quote
                self._refresh_performance()

    async def update_market_price(self, market_id, exchange, outcome, price, timestamp):
        async with self.lock:
            key = position_key(exchange, market_id, outcome)
            if timestamp >= self._market_price_timestamps.get(key, float("-inf")):
                self._market_price_timestamps[key] = timestamp
                market = self.markets.get(f"{exchange}:{market_id}")
                if market:
                    market.prices[outcome] = price

    async def get_cached_orderbook(self, market_id, outcome, exchange):
        async with self.lock:
            return deepcopy(self.orderbooks[position_key(exchange, market_id, outcome)])

    def _reserve(self, decision):
        for order in decision.orders:
            portfolio = self.portfolios[order.portfolio_id]
            portfolio.reservations[order.intent_id] = order
            portfolio.processed_intents.add(order.intent_id)
            if order.batch_id:
                portfolio.processed_batches.add(order.batch_id)

    def _release(self, order: ApprovedOrder):
        portfolio = self.portfolios[order.portfolio_id]
        portfolio.reservations.pop(order.intent_id, None)
        portfolio.unresolved_intents.discard(order.intent_id)

    def _attribute_record(self, incoming, approved=None):
        record = deepcopy(incoming)
        if approved:
            return replace(record, strategy=approved.strategy, portfolio_id=approved.portfolio_id,
                           category=approved.category, intent_id=approved.intent_id,
                           batch_id=approved.batch_id, fee_per_contract=approved.fee_per_contract,
                           risk_price=approved.price)
        portfolio = self.portfolios[record.portfolio_id]
        old = portfolio.order_history.get(f"{record.exchange}:{record.order_id}")
        if old:
            attribution_fields = (
                "strategy", "portfolio_id", "category", "intent_id", "batch_id",
                "fee_per_contract", "risk_price",
            )
            for name in attribution_fields:
                setattr(record, name, getattr(old, name))
        else:
            market = self.markets.get(f"{record.exchange}:{record.market_id}")
            category = market.category if market else "other"
            categories = self.risk_managers[record.portfolio_id].config.diversification
            record.category = category if category in categories else "other"
        return record

    def _merge_fill(self, positions, record, quantity, price, strategy=""):
        key = position_key(record.exchange, record.market_id, record.outcome)
        existing = positions.get(key)
        if record.side == Side.SELL:
            if existing is None or quantity > existing.size + 1e-9:
                raise ValueError("Fill exceeds owned inventory; reconciliation required")
            realized = quantity * (price - existing.entry_price)
            existing.size = max(0, existing.size - quantity)
            if existing.size < 1e-9:
                positions.pop(key)
            return realized
        if existing:
            total = existing.size + quantity
            existing.entry_price = (existing.entry_price * existing.size + price * quantity) / total
            existing.size = total
        else:
            positions[key] = Position(
                market_id=record.market_id, exchange=record.exchange,
                outcome=record.outcome, size=quantity, entry_price=price,
                side=Side.BUY, strategy=strategy,
                portfolio_id=record.portfolio_id, category=record.category,
            )
        return 0.0

    def _apply_order_update(
        self, incoming, *, approved=None, fill_quantity=None,
        fill_price=None, execution_id=None,
    ):
        """Apply one response, stream update, or reconciliation record.

        Preserve local attribution, determine the new fill delta, account for it
        once, then update order status and release its submission reservation.
        The caller holds the state lock for this entire operation.
        """
        record = self._attribute_record(incoming, approved)
        invalid_identity = (
            not record.order_id or not record.market_id or not record.exchange
            or record.side not in (Side.BUY, Side.SELL)
            or record.outcome not in ("YES", "NO")
        )
        if invalid_identity:
            raise ValueError("Invalid normalized order identity")
        portfolio = self.portfolios[record.portfolio_id]
        order_key = f"{record.exchange}:{record.order_id}"
        previous = portfolio.order_history.get(order_key)
        if approved and previous and not previous.strategy:
            # Recovery can assign a previously unattributed exchange fill.
            portfolio.filled_by_order.pop(order_key, None)
            portfolio.fill_notional_by_order.pop(order_key, None)
            portfolio.fees_by_order.pop(order_key, None)

        known_quantity = portfolio.filled_by_order.get(order_key, 0.0)
        cumulative_quantity = self._cumulative_fill_quantity(
            record, portfolio, order_key, known_quantity,
            fill_quantity=fill_quantity, execution_id=execution_id,
        )
        self._validate_order_quantities(record, cumulative_quantity)
        if cumulative_quantity < known_quantity - 1e-9:
            return previous or record
        if previous and previous.status in TERMINAL_ORDER_STATUSES:
            record.status = previous.status
        if not portfolio.paper and record != previous:
            # An external order can place/release cash holds without filling.
            # Buying power is usable again only after a coherent account refresh.
            portfolio.dirty_exchanges.add(record.exchange)

        fill = self._calculate_fill_amounts(
            record, portfolio, order_key, known_quantity, cumulative_quantity, fill_price,
        )
        if record.strategy and fill.quantity > 0:
            self._apply_strategy_fill(portfolio, record, fill)
            self._remember_fill(portfolio, order_key, cumulative_quantity, fill)
        elif not record.strategy:
            if fill.quantity > 0:
                portfolio.dirty_exchanges.add(record.exchange)
            # External fills have no strategy ownership. Position snapshots own
            # their inventory; only remember the counters here.
            self._remember_fill(portfolio, order_key, cumulative_quantity, fill)
        else:
            self._apply_fill_adjustment(portfolio, record, order_key, fill)
            self._remember_fill(portfolio, order_key, cumulative_quantity, fill)

        if execution_id:
            portfolio.seen_executions.add((order_key, execution_id))
        record.filled_quantity = cumulative_quantity
        portfolio.order_history[order_key] = record
        if record.status in TERMINAL_ORDER_STATUSES:
            portfolio.open_orders.pop(order_key, None)
        else:
            portfolio.open_orders[order_key] = record
        if approved:
            self._release(approved)
            if not portfolio.paper:
                portfolio.dirty_exchanges.add(record.exchange)
        return record

    def _cumulative_fill_quantity(
        self, record, portfolio, order_key, known_quantity, *,
        fill_quantity=None, execution_id=None,
    ) -> float:
        cumulative_quantity = record.filled_quantity
        if cumulative_quantity is None and record.status == OrderStatus.FILLED:
            cumulative_quantity = record.size
        elif cumulative_quantity is None and record.status in TERMINAL_ORDER_STATUSES:
            # A canceled order with unknown cumulative fills may have traded.
            # Retain the commitment until its final fill counter is established.
            record.status = OrderStatus.UNKNOWN
            portfolio.dirty_exchanges.add(record.exchange)
        if cumulative_quantity is None:
            if fill_quantity is not None:
                if not execution_id:
                    raise ValueError("Fill without cumulative quantity requires an execution ID")
                token = (order_key, execution_id)
                if token in portfolio.seen_executions:
                    cumulative_quantity = known_quantity
                else:
                    cumulative_quantity = known_quantity + fill_quantity
            else:
                cumulative_quantity = known_quantity
        return cumulative_quantity

    def _validate_order_quantities(self, record, cumulative_quantity):
        amounts = (record.size, record.price, cumulative_quantity)
        invalid_amount = any(
            isinstance(amount, bool) or not math.isfinite(amount) or amount < 0
            for amount in amounts
        )
        if invalid_amount or cumulative_quantity > record.size + 1e-9 or record.price > 1:
            raise ValueError("Invalid order quantities or price")

    def _calculate_fill_amounts(
        self, record, portfolio, order_key, known_quantity, cumulative_quantity, fill_price,
    ) -> _FillAmounts:
        # avgPx covers all fills. Subtract prior notional to obtain this delta's
        # price rather than applying the cumulative average to the new shares.
        quantity_change = max(0, cumulative_quantity - known_quantity)
        if record.average_fill_price is not None:
            if (isinstance(record.average_fill_price, bool)
                    or not math.isfinite(record.average_fill_price)
                    or not 0 <= record.average_fill_price <= 1):
                raise ValueError("Invalid average fill price")
            total_notional = cumulative_quantity * record.average_fill_price
            if quantity_change:
                previous_notional = portfolio.fill_notional_by_order.get(order_key, 0.0)
                price = (total_notional - previous_notional) / quantity_change
            else:
                price = record.price
        else:
            price = fill_price if fill_price is not None else record.price
            total_notional = portfolio.fill_notional_by_order.get(order_key, 0.0) + quantity_change * price
        if not math.isfinite(price) or not 0 <= price <= 1:
            raise ValueError("Invalid fill price")
        total_fees = record.fees_paid
        if total_fees is None:
            total_fees = portfolio.fees_by_order.get(order_key, 0.0)
        new_fees = total_fees - portfolio.fees_by_order.get(order_key, 0.0)
        if not math.isfinite(total_fees) or total_fees < 0 or new_fees < -1e-9:
            raise ValueError("Invalid cumulative fees")
        return _FillAmounts(quantity_change, price, total_notional, total_fees, new_fees)

    def _apply_strategy_fill(self, portfolio, record, fill: _FillAmounts):
        # Validate both ledgers and paper cash before mutating either.
        strategy_positions = portfolio.strategy_positions.setdefault(record.strategy, {})
        position_key_value = position_key(record.exchange, record.market_id, record.outcome)
        if record.side == Side.SELL:
            owned = strategy_positions.get(position_key_value)
            held = portfolio.positions.get(position_key_value)
            exceeds_ownership = not owned or fill.quantity > owned.size + 1e-9
            exceeds_account_inventory = portfolio.paper and (
                not held or fill.quantity > held.size + 1e-9
            )
            if exceeds_ownership or exceeds_account_inventory:
                portfolio.dirty_exchanges.add(record.exchange)
                raise ValueError("Sell fill exceeds inventory")
        if portfolio.paper:
            balance = portfolio.balances[record.exchange]["USD"]
            cash_change = (-1 if record.side == Side.BUY else 1) * fill.quantity * fill.price - fill.new_fees
            if balance.cash + cash_change < -1e-9:
                raise ValueError("Paper fill exceeds cash")
            self._merge_fill(portfolio.positions, record, fill.quantity, fill.price)
            balance.cash = max(0, balance.cash + cash_change)
            balance.buying_power = balance.cash
            balance.timestamp = self.clock()
        else:
            # Account inventory/cash are exchange-authoritative. Block entries
            # until positions, orders and balances have been reconciled together.
            portfolio.dirty_exchanges.add(record.exchange)
        realized_profit = self._merge_fill(
            strategy_positions, record, fill.quantity, fill.price, record.strategy,
        )
        portfolio.realized_pnl += realized_profit - fill.new_fees
        portfolio.strategy_fills.append(replace(record, size=fill.quantity, price=fill.price))

    def _replay_fills(self, fills, strategy=None):
        """Rebuild weighted cost and gross realized P&L in execution order."""
        positions = {}
        realized = 0.0
        for fill in fills:
            if strategy is None or fill.strategy == strategy:
                realized += self._merge_fill(
                    positions, fill, fill.size, fill.price, strategy or "",
                )
        return positions, realized

    def _apply_fill_adjustment(self, portfolio, record, order_key, fill):
        """Apply late cumulative cost/fee reports without adding shares again.

        A cumulative average reprices that order's recorded executions. Replaying
        weighted costs also corrects realized P&L if some shares were already sold.
        Live account cash/inventory remain owned by exchange reconciliation.
        """
        notional_change = fill.total_notional - portfolio.fill_notional_by_order.get(order_key, 0.0)
        cost_changed = abs(notional_change) > 1e-9
        if not cost_changed and abs(fill.new_fees) <= 1e-9:
            return

        corrected_fills = portfolio.strategy_fills
        realized_change = 0.0
        if cost_changed:
            corrected_fills = [
                replace(previous, price=record.average_fill_price)
                if (previous.exchange, previous.order_id) == (record.exchange, record.order_id)
                else previous
                for previous in portfolio.strategy_fills
            ]
            _, old_realized = self._replay_fills(portfolio.strategy_fills, record.strategy)
            strategy_positions, new_realized = self._replay_fills(corrected_fills, record.strategy)
            realized_change = new_realized - old_realized

        if portfolio.paper:
            balance = portfolio.balances[record.exchange]["USD"]
            cash_change = (-1 if record.side == Side.BUY else 1) * notional_change - fill.new_fees
            if balance.cash + cash_change < -1e-9:
                raise ValueError("Paper fill correction exceeds cash")
            if cost_changed:
                positions, _ = self._replay_fills(corrected_fills)
                portfolio.positions = positions
            balance.cash = max(0, balance.cash + cash_change)
            balance.buying_power = balance.cash
            balance.timestamp = self.clock()
        else:
            portfolio.dirty_exchanges.add(record.exchange)

        if cost_changed:
            portfolio.strategy_positions[record.strategy] = strategy_positions
            portfolio.strategy_fills = corrected_fills
        portfolio.realized_pnl += realized_change - fill.new_fees

    def _remember_fill(self, portfolio, order_key, cumulative_quantity, fill):
        portfolio.filled_by_order[order_key] = cumulative_quantity
        portfolio.fill_notional_by_order[order_key] = fill.total_notional
        portfolio.fees_by_order[order_key] = fill.total_fees

    def _replace_live_positions(self, positions, exchange):
        portfolio = self.portfolios["LIVE"]
        old = portfolio.positions
        replacements = {}
        for incoming in positions:
            if (incoming.exchange != exchange or incoming.side != Side.BUY
                    or incoming.outcome not in ("YES", "NO") or not incoming.market_id
                    or not math.isfinite(incoming.size) or incoming.size <= 0
                    or not math.isfinite(incoming.entry_price) or not 0 <= incoming.entry_price <= 1):
                raise ValueError("Invalid normalized position")
            position = deepcopy(incoming)
            key = position_key(exchange, position.market_id, position.outcome)
            position.portfolio_id = "LIVE"
            market = self.markets.get(f"{exchange}:{position.market_id}")
            category = market.category if market else "other"
            category = category if category in self.risk_managers["LIVE"].config.diversification else "other"
            position.category = old[key].category if key in old else category
            if key in old:
                position.strategy = old[key].strategy
            replacements[key] = position
        portfolio.positions = {k: v for k, v in old.items() if v.exchange != exchange} | replacements
        portfolio.positions_ready.add(exchange)
        portfolio.positions_updated_at[exchange] = self.clock()

    def _invalidate_exchange(self, exchange):
        portfolio = self.portfolios["LIVE"]
        portfolio.dirty_exchanges.add(exchange)
        portfolio.account_revisions[exchange] = portfolio.account_revisions.get(exchange, 0) + 1

    async def apply_live_position_snapshot(self, positions, exchange):
        async with self.lock:
            self._replace_live_positions(positions, exchange)
            self._invalidate_exchange(exchange)

    async def update_live_position(self, position):
        async with self.lock:
            portfolio = self.portfolios["LIVE"]
            positions = [
                held for held in portfolio.positions.values()
                if held.exchange == position.exchange and held.market_id != position.market_id
            ]
            self._replace_live_positions(positions + [position], position.exchange)
            self._invalidate_exchange(position.exchange)

    async def remove_live_position(self, market_id, exchange):
        async with self.lock:
            portfolio = self.portfolios["LIVE"]
            portfolio.positions = {
                key: position for key, position in portfolio.positions.items()
                if (position.exchange, position.market_id) != (exchange, market_id)
            }
            self._invalidate_exchange(exchange)

    @staticmethod
    def _older_balance_source(incoming: AccountBalance, existing: AccountBalance | None) -> bool:
        if existing is None or incoming.source_timestamp is None or existing.source_timestamp is None:
            return False
        return incoming.source_timestamp < existing.source_timestamp

    def _replace_balances(self, balances, exchange, timestamp=None):
        if any(not isinstance(value, AccountBalance) for value in balances.values()):
            raise ValueError("Balances must explicitly distinguish cash and buying power")
        portfolio = self.portfolios["LIVE"]
        current = portfolio.balances.get(exchange, {})
        stamp = timestamp
        if stamp is None:
            stamp = max((balance.timestamp for balance in balances.values()), default=self.clock())
        if current and max(b.timestamp for b in current.values()) > stamp:
            return False
        for currency, balance in balances.items():
            existing = current.get(currency)
            if self._older_balance_source(balance, existing):
                return False
        portfolio.balances[exchange] = deepcopy(balances)
        return True

    async def replace_balances(self, balances, exchange, timestamp=None):
        async with self.lock:
            self._replace_balances(balances, exchange, timestamp)
            self._invalidate_exchange(exchange)

    async def update_balance(self, currency, balance, exchange):
        async with self.lock:
            portfolio = self.portfolios["LIVE"]
            existing = portfolio.balances.get(exchange, {}).get(currency)
            if self._older_balance_source(balance, existing):
                return
            if existing is None or balance.timestamp >= existing.timestamp:
                portfolio.balances.setdefault(exchange, {})[currency] = deepcopy(balance)
                self._invalidate_exchange(exchange)
