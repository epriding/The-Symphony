"""Single owner of public market data and isolated portfolio ledgers.

Methods prefixed with _ are synchronous helpers called with lock held.
Network calls never run while this lock is held.
"""
import asyncio
from copy import deepcopy
from dataclasses import dataclass, replace
import time
import math
from decimal import Decimal

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
    exact_new_notional: str | None = None


class TradingState:
    def __init__(self, config: TradingConfig, clock=time.time, history_store=None, limits=None):
        self.config = config
        self.history_store = history_store
        self.limits = limits or {}
        self.history_revision = 0
        self._history_committed_revision = -1
        self._correction_projection = None
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

    def _risk_view(self):
        """Borrow state for synchronous risk reads while holding the state lock.

        The view must not escape the locked calculation. Public snapshots remain
        independent copies; equity and risk evaluation only return scalar values
        or new approvals, never references into this view.
        """
        return Snapshot(
            timestamp=self.clock(), markets=self.markets, orderbooks=self.orderbooks,
            portfolios=self.portfolios, market_quotes=self.market_quotes,
        )

    def _refresh_performance(self):
        # Callers hold the state lock. Equity reads these references synchronously
        # and returns only a number, so no ledger view escapes this calculation.
        snapshot = self._risk_view()
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
        self.history_revision += 1
        if self.history_store:
            active = sum(len(p.open_orders) + len(p.reservations) for p in self.portfolios.values() if p.paper)
            limit = self.limits.get("max_active_orders", 64)
            if active + len(decision.orders) > limit:
                raise BufferError("max_active_orders_exceeded")
            keys = {(o.exchange, o.market_id, o.outcome) for o in decision.orders if o.side == Side.BUY}
            held = {(p.exchange, p.market_id, p.outcome) for a in self.portfolios.values() if a.paper for p in a.positions.values()}
            pending = {(o.exchange, o.market_id, o.outcome) for a in self.portfolios.values() if a.paper for o in a.pending_orders if o.side == Side.BUY}
            if len(held | pending | keys) > self.limits.get("max_positions", 32):
                raise BufferError("max_positions_exceeded")
        for order in decision.orders:
            portfolio = self.portfolios[order.portfolio_id]
            portfolio.reservations[order.intent_id] = order
            portfolio.processed_intents.add(order.intent_id)
            if order.batch_id:
                portfolio.processed_batches.add(order.batch_id)

    def _release(self, order: ApprovedOrder):
        self.history_revision += 1
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
        if portfolio.paper and previous and record.revision and record.revision <= previous.revision:
            return previous
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
        elif not record.strategy:
            if fill.quantity > 0:
                portfolio.dirty_exchanges.add(record.exchange)
            # External fills have no strategy ownership. Position snapshots own
            # their inventory; only remember the counters here.
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
        self.history_revision += 1
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
        quantity_delta = Decimal(str(cumulative_quantity)) - Decimal(str(known_quantity))
        quantity_change = float(max(Decimal(0), quantity_delta))
        exact_delta = None
        if record.cumulative_notional is not None:
            total_exact = Decimal(record.cumulative_notional)
            if not total_exact.is_finite() or total_exact < 0:
                raise ValueError("Invalid cumulative notional")
            previous_record = portfolio.order_history.get(order_key)
            if previous_record and previous_record.cumulative_notional is not None:
                previous_exact = previous_record.cumulative_notional
            else:
                previous_exact = str(portfolio.fill_notional_by_order.get(order_key, 0))
            exact_delta = str(total_exact - Decimal(previous_exact))
            total_notional = float(total_exact)
            price = record.price
            if quantity_change:
                price = float(Decimal(exact_delta) / Decimal(str(quantity_change)))
        elif record.average_fill_price is not None:
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
        previous_fees = Decimal(str(portfolio.fees_by_order.get(order_key, 0.0)))
        new_fees = float(Decimal(str(total_fees)) - previous_fees)
        if not math.isfinite(total_fees) or total_fees < 0 or new_fees < -1e-9:
            raise ValueError("Invalid cumulative fees")
        return _FillAmounts(
            quantity=quantity_change,
            price=price,
            total_notional=total_notional,
            total_fees=total_fees,
            new_fees=new_fees,
            exact_new_notional=exact_delta,
        )

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
            notional = (Decimal(fill.exact_new_notional) if fill.exact_new_notional is not None
                        else Decimal(str(fill.quantity)) * Decimal(str(fill.price)))
            cash_change = (-1 if record.side == Side.BUY else 1) * notional - Decimal(str(fill.new_fees))
            new_cash = Decimal(str(balance.cash)) + cash_change
            if new_cash < Decimal("-0.000000001"):
                raise ValueError("Paper fill exceeds cash")
            self._merge_fill(portfolio.positions, record, fill.quantity, fill.price)
            balance.cash = float(max(Decimal(0), new_cash))
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
        historical = replace(record, size=fill.quantity, price=fill.price)
        if self.history_store:
            # A report may omit its cumulative counter or reuse revision zero.
            # The applied cumulative quantity still identifies this actual
            # positive fill delta across archive/export retries.
            order_key = f"{record.exchange}:{record.order_id}"
            historical.filled_quantity = portfolio.filled_by_order.get(order_key, 0.0) + fill.quantity
        portfolio.strategy_fills.append(historical)

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
        notional_change = (float(Decimal(fill.exact_new_notional)) if fill.exact_new_notional is not None
                           else fill.total_notional - portfolio.fill_notional_by_order.get(order_key, 0.0))
        cost_changed = abs(notional_change) > 1e-9
        if not cost_changed and abs(fill.new_fees) <= 1e-9:
            return

        if self.history_store and self._correction_projection is not None:
            projection = self._correction_projection
            if projection["identity"] != (portfolio.portfolio_id, order_key):
                raise RuntimeError("Correction projection identity mismatch")
            if projection["revision"] != self.history_revision:
                raise RuntimeError("Account changed during correction reconstruction")
            cash_change = (-1 if record.side == Side.BUY else 1) * Decimal(str(notional_change)) - Decimal(str(fill.new_fees))
            if portfolio.paper:
                balance = portfolio.balances[record.exchange]["USD"]
                new_cash = Decimal(str(balance.cash)) + cash_change
                if new_cash < Decimal("-0.000000001"):
                    raise ValueError("Paper fill correction exceeds cash")
                portfolio.positions = projection["positions"]
                balance.cash = float(max(Decimal(0), new_cash))
                balance.buying_power = balance.cash
                balance.timestamp = self.clock()
            else:
                portfolio.dirty_exchanges.add(record.exchange)
            portfolio.strategy_positions[record.strategy] = projection["strategy_positions"]
            portfolio.realized_pnl += projection["realized_change"] - fill.new_fees
            return

        corrected_fills = portfolio.strategy_fills
        realized_change = 0.0
        if cost_changed:
            corrected_fills = [
                replace(previous, price=(float(Decimal(record.cumulative_notional) / Decimal(str(record.filled_quantity)))
                                         if record.cumulative_notional is not None and record.filled_quantity
                                         else record.average_fill_price))
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
            balance.cash = float(max(Decimal(0), Decimal(str(balance.cash)) + Decimal(str(cash_change))))
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

    def _apply_cash_credit(self, credit):
        portfolio = self.portfolios[credit.portfolio_id]
        amount = Decimal(credit.amount)
        if not portfolio.paper or not amount.is_finite() or amount < 0 or not credit.credit_id:
            raise ValueError("Invalid paper cash credit")
        if credit.credit_id in portfolio.cash_credits:
            if Decimal(str(portfolio.cash_credits[credit.credit_id])) != amount:
                raise ValueError("Credit ID reused with a different amount")
            return
        balance = portfolio.balances[credit.exchange]["USD"]
        balance.cash = float(Decimal(str(balance.cash)) + amount)
        balance.buying_power, balance.timestamp = balance.cash, self.clock()
        portfolio.cash_credits[credit.credit_id] = amount if self.history_store else float(amount)
        self.history_revision += 1
        portfolio.realized_pnl += float(amount)

    def _apply_settlement(self, event):
        if set(event.payouts) != {"YES", "NO"}:
            raise ValueError("Explicit YES and NO payouts are required")
        payouts = {outcome: Decimal(value) for outcome, value in event.payouts.items()}
        if any(not value.is_finite() or not 0 <= value <= 1 for value in payouts.values()):
            raise ValueError("Invalid authoritative settlement payout")
        key = f"{event.exchange}:{event.market_id}"
        for portfolio in self.portfolios.values():
            if not portfolio.paper or key in portfolio.settlements:
                continue
            for strategy, positions in list(portfolio.strategy_positions.items()):
                for position in list(positions.values()):
                    if (position.exchange, position.market_id) != (event.exchange, event.market_id):
                        continue
                    price = payouts[position.outcome]
                    record = OrderRecord(
                        f"settlement:{event.settlement_id}:{strategy}:{position.outcome}",
                        event.market_id, event.exchange, position.outcome, Side.SELL,
                        position.size, float(price), status=OrderStatus.FILLED,
                        strategy=strategy, portfolio_id=portfolio.portfolio_id,
                        filled_quantity=position.size, average_fill_price=float(price),
                        cumulative_notional=str(Decimal(str(position.size)) * price),
                        fees_paid=0, reason="authoritative_settlement", revision=1,
                    )
                    self._apply_order_update(record)
            portfolio.settlements.add(key)
            self.history_revision += 1

    async def prepare_history_update(self, incoming, approved=None, execution_id=None, fill_quantity=None):
        """Load indexed evidence and reconstruct late costs without the state lock.

        Caller owns the coordinator lock. Public data may advance concurrently;
        account transitions may not. The revision guards that assumption.
        """
        store = self.history_store
        self._correction_projection = None
        if store is None:
            return
        pid = approved.portfolio_id if approved else incoming.portfolio_id
        key = f"{incoming.exchange}:{incoming.order_id}"
        async with self.lock:
            old = self.portfolios[pid].order_history.get(key)
        if old is None:
            old = await store.aget(f"orders:{pid}", key)
        async with self.lock:
            portfolio = self.portfolios[pid]
            hot = portfolio.order_history.get(key)
            if hot is not None:
                old = hot
            if old is not None:
                if hot is None:
                    self._history_committed_revision = -1
                portfolio.order_history[key] = old
                portfolio.filled_by_order[key] = old.filled_quantity or 0
                portfolio.fill_notional_by_order[key] = float(old.cumulative_notional) if old.cumulative_notional is not None else (old.filled_quantity or 0) * (old.average_fill_price if old.average_fill_price is not None else old.price)
                portfolio.fees_by_order[key] = old.fees_paid or 0
            revision = self.history_revision
        duplicate_execution = execution_id and await store.acontains(f"seen_executions:{pid}", (key, execution_id))
        if duplicate_execution:
            async with self.lock:
                portfolio.seen_executions.add((key, execution_id))
        quantity = incoming.filled_quantity
        if quantity is None and incoming.status == OrderStatus.FILLED:
            quantity = incoming.size
        elif quantity is None and old is not None:
            quantity = (old.filled_quantity or 0) + (fill_quantity if fill_quantity is not None and not duplicate_execution else 0)
        if (old is None or quantity is None or quantity != (old.filled_quantity or 0)
                or not old.strategy or (incoming.revision and incoming.revision <= old.revision)):
            return
        old_notional = Decimal(old.cumulative_notional) if old.cumulative_notional is not None else Decimal(str((old.filled_quantity or 0) * (old.average_fill_price if old.average_fill_price is not None else old.price)))
        new_notional = Decimal(incoming.cumulative_notional) if incoming.cumulative_notional is not None else (Decimal(str(quantity)) * Decimal(str(incoming.average_fill_price)) if incoming.average_fill_price is not None else old_notional)
        if new_notional == old_notional:
            return
        if not new_notional.is_finite() or quantity <= 0:
            raise ValueError("Invalid cumulative correction")
        price = float(new_notional / Decimal(str(quantity)))
        if not 0 <= price <= 1:
            raise ValueError("Invalid cumulative correction price")
        # Flush any hot fill sequence before streaming. No state lock is held.
        await self.compact_history()
        def reconstruct(conn):
            old_positions, positions = {}, {}
            old_owned, owned = {}, {}
            old_realized = new_realized = 0.0
            cursor = conn.execute('SELECT sequence,value FROM records WHERE stream=? ORDER BY sequence', (f'fills:{pid}',))
            while True:
                page = cursor.fetchmany(store.page_size)
                if not page:
                    break
                store._check_deadline()
                for sequence, raw in page:
                    fill = store._decode(raw)
                    effective = fill
                    corrected_price = store._get(conn, f'corrected-costs:{pid}', f'{fill.exchange}:{fill.order_id}')
                    if corrected_price is not None:
                        if isinstance(corrected_price, dict):
                            if sequence <= corrected_price['through_sequence']:
                                effective = replace(fill, price=corrected_price['price'])
                        else:
                            effective = replace(fill, price=corrected_price)
                    corrected = replace(effective, price=price) if (fill.exchange, fill.order_id) == (incoming.exchange, incoming.order_id) else effective
                    self._merge_fill(old_positions, effective, effective.size, effective.price)
                    self._merge_fill(positions, corrected, corrected.size, corrected.price)
                    if fill.strategy == old.strategy:
                        old_realized += self._merge_fill(old_owned, effective, effective.size, effective.price, old.strategy)
                        new_realized += self._merge_fill(owned, corrected, corrected.size, corrected.price, old.strategy)
                    if max(len(old_positions), len(positions), len(old_owned), len(owned)) > self.limits.get('max_positions', 32):
                        raise BufferError('correction_position_capacity_exceeded')
            return dict(identity=(pid, key), revision=revision, positions=positions,
                        strategy_positions=owned, realized_change=new_realized-old_realized)
        projection = await store.run(reconstruct)
        async with self.lock:
            if self.history_revision != revision:
                raise RuntimeError("Account changed during correction reconstruction")
            self._correction_projection = projection

    async def compact_history(self, *, force=True):
        """Commit indexed history/fills before removing any hot evidence.

        Caller serializes account transitions with the coordinator lock. Snapshots
        can be taken while SQLite works and contain only bounded hot history.
        """
        store = self.history_store
        if store is None:
            return
        if self._history_committed_revision == self.history_revision:
            return
        hot_limit = self.limits.get("max_hot_history_records", 16)
        if not force:
            # Keep a finite batch hot between durable commits. Active evidence
            # never needs eviction, and syncing every reservation/no-fill ack
            # would add filesystem latency without improving retention safety.
            # A full batch, explicit query/finalization or reconstruction forces
            # commit before the ordinary hot-window eviction below.
            batch = min(hot_limit, self.limits.get('max_archive_cache_records', 8))
            membership_limit = hot_limit + self.limits.get('max_active_orders', 64)
            needs_commit = any(
                sum(record.status in TERMINAL_ORDER_STATUSES for record in portfolio.order_history.values()) >= hot_limit + batch
                or len(portfolio.strategy_fills) >= batch
                or len(portfolio.seen_executions) >= batch
                or len(portfolio.cash_credits) >= hot_limit + batch
                or len(portfolio.processed_intents) >= membership_limit
                or len(portfolio.processed_batches) >= membership_limit
                for portfolio in self.portfolios.values() if portfolio.paper)
            if not needs_commit:
                return
        for pid, portfolio in self.portfolios.items():
            if not portfolio.paper:
                continue
            async with self.lock:
                records = list(portfolio.order_history.items())
                fills = list(portfolio.strategy_fills)
                memberships = {name: list(getattr(portfolio, name)) for name in
                    ("processed_intents", "processed_batches", "seen_executions", "settlements")}
                credits = list(portfolio.cash_credits.items())
                revision = self.history_revision
            def commit(conn):
                for key, record in records:
                    old = store._get(conn, f'orders:{pid}', key)
                    if old == record:
                        continue
                    store._put(conn, f'orders:{pid}', key, record, True)
                    # Legacy reports may carry revision zero. The account
                    # transition supplies an immutable archive identity too.
                    store._put(conn, f'order-revisions:{pid}', (key, revision, record.revision), record)
                    if old and (old.filled_quantity or 0) == (record.filled_quantity or 0):
                        old_n = Decimal(old.cumulative_notional) if old.cumulative_notional is not None else Decimal(str((old.filled_quantity or 0) * (old.average_fill_price if old.average_fill_price is not None else old.price)))
                        new_n = Decimal(record.cumulative_notional) if record.cumulative_notional is not None else Decimal(str((record.filled_quantity or 0) * (record.average_fill_price if record.average_fill_price is not None else record.price)))
                        fee_delta = Decimal(str(record.fees_paid or 0)) - Decimal(str(old.fees_paid or 0))
                        if new_n != old_n and record.filled_quantity:
                            through_sequence = conn.execute('SELECT COALESCE(MAX(sequence),0) FROM records WHERE stream=?', (f'fills:{pid}',)).fetchone()[0]
                            store._put(conn, f'corrected-costs:{pid}', key,
                                dict(price=float(new_n / Decimal(str(record.filled_quantity))), through_sequence=through_sequence), True)
                        if new_n != old_n or fee_delta:
                            store._append(conn, 'executions', dict(kind='correction', target_order_id=record.order_id, exchange=record.exchange, portfolio_id=pid, strategy=record.strategy, side=record.side.value, revision=record.revision, accounting_order=revision, notional_delta=str(new_n-old_n), fee_delta=str(fee_delta)))
                for fill in fills:
                    identity = (fill.exchange, fill.order_id, fill.filled_quantity)
                    if store._put(conn, f'accounting_fill_ids:{pid}', identity, True):
                        store._append(conn, f'fills:{pid}', fill)
                for name, values in memberships.items():
                    for value in values:
                        store._put(conn, f'{name}:{pid}', value, True)
                for key, amount in credits:
                    store._put(conn, f'cash_credits:{pid}', key, amount)
                conn.commit()
            await store.run(commit)
            async with self.lock:
                if self.history_revision != revision:
                    raise RuntimeError("Account changed during history commit")
                # Insertion order supplies a deterministic bounded diagnostic tail.
                terminal = [key for key, record in portfolio.order_history.items()
                            if record.status in TERMINAL_ORDER_STATUSES]
                for key in terminal[:-hot_limit]:
                    portfolio.order_history.pop(key, None)
                    portfolio.filled_by_order.pop(key, None)
                    portfolio.fill_notional_by_order.pop(key, None)
                    portfolio.fees_by_order.pop(key, None)
                del portfolio.strategy_fills[:len(fills)]
                keep_intents = {record.intent_id for record in portfolio.order_history.values()}
                keep_intents.update(portfolio.reservations)
                portfolio.processed_intents.intersection_update(keep_intents)
                keep_batches = {record.batch_id for record in portfolio.order_history.values() if record.batch_id}
                keep_batches.update(o.batch_id for o in portfolio.reservations.values() if o.batch_id)
                portfolio.processed_batches.intersection_update(keep_batches)
                portfolio.seen_executions.clear()
                for key in list(portfolio.cash_credits)[:-hot_limit]:
                    portfolio.cash_credits.pop(key)
                # Settled markets are bounded by subscriptions; retain a hot tail.
                if len(portfolio.settlements) > hot_limit:
                    portfolio.settlements = set(sorted(portfolio.settlements)[-hot_limit:])
                self._correction_projection = None
        self._history_committed_revision = self.history_revision

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
