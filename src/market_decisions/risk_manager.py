"""Pure sizing and risk evaluation; the coordinator owns reservations and I/O."""
from collections import defaultdict
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal, ROUND_FLOOR
from fractions import Fraction
from math import gcd, isfinite, lcm
import time

from ..utilities.classes import (
    ApprovedOrder, Exposure, OrderIntent, OrderIntentBatch, OrderType,
    MarketInfo, PortfolioState, Position, RiskConfig, RiskDecision, Side, Snapshot, TimeInForce,
)


def finite(value) -> bool:
    """Accept finite numbers, excluding bool (which Python treats as an int)."""
    return isinstance(value, (float, int)) and not isinstance(value, bool) and isfinite(value)


def position_key(exchange: str, market_id: str, outcome: str) -> str:
    return f"{exchange}:{market_id}:{outcome}"


def allocation_key(strategy: str, exchange: str, market_id: str, outcome: str) -> str:
    return f"{strategy}:{exchange}:{market_id}:{outcome}"


def round_quantity(quantity: float, step: float) -> float:
    """Round down to a venue increment without rounding a float upward."""
    quantity = Decimal(str(quantity))
    step = Decimal(str(step))
    whole_steps = (quantity / step).to_integral_value(rounding=ROUND_FLOOR)
    return float(whole_steps * step)


class RiskManager:
    def __init__(self, config: RiskConfig, portfolio_id="LIVE", clock=time.time):
        config.validate()
        self.config = config
        self.portfolio_id = portfolio_id
        self.clock = clock

    def kelly_criterion(self, bot_probability, price, resolution_date):
        if not finite(bot_probability) or not 0 <= bot_probability <= 1:
            raise ValueError("Probability must be in [0, 1]")
        if not finite(price) or not 0 < price < 1:
            raise ValueError("Kelly price must be strictly between 0 and 1")
        remaining = resolution_date - self.clock() if finite(resolution_date) else -1
        if remaining <= 0:
            raise ValueError("Resolution must be in the future")
        horizon = "long_term" if remaining >= self.config.long_term_expiration_minimum else "short_term"
        return self.config.kelly_criterion[horizon] * max(0.0, (bot_probability - price) / (1 - price))

    def quote(
        self, snapshot: Snapshot, exchange: str, market_id: str, outcome: str, side: Side,
    ) -> float | None:
        """Return the newest fresh executable quote from full or lite data."""
        key = position_key(exchange, market_id, outcome)
        candidates = []
        book = snapshot.orderbooks.get(key)
        if book:
            levels = book.asks if side == Side.BUY else book.bids
            if levels:
                candidates.append((book.timestamp, min(levels) if side == Side.BUY else max(levels)))
        quote = snapshot.market_quotes.get(key)
        if quote:
            price = quote.best_ask if side == Side.BUY else quote.best_bid
            if price is not None:
                candidates.append((quote.timestamp, price))
        now = self.clock()
        fresh_quotes = []
        for timestamp, price in candidates:
            if not finite(timestamp) or not finite(price):
                continue
            age = now - timestamp
            if 0 <= age <= self.config.max_quote_age_seconds and 0 <= price <= 1:
                fresh_quotes.append((timestamp, price))
        return max(fresh_quotes)[1] if fresh_quotes else None

    def equity(self, snapshot: Snapshot) -> float | None:
        """Value owned cash and inventory; None means the account is not usable."""
        portfolio = snapshot.portfolios[self.portfolio_id]
        total = 0.0
        if not portfolio.paper and portfolio.requires_reconciliation:
            return None
        if not portfolio.balances:
            return None
        for exchange, currencies in portfolio.balances.items():
            if set(currencies) != {self.config.currency}:
                return None
            balance = currencies[self.config.currency]
            if balance.cash is None or not finite(balance.cash) or balance.cash < 0:
                return None
            if not portfolio.paper and not (
                0 <= self.clock() - balance.timestamp <= self.config.max_account_age_seconds
            ):
                return None
            if not portfolio.paper and (
                exchange not in portfolio.positions_ready or exchange not in portfolio.orders_ready
            ):
                return None
            total += balance.cash
        for position in portfolio.positions.values():
            if position.exchange not in portfolio.balances:
                return None
            bid = self.quote(snapshot, position.exchange, position.market_id, position.outcome, Side.SELL)
            if bid is None:
                return None
            total += position.size * bid
        return total if finite(total) and total >= 0 else None

    def get_exposure(self, snapshot: Snapshot) -> Exposure:
        """Count account exposure once, with separate strategy sizing targets."""
        portfolio = snapshot.portfolios[self.portfolio_id]
        markets = defaultdict(float)
        categories = defaultdict(float)
        allocations = defaultdict(float)
        slots = set()

        def add_commitment(item, quantity, dollars, strategy):
            if quantity <= 0:
                return
            market_key = f"{item.exchange}:{item.market_id}"
            slots.add(market_key)
            markets[market_key] += dollars
            categories[item.category] += dollars
            allocation = allocation_key(strategy, item.exchange, item.market_id, item.outcome)
            allocations[allocation] += dollars

        for position in portfolio.positions.values():
            add_commitment(position, position.size, position.size * position.entry_price, "")
        # Attribution affects Kelly targets, but never increases account exposure.
        for strategy, positions in portfolio.strategy_positions.items():
            for position in positions.values():
                allocation = allocation_key(
                    strategy, position.exchange, position.market_id, position.outcome,
                )
                allocations[allocation] += position.size * position.entry_price
        for order in portfolio.pending_orders:
            if order.side == Side.BUY:
                add_commitment(order, order.remaining_quantity, order.commitment, order.strategy)
        return Exposure(
            total=sum(markets.values()), by_market=dict(markets),
            by_category=dict(categories), by_allocation=dict(allocations),
            market_slots=frozenset(slots),
        )

    def available_cash(self, portfolio: PortfolioState, exchange: str) -> float:
        balance = portfolio.balances.get(exchange, {}).get(self.config.currency)
        if balance is None:
            return 0.0
        reserved = sum(
            order.commitment for order in portfolio.reservations.values()
            if order.exchange == exchange
        )
        # Live buying power already includes venue holds. Paper cash does not.
        if not balance.buying_power_includes_holds:
            reserved += sum(
                order.commitment for order in portfolio.open_orders.values()
                if order.exchange == exchange and order.side == Side.BUY
            )
        return max(0.0, balance.buying_power - reserved)

    def _reject(self, reason):
        return RiskDecision(False, reason)

    def evaluate(
        self, intent: OrderIntent, snapshot: Snapshot, *,
        group_budget: float | None = None, group_quantity: float | None = None,
    ) -> RiskDecision:
        """Validate an opportunity, then size either an entry or a reduction.

        The group arguments are internal sizing overrides used by linked batches
        and by the coordinator when rechecking an existing approval.
        """
        portfolio = snapshot.portfolios[self.portfolio_id]
        now = self.clock()
        rejection = self._validate_signal(intent, portfolio, now)
        if rejection:
            return self._reject(rejection)

        market = snapshot.markets.get(f"{intent.exchange}:{intent.market_id}")
        rejection = self._validate_market(intent, market)
        if rejection:
            return self._reject(rejection)

        price = intent.price
        if intent.order_type == OrderType.MARKET:
            if type(intent.slippage) is not int or intent.slippage < 0:
                return self._reject("unbounded_market_order")
            direction = 1 if intent.side == Side.BUY else -1
            price = float(
                Decimal(str(price))
                + direction * intent.slippage * Decimal(str(market.tick_size))
            )
            if not 0 < price < 1:
                return self._reject("invalid_slippage_bound")

        quote = self.quote(snapshot, intent.exchange, intent.market_id, intent.outcome, intent.side)
        if quote is None:
            return self._reject("stale_quote")
        requires_immediate_fill = (
            intent.order_type == OrderType.MARKET
            or intent.tif in (TimeInForce.FOK, TimeInForce.IOC)
        )
        price_outside_bound = (
            (intent.side == Side.BUY and quote > price)
            or (intent.side == Side.SELL and quote < price)
        )
        if requires_immediate_fill and price_outside_bound:
            return self._reject("price_bound_violated")

        category = market.category if market.category in self.config.diversification else "other"
        committed_categories = self._committed_categories(intent, portfolio)
        if len(committed_categories) > 1 and intent.side == Side.BUY:
            return self._reject("market_category_conflict")
        if committed_categories:
            category = min(committed_categories)

        if intent.side == Side.SELL:
            return self._evaluate_exit(intent, snapshot, market, price, now)
        return self._evaluate_entry(
            intent, snapshot, market, price, category, now,
            group_budget=group_budget, group_quantity=group_quantity,
        )

    def _validate_signal(self, intent: OrderIntent, portfolio: PortfolioState, now: float) -> str | None:
        if not intent.intent_id or intent.intent_id in portfolio.processed_intents:
            return "duplicate_intent"
        if (not intent.strategy or intent.outcome not in ("YES", "NO")
                or intent.side not in (Side.BUY, Side.SELL)):
            return "invalid_identity"
        if (not finite(intent.created_at)
                or not 0 <= now - intent.created_at <= self.config.max_signal_age_seconds):
            return "stale_signal"
        if intent.expires_at is not None and (
            not finite(intent.expires_at) or intent.expires_at <= now
            or intent.expires_at < intent.created_at
        ):
            return "expired_signal"
        if not finite(intent.price) or not 0 < intent.price < 1:
            return "invalid_price"
        if intent.tif not in tuple(TimeInForce) or intent.order_type not in tuple(OrderType):
            return "invalid_execution_instruction"
        if intent.tif == TimeInForce.GTD and (
            not finite(intent.good_till_date) or intent.good_till_date <= now
        ):
            return "invalid_order_expiry"
        return None

    def _validate_market(self, intent: OrderIntent, market: MarketInfo | None) -> str | None:
        if market is None or not market.active or not market.tradable.get(intent.outcome, False):
            return "market_unavailable"
        if market.currency != self.config.currency:
            return "unsupported_currency"
        rules = (
            market.tick_size, market.quantity_step, market.min_quantity,
            market.min_notional, market.fee_per_contract,
        )
        if (any(not finite(value) or value < 0 for value in rules)
                or not market.tick_size or not market.quantity_step or not market.min_quantity):
            return "unknown_market_rules"
        ticks = Decimal(str(intent.price)) / Decimal(str(market.tick_size))
        if ticks != ticks.to_integral_value():
            return "invalid_price_tick"
        return None

    def _committed_categories(self, intent, portfolio) -> set[str]:
        """Keep a market's category fixed while any inventory or entries remain."""
        categories = {
            position.category for position in portfolio.positions.values()
            if position.exchange == intent.exchange and position.market_id == intent.market_id
        }
        for order in portfolio.pending_orders:
            if (order.exchange == intent.exchange and order.market_id == intent.market_id
                    and order.side == Side.BUY):
                categories.add(order.category)
        return categories

    def _evaluate_exit(self, intent, snapshot, market, price, now) -> RiskDecision:
        portfolio = snapshot.portfolios[self.portfolio_id]
        if not intent.reduce_only or not finite(intent.close_fraction) or not 0 < intent.close_fraction <= 1:
            return self._reject("exit_must_reduce_inventory")

        key = position_key(intent.exchange, intent.market_id, intent.outcome)
        owned = portfolio.strategy_positions.get(intent.strategy, {}).get(key)
        held = portfolio.positions.get(key)
        inventory_age = now - portfolio.positions_updated_at.get(intent.exchange, float("-inf"))
        inventory_unavailable = (
            intent.exchange not in portfolio.positions_ready
            or intent.exchange in portfolio.disconnected_exchanges
            or intent.exchange in portfolio.dirty_exchanges
            or not 0 <= inventory_age <= self.config.max_account_age_seconds
        )
        if not portfolio.paper and inventory_unavailable:
            return self._reject("inventory_requires_reconciliation")
        if not owned or not held:
            return self._reject("no_owned_inventory")

        def matches_exit(order):
            return (
                order.exchange == intent.exchange and order.market_id == intent.market_id
                and order.outcome == intent.outcome and order.side == Side.SELL
            )

        pending_exits = [order for order in portfolio.reservations.values() if matches_exit(order)]
        open_exits = [order for order in portfolio.open_orders.values() if matches_exit(order)]
        total_reserved = (
            sum(order.size for order in pending_exits)
            + sum(order.remaining_quantity for order in open_exits)
        )
        owned_reserved = (
            sum(order.size for order in pending_exits if order.strategy == intent.strategy)
            + sum(order.remaining_quantity for order in open_exits if order.strategy == intent.strategy)
        )
        available_inventory = max(0, held.size - total_reserved)
        requested_inventory = max(0, owned.size * intent.close_fraction - owned_reserved)
        quantity = min(available_inventory, requested_inventory)
        return self._approve(intent, market, price, owned.category, quantity, now)

    def _evaluate_entry(
        self, intent, snapshot, market, price, category, now, *, group_budget, group_quantity,
    ) -> RiskDecision:
        portfolio = snapshot.portfolios[self.portfolio_id]
        if intent.reduce_only:
            return self._reject("reduce_only_buy_unsupported")
        if portfolio.entries_halted or portfolio.requires_reconciliation:
            return self._reject("account_requires_reconciliation")
        if not portfolio.paper and self._has_opposite_commitment(intent, portfolio):
            return self._reject("opposite_inventory_conflict")

        equity = self.equity(snapshot)
        if equity is None:
            return self._reject("account_or_valuation_unavailable")
        drawdown_limit = self.config.max_drawdown_pct
        high_water = portfolio.equity_high_water
        if (drawdown_limit is not None and 0 <= equity < high_water
                and equity <= high_water * (1 - drawdown_limit / 100)):
            return self._reject("drawdown_limit")
        capital = self._capital_base(equity)
        resolution = market.resolution_date if market.resolution_date is not None else intent.resolution_date
        if not finite(resolution) or resolution <= now:
            return self._reject("invalid_resolution_date")

        exposure = self.get_exposure(snapshot)
        market_key = f"{intent.exchange}:{intent.market_id}"
        is_new_market = market_key not in exposure.market_slots
        if is_new_market and len(exposure.market_slots) >= self.config.max_open_positions:
            return self._reject("position_count_limit")

        sizing = {}
        if group_budget is None:
            try:
                fraction = self.kelly_criterion(
                    intent.estimated_probability, price + market.fee_per_contract, resolution,
                )
            except ValueError:
                return self._reject("invalid_probability")
            target = capital * fraction
            allocation = allocation_key(intent.strategy, intent.exchange, intent.market_id, intent.outcome)
            existing = exposure.by_allocation.get(allocation, 0.0)
            additional = max(0.0, target - existing)
            sizing = {"equity": equity, "capital": capital, "kelly_fraction": fraction,
                      "target": target, "existing": existing}
        else:
            additional = group_budget

        category_limit = capital * self.config.diversification.get(category, 0.0)
        budgets = {
            "kelly_target" if group_budget is None else "group_ceiling": additional,
            "buying_power": self.available_cash(portfolio, intent.exchange),
            "total_exposure": max(0, self.config.max_total_exposure - exposure.total),
            "market_exposure": max(
                0, self.config.max_position_exposure - exposure.by_market.get(market_key, 0),
            ),
            "category_exposure": max(0, category_limit - exposure.by_category.get(category, 0)),
        }
        limiting_budget = min(budgets, key=budgets.get)
        available_dollars = budgets[limiting_budget]
        cost_per_contract = price + market.fee_per_contract
        quantity = available_dollars / cost_per_contract
        sizing.update(budgets)
        if group_quantity is not None:
            if group_quantity * cost_per_contract > available_dollars + 1e-9:
                return self._reject(f"batch_{limiting_budget}")
            quantity = group_quantity
        return self._approve(
            intent, market, price, category, quantity, now,
            sizing=sizing, limiting_budget=limiting_budget,
        )

    def _has_opposite_commitment(self, intent, portfolio) -> bool:
        opposite = "NO" if intent.outcome == "YES" else "YES"
        if position_key(intent.exchange, intent.market_id, opposite) in portfolio.positions:
            return True
        return any(
            order.exchange == intent.exchange and order.market_id == intent.market_id
            and order.outcome == opposite and order.side == Side.BUY
            for order in portfolio.pending_orders
        )

    def _capital_base(self, equity: float) -> float:
        if self.config.capital_allocation is None:
            return equity
        return min(equity, self.config.capital_allocation)

    def _approve(
        self, intent, market, price, category, quantity, now, *,
        sizing=None, limiting_budget="inventory",
    ) -> RiskDecision:
        quantity = round_quantity(quantity, market.quantity_step)
        below_minimum = (
            quantity <= 0
            or quantity + 1e-12 < market.min_quantity
            or quantity * price + 1e-12 < market.min_notional
        )
        if below_minimum:
            return self._reject(f"below_minimum:{limiting_budget}")
        valid_until = min(
            now + self.config.approval_ttl_seconds,
            intent.created_at + self.config.max_signal_age_seconds,
            intent.expires_at or float("inf"),
        )
        order = ApprovedOrder(
            intent=intent, portfolio_id=self.portfolio_id, category=category,
            size=quantity, price=price, fee_per_contract=market.fee_per_contract,
            approved_until=valid_until,
        )
        return RiskDecision(True, "approved", [order], sizing or {}, limiting_budget)

    def evaluate_batch(self, batch: OrderIntentBatch, snapshot: Snapshot):
        if (not batch.batch_id or not batch.legs or len(batch.legs) != len(batch.quantity_ratios)
                or len({o.intent_id for o in batch.legs}) != len(batch.legs)
                or any(type(r) is not int or r <= 0 for r in batch.quantity_ratios)
                or not finite(batch.capital_ceiling) or batch.capital_ceiling <= 0):
            return self._reject("invalid_batch")
        if batch.expires_at is not None and (
            not finite(batch.expires_at) or batch.expires_at <= self.clock()
        ):
            return self._reject("expired_batch")
        if any(o.side != Side.BUY or o.reduce_only for o in batch.legs):
            return self._reject("batch_entries_only")
        if batch.batch_id in snapshot.portfolios[self.portfolio_id].processed_batches:
            return self._reject("duplicate_batch")
        leg_limits = []
        for leg in batch.legs:
            decision = self.evaluate(leg, snapshot, group_budget=batch.capital_ceiling)
            if not decision.approved:
                return decision
            leg_limits.append(decision.orders[0])
        # Smallest common scale that preserves every venue's quantity increment.
        scale_increment = self._batch_scale_increment(batch, leg_limits, snapshot)
        cost_per_group = sum(
            order.price * ratio + order.fee_per_contract * ratio
            for order, ratio in zip(leg_limits, batch.quantity_ratios)
        )
        max_scale = min(
            batch.capital_ceiling / cost_per_group,
            *(order.size / ratio for order, ratio in zip(leg_limits, batch.quantity_ratios)),
        )
        exposure = self.get_exposure(snapshot)
        portfolio = snapshot.portfolios[self.portfolio_id]
        equity = self.equity(snapshot)
        capital = self._capital_base(equity)
        group_costs = defaultdict(float)
        for order, ratio in zip(leg_limits, batch.quantity_ratios):
            cost = ratio * (order.price + order.fee_per_contract)
            group_costs[("exchange", order.exchange)] += cost
            group_costs[("market", f"{order.exchange}:{order.market_id}")] += cost
            group_costs[("category", order.category)] += cost
        max_scale = min(max_scale, max(0, self.config.max_total_exposure - exposure.total) / cost_per_group)
        for (budget_type, key), cost in group_costs.items():
            if budget_type == "exchange":
                budget = self.available_cash(portfolio, key)
            elif budget_type == "market":
                budget = max(0, self.config.max_position_exposure - exposure.by_market.get(key, 0))
            else:
                category_limit = capital * self.config.diversification.get(key, 0.0)
                budget = max(0, category_limit - exposure.by_category.get(key, 0))
            max_scale = min(max_scale, budget / cost)
        scale = (Fraction(str(max_scale)) // scale_increment) * scale_increment
        if scale <= 0:
            return self._reject("batch_below_minimum")
        projected = deepcopy(snapshot)
        orders = []
        for leg_limit, ratio in zip(leg_limits, batch.quantity_ratios):
            quantity = float(scale * ratio)
            decision = self.evaluate(
                leg_limit.intent, projected,
                group_budget=batch.capital_ceiling, group_quantity=quantity,
            )
            if not decision.approved:
                return decision
            order = decision.orders[0]
            if abs(order.size - quantity) > 1e-9:
                return self._reject("batch_ratio_rounding")
            order = replace(
                order, batch_id=batch.batch_id,
                approved_until=min(order.approved_until, batch.expires_at or float("inf")),
            )
            orders.append(order)
            projected.portfolios[self.portfolio_id].reservations[order.intent_id] = order
        return RiskDecision(True, "approved", orders, {"scale": float(scale)}, "group_budget")

    def _batch_scale_increment(self, batch, leg_limits, snapshot) -> Fraction:
        """Find the smallest group increment that fits every venue's size step.

        Divide each venue step by its leg ratio, then take the common multiple.
        Fractions keep repeating scales (such as 0.1 / 3) exact until sizing ends.
        """
        scale_steps = []
        for order, ratio in zip(leg_limits, batch.quantity_ratios):
            market = snapshot.markets[f"{order.exchange}:{order.market_id}"]
            scale_steps.append(Fraction(str(market.quantity_step)) / ratio)
        return Fraction(
            lcm(*(step.numerator for step in scale_steps)),
            gcd_all(step.denominator for step in scale_steps),
        )

    def check_trade_allowed(self, order, snapshot):
        return self.evaluate(order, snapshot).approved

    def check_open_exposure(self, snapshot):
        return self.get_exposure(snapshot).total <= self.config.max_total_exposure

    def check_stop_loss(self, position: Position, current_price):
        if not finite(current_price) or not 0 <= current_price <= 1 or position.entry_price <= 0:
            return False
        stop_price = position.entry_price * (1 - self.config.stop_loss_pct / 100)
        return current_price <= stop_price

    def stop_loss_intents(self, snapshot):
        intents = []
        portfolio = snapshot.portfolios[self.portfolio_id]
        for strategy, positions in portfolio.strategy_positions.items():
            for position in positions.values():
                bid = self.quote(snapshot, position.exchange, position.market_id, position.outcome, Side.SELL)
                if bid is not None and bid > 0 and self.check_stop_loss(position, bid):
                    market = snapshot.markets.get(f"{position.exchange}:{position.market_id}")
                    if not market or not market.tick_size:
                        continue
                    intents.append(OrderIntent(
                        market_id=position.market_id, exchange=position.exchange,
                        outcome=position.outcome, side=Side.SELL,
                        price=round_quantity(bid, market.tick_size), strategy=strategy,
                        reduce_only=True, created_at=self.clock(), reason="stop_loss",
                    ))
        return intents


def gcd_all(values):
    result = 0
    for value in values:
        result = gcd(result, value)
    return result
