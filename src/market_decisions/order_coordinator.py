"""The sole submission gateway. All fill sources share the same ledger."""
import asyncio
import math
from copy import deepcopy
from dataclasses import replace

from ..paper_trading.paper_trading_engine import PaperTradingEngine
from ..paper_trading.fees import FeeReserveProvider
from ..utilities.classes import (
    OrderIntent,
    OrderIntentBatch,
    OrderNotSubmittedError,
    OrderStatus,
    RiskDecision,
    SubmissionResult,
    TERMINAL_ORDER_STATUSES,
)
from ..utilities.logger import setup_logger

logger = setup_logger(__name__)


class OrderCoordinator:
    def __init__(self, state, clients=None, paper_engine=None):
        self.state = state
        self.clients = clients or {}
        self.paper_engine = paper_engine or PaperTradingEngine(state.config.paper_execution, state.clock)
        if getattr(self.paper_engine, "asynchronous", False):
            self.paper_engine.bind(self)
            if self.paper_engine.config:
                for portfolio_id, manager in state.risk_managers.items():
                    if state.portfolios[portfolio_id].paper:
                        manager.fee_reserve_provider = FeeReserveProvider(self.paper_engine.config, state.clock)
                        manager.netted_exchanges = {
                            name for name, rules in self.paper_engine.config.venues.items()
                            if rules.netted_outcomes
                        }
        # This lock serializes submissions and account events. The separate state
        # lock is released during network calls so market data can keep updating.
        self.lock = asyncio.Lock()

    def _rejected(self, reason: str) -> SubmissionResult:
        return SubmissionResult(RiskDecision(False, reason))

    async def submit(self, intent: OrderIntent) -> SubmissionResult:
        async with self.lock:
            portfolio_id = self.state.portfolio_for_strategy(intent.strategy)
            if portfolio_id is None:
                return self._rejected("unknown_strategy_route")
            portfolio = self.state.portfolios[portfolio_id]
            if portfolio.paper and getattr(self.paper_engine, "asynchronous", False):
                if not self.paper_engine.config or intent.exchange not in self.paper_engine.config.venues:
                    return self._rejected("missing_paper_venue_profile")
            if not portfolio.paper and intent.exchange not in self.clients:
                return self._rejected("missing_exchange_client")

            async with self.state.lock:
                self.state._refresh_performance()
                manager = self.state.risk_managers[portfolio_id]
                decision = manager.evaluate(intent, self.state._snapshot())
                if not decision.approved:
                    return SubmissionResult(decision)
                self.state._reserve(decision)
            return await self._send_reserved(decision)

    async def submit_many(self, intents: list[OrderIntent]) -> list[SubmissionResult]:
        """Submit independent opportunities in list order, reserving between them."""
        results = []
        for intent in intents:
            results.append(await self.submit(intent))
        return results

    def _revalidate(self, order) -> bool:
        """Recheck an approval without counting its own reservation against it."""
        if self.state.clock() >= order.approved_until:
            return False
        snapshot = self.state._snapshot()
        portfolio = snapshot.portfolios[order.portfolio_id]
        portfolio.processed_intents.discard(order.intent_id)
        portfolio.reservations.pop(order.intent_id, None)
        manager = self.state.risk_managers[order.portfolio_id]
        decision = manager.evaluate(
            order.intent, snapshot,
            group_budget=order.commitment, group_quantity=order.size,
        )
        return decision.approved and decision.orders[0].size + 1e-9 >= order.size

    async def _send_reserved(self, decision: RiskDecision) -> SubmissionResult:
        order = decision.orders[0]
        async with self.state.lock:
            if not self._revalidate(order):
                self.state._release(order)
                return self._rejected("approval_no_longer_valid")
            portfolio = self.state.portfolios[order.portfolio_id]
            paper = portfolio.paper
            snapshot = self.state._snapshot()
            market = snapshot.markets[f"{order.exchange}:{order.market_id}"]
            manager = self.state.risk_managers[order.portfolio_id]
            quote = manager.quote(
                snapshot, order.exchange, order.market_id, order.outcome, order.side,
            )

        try:
            if paper:
                if getattr(self.paper_engine, "asynchronous", False):
                    identifier = self.paper_engine.enqueue(order, market)
                    return SubmissionResult(decision, status="pending", submission_ids=[identifier])
                incoming = await self.paper_engine.simulate_fill(order, market, quote)
            else:
                incoming = await self.clients[order.exchange].place_order(order)
            if incoming is None:
                # Only a rejection record proves the venue rejected a submission.
                raise RuntimeError("Submission returned no identifiable order")

            async with self.state.lock:
                expected = (order.exchange, order.market_id, order.outcome, order.side)
                reported = (incoming.exchange, incoming.market_id, incoming.outcome, incoming.side)
                if reported != expected or abs(incoming.size - order.size) > 1e-9:
                    raise ValueError("Exchange response does not match approved order")
                record = self.state._apply_order_update(incoming, approved=order)
                self.state._refresh_performance()
            status = "exchange_rejected" if record.status == OrderStatus.REJECTED else "submitted"
            return SubmissionResult(decision, [deepcopy(record)], status)
        except BaseException as error:
            # Cancellation also needs accounting cleanup before it propagates.
            async with self.state.lock:
                if paper or isinstance(error, OrderNotSubmittedError):
                    self.state._release(order)
                else:
                    portfolio.unresolved_intents.add(order.intent_id)
            if isinstance(error, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            if isinstance(error, OrderNotSubmittedError):
                return self._rejected("submission_not_sent")
            logger.exception("Submission failed for intent %s", order.intent_id)
            status = "failed" if paper else "unknown"
            return SubmissionResult(decision, status=status)

    async def submit_batch(self, batch: OrderIntentBatch) -> SubmissionResult:
        async with self.lock:
            routes = {self.state.portfolio_for_strategy(leg.strategy) for leg in batch.legs}
            if len(routes) != 1 or None in routes:
                return self._rejected("batch_requires_one_portfolio")
            portfolio_id = next(iter(routes))
            portfolio = self.state.portfolios[portfolio_id]
            asynchronous = getattr(self.paper_engine, "asynchronous", False)
            supported = "paper_independent_legs" if asynchronous else "paper_all_or_none"
            if not portfolio.paper or batch.execution_policy != supported:
                return self._rejected("unsupported_batch_execution_policy")
            if asynchronous and (batch.dispatch_policy not in {"concurrent", "sequential"}
                    or batch.execution_deadline is None
                    or not math.isfinite(batch.execution_deadline)
                    or batch.execution_deadline <= self.state.clock()):
                return self._rejected("invalid_batch_execution_deadline_or_policy")

            async with self.state.lock:
                self.state._refresh_performance()
                manager = self.state.risk_managers[portfolio_id]
                snapshot = self.state._snapshot()
                decision = manager.evaluate_batch(batch, snapshot)
                if not decision.approved:
                    return SubmissionResult(decision)
                self.state._reserve(decision)
            try:
                if asynchronous:
                    # Local queue insertion cannot dispatch until locks release.
                    order_ids = self.paper_engine.enqueue_batch(batch, decision.orders, snapshot.markets)
                    return SubmissionResult(decision, status="pending", submission_ids=order_ids)
                records = await self._simulate_paper_batch(decision, snapshot, manager)
                async with self.state.lock:
                    applied = self._apply_paper_batch(decision, records, portfolio_id)
                    self.state._refresh_performance()
                return SubmissionResult(decision, deepcopy(applied), "submitted")
            except BaseException as error:
                async with self.state.lock:
                    for order in decision.orders:
                        self.state._release(order)
                if isinstance(error, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                    raise
                logger.exception("Paper batch %s failed", batch.batch_id)
                return SubmissionResult(decision, status="failed")

    async def _simulate_paper_batch(self, decision, snapshot, manager):
        records = []
        for order in decision.orders:
            market = snapshot.markets[f"{order.exchange}:{order.market_id}"]
            quote = manager.quote(
                snapshot, order.exchange, order.market_id, order.outcome, order.side,
            )
            records.append(await self.paper_engine.simulate_fill(order, market, quote))
        return records

    def _apply_paper_batch(self, decision, records, portfolio_id):
        """Apply every leg or restore the portfolio; caller holds the state lock."""
        if any(not self._revalidate(order) for order in decision.orders):
            raise ValueError("Batch approval expired or changed")
        backup = deepcopy(self.state.portfolios[portfolio_id])
        try:
            applied = []
            for order, record in zip(decision.orders, records):
                applied.append(self.state._apply_order_update(record, approved=order))
            return applied
        except BaseException:
            self.state.portfolios[portfolio_id] = backup
            raise

    async def apply_order_update(
        self, order, *, fill_quantity=None, fill_price=None, execution_id=None,
    ):
        async with self.lock:
            async with self.state.lock:
                result = self.state._apply_order_update(
                    order, fill_quantity=fill_quantity,
                    fill_price=fill_price, execution_id=execution_id,
                )
                self.state._refresh_performance()
                return deepcopy(result)

    async def apply_paper_update(self, record, approved):
        """Report delivery is outside engine and portfolio critical sections."""
        async with self.lock:
            async with self.state.lock:
                if not self.state.portfolios[approved.portfolio_id].paper:
                    raise ValueError("Paper reports cannot mutate a live account")
                expected = (approved.exchange, approved.market_id, approved.outcome,
                            approved.side, approved.size, approved.intent_id)
                actual = (record.exchange, record.market_id, record.outcome,
                          record.side, record.size, record.intent_id)
                if actual != expected:
                    raise ValueError("Paper response does not match approved order")
                result = self.state._apply_order_update(record, approved=approved)
                self.state._refresh_performance()
                return deepcopy(result)

    async def apply_paper_credit(self, credit):
        async with self.lock:
            async with self.state.lock:
                self.state._apply_cash_credit(credit)
                self.state._refresh_performance()

    async def resolve_paper_submission(self, order_id):
        record = self.paper_engine.get_order(order_id)
        if record is None:
            return None
        approved = self.paper_engine.orders[order_id].approved
        return await self.apply_paper_update(record, approved)

    async def apply_order_snapshot(self, orders, exchange, complete=True):
        """A missing order is not evidence of cancellation or lack of fills."""
        if not complete:
            return
        async with self.lock:
            async with self.state.lock:
                portfolio = self.state.portfolios["LIVE"]
                for order in orders:
                    if order.exchange != exchange:
                        raise ValueError("Order snapshot exchange mismatch")
                    self.state._apply_order_update(order)
                portfolio.orders_ready.add(exchange)

    async def cancel(self, order_id, exchange, portfolio_id="LIVE") -> bool:
        async with self.lock:
            async with self.state.lock:
                portfolio = self.state.portfolios[portfolio_id]
                if portfolio.paper and getattr(self.paper_engine, "asynchronous", False):
                    working = self.paper_engine.orders.get(order_id)
                    if not working or (working.approved.exchange, working.approved.portfolio_id) != (exchange, portfolio_id):
                        return False
                    # True means request queued; terminal confirmation arrives later.
                    return self.paper_engine.enqueue_cancel(order_id)
                order = deepcopy(portfolio.open_orders.get(f"{exchange}:{order_id}"))
            if order is None:
                return False
            if portfolio.paper:
                confirmed = replace(order, status=OrderStatus.CANCELED)
            else:
                client = self.clients[exchange]
                await client.cancel_order(order_id, order.market_id)
                # HTTP cancellation acknowledgment alone is insufficient.
                confirmed = await client.get_order(order_id)
            if confirmed is None:
                return False
            async with self.state.lock:
                self.state._apply_order_update(confirmed)
                if not portfolio.paper:
                    portfolio.dirty_exchanges.add(exchange)
            return confirmed.status in TERMINAL_ORDER_STATUSES

    async def reconcile(self, exchange=None):
        """Refresh one live venue at a time; retain unknown submissions."""
        async with self.lock:
            exchanges = [exchange] if exchange else list(self.clients)
            for venue in exchanges:
                await self._reconcile_exchange(venue)

    async def _reconcile_exchange(self, exchange):
        async with self.state.lock:
            portfolio = self.state.portfolios["LIVE"]
            portfolio.dirty_exchanges.add(exchange)
            revision = portfolio.account_revisions.get(exchange, 0)
            existing_orders = [
                deepcopy(order) for order in portfolio.open_orders.values()
                if order.exchange == exchange
            ]

        client = self.clients[exchange]
        orders = await client.get_open_orders()
        open_order_ids = {order.order_id for order in orders}
        has_missing_orders = False
        for previous in existing_orders:
            if previous.order_id in open_order_ids:
                continue
            confirmed = await client.get_order(previous.order_id)
            if confirmed is None:
                has_missing_orders = True
            else:
                orders.append(confirmed)
        # Read account inventory after fill counters, then cash. Private updates
        # during these reads invalidate the result instead of mixing versions.
        positions = await client.get_positions()
        balances = await client.get_balance()

        async with self.state.lock:
            if portfolio.account_revisions.get(exchange, 0) != revision:
                raise RuntimeError(f"{exchange} account changed during reconciliation; retry")
            for order in orders:
                self.state._apply_order_update(order)
            self.state._replace_live_positions(positions, exchange)
            if not self.state._replace_balances(balances, exchange):
                raise RuntimeError(f"{exchange} reconciliation returned older balances; retry")
            portfolio.orders_ready.add(exchange)
            has_unknown_orders = any(
                order.exchange == exchange and order.status == OrderStatus.UNKNOWN
                for order in portfolio.open_orders.values()
            )
            if not has_missing_orders and not has_unknown_orders:
                portfolio.dirty_exchanges.discard(exchange)
            if self._ownership_exceeds_inventory(portfolio, exchange):
                portfolio.dirty_exchanges.add(exchange)
            self.state._refresh_performance()

    def _ownership_exceeds_inventory(self, portfolio, exchange) -> bool:
        """Compare gross strategy ownership with confirmed account inventory."""
        for strategy_positions in portfolio.strategy_positions.values():
            for key, position in strategy_positions.items():
                if position.exchange != exchange:
                    continue
                owned_quantity = sum(
                    positions[key].size
                    for positions in portfolio.strategy_positions.values()
                    if key in positions
                )
                held = portfolio.positions.get(key)
                if held is None or owned_quantity > held.size + 1e-9:
                    return True
        return False

    async def resolve_unknown(
        self, intent_id, *, order=None, confirmed_absent=False, portfolio_id="LIVE",
    ):
        """Recover only after auditing venue history; open-order absence is not proof.

        Supply the confirmed exchange order, or explicitly confirm its absence
        after an audit. This SDK has no client-order-ID lookup for recovery.
        """
        async with self.lock:
            async with self.state.lock:
                portfolio = self.state.portfolios[portfolio_id]
                approved = portfolio.reservations.get(intent_id)
                if approved is None or intent_id not in portfolio.unresolved_intents:
                    raise ValueError("Intent is not unresolved")
                if order is not None:
                    expected = (
                        approved.exchange, approved.market_id, approved.outcome,
                        approved.side, approved.size,
                    )
                    reported = (
                        order.exchange, order.market_id, order.outcome,
                        order.side, order.size,
                    )
                    if reported != expected:
                        raise ValueError("Recovery order does not match approval")
                    self.state._apply_order_update(order, approved=approved)
                elif confirmed_absent:
                    self.state._release(approved)
                else:
                    raise ValueError("Provide a confirmed order or audited absence")
