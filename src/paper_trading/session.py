"""Paper-only session composition, shared by live feeds and offline replay."""
from copy import deepcopy
from dataclasses import replace
import json
import random
from pathlib import Path

from .clock import IdGenerator, VirtualClock
from .models import dec
from .paper_trading_engine import PaperTradingEngine
from .recording import Recorder
from .reporting import Reporter
from .scheduler import Scheduler
from ..market_decisions.order_coordinator import OrderCoordinator
from ..utilities.classes import (
    OrderBook, OrderBookSnapshotEvent, LiteOrderBookSnapshotEvent, OrderIntent, OrderStatus,
)
from ..utilities.market_events import PublicConnectionEvent, MarketStatusEvent, SettlementEvent
from ..utilities.trading_state import TradingState


class PaperSession:
    def __init__(self, config, clock, capture_directory=None, audit_directory=None):
        if config.paper_execution is None:
            raise ValueError("Explicit paper execution configuration is required")
        if any(value == "LIVE" for value in config.strategy_portfolios.values()) or any(
                not paper for paper in config.paper_trading.values()):
            raise ValueError("Paper sessions cannot route strategies to LIVE")
        self.clock = clock
        self.state = TradingState(config, clock)
        self.engine = PaperTradingEngine(config.paper_execution, clock)
        self.coordinator = OrderCoordinator(self.state, paper_engine=self.engine)
        self.reporter = Reporter(self.engine)
        self.recorder = Recorder(capture_directory, config, clock()) if capture_directory else None
        self.audit_file = None
        audit_directory = audit_directory or capture_directory
        if audit_directory:
            Path(audit_directory).mkdir(parents=True, exist_ok=True)
            self.engine.audit_path = Path(audit_directory) / "audit.jsonl"
            self.audit_file = self.engine.audit_path.open("x")
            self.engine.audit_sink = self._write_audit
        self.closed = False
        self.data_end = clock()
        self.strategy = None
        self.settled_markets = set()
        self.intent_ids = IdGenerator(config.paper_execution.session_id + "-intent")
        self.random = random.Random(config.paper_execution.seed)
        self._public_event_index = 0
        self._strategy_event = None
        self._replay_decisions = None

    def _write_audit(self, row):
        self.audit_file.write(json.dumps(row, default=str, sort_keys=True) + "\n")

    def new_intent(self, **values):
        """Strategy helper using session time and reproducible intent IDs."""
        values.setdefault("created_at", self.clock())
        if "intent_id" not in values:
            values["intent_id"] = self.intent_ids()
        return OrderIntent(**values)

    def record(self, kind, payload):
        if self.recorder:
            origin = "strategy" if self._strategy_event is not None else "driver"
            self.recorder.write(kind, self.clock(), payload, origin=origin, cause=self._strategy_event)

    async def add_market(self, market):
        if market.exchange not in self.engine.config.venues:
            raise ValueError("No venue profile for market")
        schedule = self.engine.config.venues[market.exchange].fee_schedule(market.market_id, self.clock())
        coefficient = max(dec(schedule.taker), dec(schedule.maker))
        fee_bound = coefficient if schedule.model == "flat" else coefficient / 4
        if market.fee_per_contract is None or dec(market.fee_per_contract) < fee_bound:
            market = replace(market, fee_per_contract=float(fee_bound))
        precision_rules = (market.tick_size, market.quantity_step, market.min_quantity)
        if any(value is None or dec(value) <= 0 for value in precision_rules):
            raise ValueError("Explicit price and quantity rules required")
        await self.state.update_trading_state(market)
        self.record("market", market)

    def publish(self, envelope):
        if self.closed:
            raise RuntimeError("Session is closed")
        available = max(envelope.available_at, self.clock())
        envelope = replace(deepcopy(envelope), available_at=available)
        self.engine.scheduler.schedule(available, Scheduler.PUBLIC, self._apply_public, envelope)
        self.record("public", envelope)

    async def apply_public_immediately(self, envelope):
        """Apply a shutdown/recovery input without running other queued work."""
        self.record("public_immediate", envelope)
        await self._apply_public(envelope)

    async def _apply_public(self, envelope):
        event = envelope.event
        self._public_event_index += 1
        self.data_end = max(self.data_end, self.clock())
        if isinstance(event, SettlementEvent):
            await self._handle_settlement(event)
            return
        if (event.exchange, getattr(event, "market_id", "")) in self.settled_markets:
            return
        try:
            await self.engine.on_public(envelope)
        except (ValueError, ArithmeticError) as error:
            pool = self.engine.books.get((event.exchange, getattr(event, "market_id", "")))
            if pool:
                pool.invalidate("malformed_market_data")
            self.engine.log("invalid_public_data", exchange=event.exchange, reason=str(error))
        async with self.state.lock:
            self._update_public_state(envelope)
            self.state._refresh_performance()
        self.reporter.observe()
        if (self.strategy or self._replay_decisions) and not isinstance(event, PublicConnectionEvent):
            previous_event = self._strategy_event
            self._strategy_event = self._public_event_index
            try:
                if self._replay_decisions:
                    await self._replay_decisions()
                else:
                    intents = await self.strategy.on_snapshot(await self.state.snapshot())
                    for intent in intents or []:
                        await self.submit(intent)
            finally:
                self._strategy_event = previous_event

    def _update_public_state(self, envelope):
        """Publish strategy-visible data together; caller holds the state lock."""
        event = envelope.event
        if isinstance(event, PublicConnectionEvent):
            for key in list(self.state.orderbooks):
                same_exchange = key.startswith(event.exchange + ":")
                same_market = not event.market_id or key.startswith(f"{event.exchange}:{event.market_id}:")
                if same_exchange and same_market:
                    self.state.orderbooks.pop(key, None)
                    self.state.market_quotes.pop(key, None)
        elif isinstance(event, OrderBookSnapshotEvent):
            self._update_outcome_books(event, envelope.received_at)
        elif isinstance(event, LiteOrderBookSnapshotEvent):
            key = f"{event.exchange}:{event.market_id}:{event.outcome}"
            self.state.market_quotes[key] = replace(event, timestamp=envelope.received_at)
        elif isinstance(event, MarketStatusEvent):
            market = self.state.markets.get(f"{event.exchange}:{event.market_id}")
            if market:
                market.active = event.status in {"OPEN", "MARKET_STATE_OPEN", "active", "open"}

    def _update_outcome_books(self, event, received_at):
        """Expose YES and NO views of the same canonical pool atomically."""
        pool = self.engine.books[(event.exchange, event.market_id)]
        for outcome in ("YES", "NO"):
            key = f"{event.exchange}:{event.market_id}:{outcome}"
            if not pool.valid:
                self.state.orderbooks.pop(key, None)
                continue
            bids = pool.observed["BUY"]
            asks = pool.observed["SELL"]
            if outcome == "NO":
                bids, asks = (
                    {1 - price: quantity for price, quantity in asks.items()},
                    {1 - price: quantity for price, quantity in bids.items()},
                )
            self.state.orderbooks[key] = OrderBook(
                market_id=event.market_id,
                exchange=event.exchange,
                outcome=outcome,
                bids={float(price): float(quantity) for price, quantity in bids.items()},
                asks={float(price): float(quantity) for price, quantity in asks.items()},
                timestamp=received_at,
            )
        market = self.state.markets.get(f"{event.exchange}:{event.market_id}")
        if market:
            market.active = pool.state in {"OPEN", "MARKET_STATE_OPEN", "active", "open"}

    async def _handle_settlement(self, event):
        try:
            if (not event.settlement_id or set(event.payouts) != {"YES", "NO"}
                    or any(not 0 <= dec(value) <= 1 for value in event.payouts.values())):
                raise ValueError("Explicit authoritative payouts are required")
        except (ValueError, ArithmeticError):
            self.engine.log("invalid_settlement", exchange=event.exchange, market_id=event.market_id)
            return

        # Stop venue activity immediately, but preserve private report delays.
        # Settlement cannot reveal a not-yet-observed fill early.
        account_available_at = self.clock()
        market_key = (event.exchange, event.market_id)
        for order in list(self.engine.orders.values()):
            if (order.approved.exchange, order.approved.market_id) == market_key:
                self.engine._finish(order, OrderStatus.EXPIRED, "market_settled")
                account_available_at = max(account_available_at, order.last_report_at or self.clock())
        async with self.state.lock:
            market = self.state.markets.get(f"{event.exchange}:{event.market_id}")
            if market:
                market.active = False
            self.state._refresh_performance()
        self.settled_markets.add(market_key)
        pool = self.engine.books.get(market_key)
        if pool:
            pool.invalidate("market_settled")
        self.engine.log("settlement", exchange=event.exchange, market_id=event.market_id,
                        settlement_id=event.settlement_id)
        self.engine.scheduler.schedule(max(account_available_at, self.clock()), Scheduler.REPORT,
                                       self._settle_account, event)

    async def _settle_account(self, event):
        await self.engine.retry_reports()
        async with self.state.lock:
            self.state._apply_settlement(event)
            self.state._refresh_performance()
        self.engine.log("settlement_accounted", exchange=event.exchange, market_id=event.market_id,
                        settlement_id=event.settlement_id)

    async def submit(self, intent):
        self.record("intent", intent)
        result = await self.coordinator.submit(intent)
        self.engine.log("risk_decision", intent_id=intent.intent_id, approved=result.decision.approved,
                        reason=result.decision.reason, status=result.status)
        return result

    async def submit_batch(self, batch):
        self.record("batch", batch)
        return await self.coordinator.submit_batch(batch)

    async def cancel(self, order_id, exchange, portfolio_id="PAPER"):
        self.record("cancel", dict(order_id=order_id, exchange=exchange, portfolio_id=portfolio_id))
        return await self.coordinator.cancel(order_id, exchange, portfolio_id)

    async def replace_order(self, order_id, intent):
        original = self.engine.orders.get(order_id)
        if not original or original.phase != "terminal":
            raise ValueError("Confirm cancellation before submitting a replacement")
        if original.record.status not in {OrderStatus.CANCELED, OrderStatus.EXPIRED}:
            raise ValueError("Replacement requires a canceled or expired original order")
        portfolio = self.state.portfolios[original.approved.portfolio_id]
        observed = portfolio.order_history.get(f"{original.approved.exchange}:{order_id}")
        if observed is None or observed.status != original.record.status:
            raise ValueError("Cancellation has not been observed yet")
        return await self.submit(intent)

    async def advance(self, until, inclusive=True):
        if self.closed:
            raise RuntimeError("Session is closed")
        self.record("advance", dict(until=until, inclusive=inclusive))
        await self.engine.scheduler.advance(until, inclusive)
        self.reporter.observe()

    async def finish(self, output_directory=None):
        if self.closed:
            return self.reporter.summary(self.state)
        ended_at = self.clock()
        if self.recorder:
            self.recorder.close(ended_at)
        # Only private outcomes already determined by input data may be flushed.
        # Do not execute later arrivals/expiry/trades beyond the capture.
        for report in self.engine.scheduler.take_events(Scheduler.REPORT):
            if isinstance(self.clock, VirtualClock):
                self.clock.advance(max(self.clock(), report.timestamp_ns / 1_000_000_000))
            result = report.callback(*report.args)
            if hasattr(result, "__await__"):
                await result
        await self.engine.retry_reports()
        self.reporter.observe()
        self.closed = True
        if self.audit_file:
            self.audit_file.flush()
            self.audit_file.close()
            self.engine.audit_sink = None
        if output_directory:
            return self.reporter.write(output_directory, self.state)
        return self.reporter.summary(self.state)


if __name__ == "__main__":
    from .cli import main
    main()
