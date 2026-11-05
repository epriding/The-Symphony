"""Event-driven paper exchange; all portfolio mutations use the coordinator."""
from collections import Counter, deque
from copy import deepcopy
from dataclasses import dataclass, field, asdict
from decimal import Decimal
import random

from .clock import IdGenerator, RealClock
from .fees import FeeAccumulator, FeeModel
from .matching import PassiveQueue, depth_allocations
from .models import CashCredit, Execution, dec
from .orderbook import BookPool, canonical_order
from .scheduler import Scheduler
from .venue_rules import RequestBudget
from ..utilities.classes import (
    ApprovedOrder, MarketInfo,
    OrderBookSnapshotEvent, LiteOrderBookSnapshotEvent, TradeEvent,
    OrderRecord, OrderStatus, Side, TimeInForce, OrderType, TERMINAL_ORDER_STATUSES,
)
from ..utilities.market_events import PublicConnectionEvent, MarketStatusEvent
from ..utilities.trading_rules import valid_price


@dataclass
class WorkingOrder:
    approved: ApprovedOrder
    market: MarketInfo
    record: OrderRecord
    phase: str = 'pending'
    fees: FeeAccumulator = field(default_factory=FeeAccumulator)
    notional: Decimal = Decimal(0)
    charged: Decimal = Decimal(0)
    filled: Decimal = Decimal(0)
    canonical_side: str = ''
    canonical_price: Decimal = Decimal(0)
    decision_quote: float | None = None
    arrival_quote: float | None = None
    decision_mid: float | None = None
    submitted_at: float = 0.0
    dispatched_at: float | None = None
    arrived_at: float | None = None
    first_fill_at: float | None = None
    last_fill_at: float | None = None
    terminal_at: float | None = None
    cancel_requested_at: float | None = None
    last_report_at: float | None = None

    @property
    def remaining(self):
        return dec(self.approved.size) - self.filled


@dataclass(frozen=True)
class FillCharge:
    """Fee calculation prepared without mutating the working order."""
    fee: Decimal
    credit: Decimal
    accumulator: FeeAccumulator
    fee_version: str


class PaperTradingEngine:
    asynchronous = True

    def __init__(self, config=None, clock=None, history_store=None, limits=None):
        self.config = config
        self.history_store = history_store
        self.limits = limits or {}
        self.history_failed = False
        self.clock = clock or RealClock()
        self.scheduler = Scheduler(self.clock, config.max_pending_events if config else 100_000)
        self.ids = IdGenerator(config.session_id if config else 'paper')
        self.rng = random.Random(config.seed if config else 0)
        self.orders = {}
        self.books = {}
        self.queues = {}
        self.groups = {}
        self.budgets = {name: RequestBudget(rules, self.clock())
                        for name, rules in config.venues.items()} if config else {}
        self.coordinator = None
        self.outbox = deque()
        self.credit_outbox = deque()
        self.executions = []
        self.audit = deque(maxlen=config.max_pending_events if config else 100_000)
        self.diagnostics = Counter()
        self.audit_sink = None
        self.audit_path = None
        self.seen_trades = set()

    def bind(self, coordinator):
        self.coordinator = coordinator

    def log(self, event, **values):
        row = dict(timestamp=self.clock(), event=event, **values)
        self.audit.append(row)
        self.diagnostics[event] += 1
        if self.audit_sink:
            self.audit_sink(row)

    def enqueue(self, approved, market):
        if self.history_failed:
            raise RuntimeError('history_unavailable')
        if self.history_store and sum(o.phase != 'terminal' for o in self.orders.values()) >= self.limits.get('max_active_orders', 64):
            raise BufferError('max_active_orders_exceeded')
        if self.config is None or approved.exchange not in self.config.venues:
            raise ValueError('missing_paper_venue_profile')
        order_id = self.ids()
        record = OrderRecord(
            order_id=order_id,
            market_id=approved.market_id,
            exchange=approved.exchange,
            outcome=approved.outcome,
            side=approved.side,
            size=approved.size,
            price=approved.price,
            strategy=approved.strategy,
            tif=approved.tif,
            filled_quantity=0,
            portfolio_id=approved.portfolio_id,
            category=approved.category,
            intent_id=approved.intent_id,
            batch_id=approved.batch_id,
            fee_per_contract=approved.fee_per_contract,
            risk_price=approved.price,
            fees_paid=0,
            cumulative_notional='0',
            remaining_fee_reserve=approved.fee_reserve,
        )
        side, price = canonical_order(approved.outcome, approved.side.value, approved.price)
        pool = self.books.get((approved.exchange, approved.market_id))
        quote = pool.quote(approved.side.value, approved.outcome) if pool else None
        bid = pool.quote('SELL', approved.outcome) if pool else None
        ask = pool.quote('BUY', approved.outcome) if pool else None
        working = WorkingOrder(approved, deepcopy(market), record,
            canonical_side=side, canonical_price=price, decision_quote=quote,
            decision_mid=(bid + ask) / 2 if bid is not None and ask is not None else None,
            submitted_at=self.clock())
        self.scheduler.schedule(self.clock() + self.config.processing_delay.sample(self.rng),
                                Scheduler.ARRIVAL, self._dispatch, order_id)
        self.orders[order_id] = working
        self.log('submission_pending', order_id=order_id, intent_id=approved.intent_id)
        return order_id

    def enqueue_batch(self, batch, orders, markets):
        """Queue every approved leg, or restore the pre-dispatch engine state."""
        order_ids = []
        scheduled_events = list(self.scheduler.events)
        scheduler_counter = self.scheduler.counter
        random_state = self.rng.getstate()
        id_counter = self.ids.counter
        try:
            for approved in orders:
                market = markets[f'{approved.exchange}:{approved.market_id}']
                order_ids.append(self.enqueue(approved, market))
            self.register_group(batch, order_ids)
        except BaseException:
            self.scheduler.events = scheduled_events
            self.scheduler.counter = scheduler_counter
            self.rng.setstate(random_state)
            self.ids.counter = id_counter
            for order_id in order_ids:
                self.orders.pop(order_id, None)
            self.groups.pop(batch.batch_id, None)
            raise
        return order_ids

    async def _dispatch(self, order_id):
        order = self.orders[order_id]
        if order.phase != 'pending':
            return
        dispatch = self.budgets[order.approved.exchange].request(self.clock(), 'submit')
        if dispatch is None:
            return self._finish(order, OrderStatus.REJECTED, 'venue_rate_limited')
        if dispatch > self.clock():
            self.log('venue_rate_queue', order_id=order_id, queued_until=dispatch)
            self.scheduler.schedule(dispatch, Scheduler.ARRIVAL, self._dispatch_reserved, order_id)
        else:
            await self._dispatch_reserved(order_id)

    async def _dispatch_reserved(self, order_id):
        order = self.orders[order_id]
        if order.phase != 'pending':
            return
        async with self.coordinator.lock:
            async with self.coordinator.state.lock:
                valid = self.coordinator._revalidate(order.approved)
        if not valid:
            return self._finish(order, OrderStatus.REJECTED, 'approval_expired_before_dispatch')
        order.phase = 'dispatched'
        order.dispatched_at = self.clock()
        delay = self.config.venues[order.approved.exchange].outbound.sample(self.rng)
        self.log('dispatched', order_id=order_id, outbound_delay=delay)
        self.scheduler.schedule(self.clock() + delay, Scheduler.ARRIVAL, self._arrive, order_id)

    def _arrive(self, order_id):
        order = self.orders[order_id]
        if order.phase != 'dispatched':
            return
        approved = order.approved
        rules = self.config.venues[approved.exchange]
        order.arrived_at = self.clock()
        pool = self.books.get((approved.exchange, approved.market_id))
        reason = self._arrival_rejection_reason(order, pool, rules)
        if reason:
            return self._finish(order, OrderStatus.REJECTED, reason)
        if self._would_cross_own_order(order):
            return self._finish(order, OrderStatus.REJECTED, 'self_trade_prevented')
        order.arrival_quote = pool.quote(approved.side.value, approved.outcome)
        allocations, remaining = depth_allocations(pool, order.canonical_side,
            order.canonical_price, order.remaining, order.market.quantity_step)
        if approved.intent.post_only and allocations:
            return self._finish(order, OrderStatus.REJECTED, 'post_only_would_cross')
        if approved.tif == TimeInForce.FOK and remaining:
            return self._finish(order, OrderStatus.CANCELED, 'insufficient_depth_fok')
        try:
            proposed = self._preflight(order, allocations, 'taker')
        except ValueError as error:
            return self._finish(order, OrderStatus.REJECTED, str(error))
        order.phase = 'working'
        if not self.config.lose_acknowledgements:
            self._report(order, rules.acknowledgement.sample(self.rng))
        opposite = 'SELL' if order.canonical_side == 'BUY' else 'BUY'
        self._log_depth_participation(order, pool, allocations)
        for (price, quantity), charge in zip(allocations, proposed):
            pool.available[opposite][price] -= quantity
            self._fill(order, quantity, price, 'taker', charge)
        if order.remaining == 0:
            order.phase = 'terminal'
            order.terminal_at = self.clock()
            self._group_progress(order)
        elif approved.tif in (TimeInForce.IOC, TimeInForce.FOK):
            self._finish(order, OrderStatus.CANCELED, 'ioc_remainder_canceled')
        else:
            self._rest_remaining(order, pool)

    def _arrival_rejection_reason(self, order, pool, rules):
        """Recheck market rules at arrival, retaining rejection precedence."""
        approved = order.approved
        manager = self.coordinator.state.risk_managers[approved.portfolio_id]
        max_age = min(rules.max_book_age, manager.config.max_quote_age_seconds)
        reason = pool.executable(self.clock(), max_age) if pool else 'depth_unavailable'
        current = self.coordinator.state.markets.get(f'{approved.exchange}:{approved.market_id}')

        if current is None or not current.active or not current.tradable.get(approved.outcome, False):
            reason = 'market_unavailable'
        elif (current.tick_size, current.quantity_step, current.price_ranges) != (
            order.market.tick_size, order.market.quantity_step, order.market.price_ranges
        ):
            reason = 'market_rules_changed_since_approval'
        elif not valid_price(current, approved.price, approved.outcome):
            reason = 'invalid_price_tick'

        if approved.tif.value not in rules.supported_tifs:
            reason = 'unsupported_venue_instruction'
        if approved.order_type == OrderType.MARKET and approved.tif not in (TimeInForce.FOK, TimeInForce.IOC):
            reason = 'unsupported_venue_instruction'
        if approved.good_till_date is not None and self.clock() >= approved.good_till_date:
            reason = 'expired_before_arrival'
        if approved.intent.post_only and not rules.post_only:
            reason = 'unsupported_venue_instruction'
        return reason

    def _would_cross_own_order(self, order):
        for other in self.orders.values():
            same_account = other.approved.portfolio_id == order.approved.portfolio_id
            same_market = (other.approved.exchange, other.approved.market_id) == (
                order.approved.exchange, order.approved.market_id
            )
            if other.phase != 'working' or not same_account or not same_market:
                continue
            if other.canonical_side == order.canonical_side:
                continue
            if order.canonical_side == 'BUY' and order.canonical_price >= other.canonical_price:
                return True
            if order.canonical_side == 'SELL' and order.canonical_price <= other.canonical_price:
                return True
        return False

    def _log_depth_participation(self, order, pool, allocations):
        opposite = 'SELL' if order.canonical_side == 'BUY' else 'BUY'
        taken = sum((quantity for _, quantity in allocations), Decimal(0))
        visible = Decimal(0)
        for price, quantity in pool.observed[opposite].items():
            within_bound = (price <= order.canonical_price if order.canonical_side == 'BUY'
                            else price >= order.canonical_price)
            if within_bound:
                visible += quantity
        if taken and visible and taken / visible >= dec(self.config.participation_alert):
            self.log(
                'participation_alert', order_id=order.record.order_id,
                exchange=order.approved.exchange, market_id=order.approved.market_id,
                basis='displayed_eligible_depth', quantity=str(taken),
                reference_quantity=str(visible), ratio=str(taken / visible),
            )

    def _rest_remaining(self, order, pool):
        approved = order.approved
        order_id = order.record.order_id
        key = (approved.exchange, approved.market_id, order.canonical_side, order.canonical_price)
        queue = self.queues.setdefault(key, PassiveQueue())
        visible = pool.observed[order.canonical_side].get(order.canonical_price, 0)
        queue.join(order_id, visible)
        self.log('resting', order_id=order_id, queue=deepcopy(queue.segments), book_revision=pool.revision)
        if approved.tif == TimeInForce.GTD:
            self.scheduler.schedule(approved.good_till_date, Scheduler.CANCEL, self._expire, order_id)

    def _preflight(self, order, allocations, role):
        if self.history_failed:
            raise RuntimeError('history_unavailable')
        fees = deepcopy(order.fees)
        schedule = self.config.venues[order.approved.exchange].fee_schedule(order.approved.market_id, self.clock())
        fee_model = FeeModel(schedule)
        total_fees = order.charged
        proposed = []
        for price, quantity in allocations:
            outcome_price = price if order.approved.outcome == 'YES' else 1 - price
            fee, credit = fee_model.charge(quantity, outcome_price, order.approved.side.value, role, fees)
            total_fees += fee
            proposed.append(FillCharge(fee, credit, deepcopy(fees), schedule.version))
        reserve = order.approved.fee_reserve
        if reserve is None:
            reserve = dec(order.approved.size) * dec(order.approved.fee_per_contract)
        if total_fees > dec(reserve) + Decimal('.000000000001'):
            raise ValueError('fee_reserve_exceeded')
        if len(self.scheduler.events) + 2 * len(proposed) + 4 >= self.scheduler.limit:
            raise ValueError('scheduler_capacity_unavailable')
        return proposed

    def _fill(self, order, quantity, price, role, charge: FillCharge):
        outcome_price = price if order.approved.outcome == 'YES' else 1 - price
        order.filled += quantity
        order.first_fill_at = self.clock() if order.first_fill_at is None else order.first_fill_at
        order.last_fill_at = self.clock()
        if order.cancel_requested_at is not None:
            self.log('fill_after_cancel_request', order_id=order.record.order_id)
        order.notional += quantity * outcome_price
        order.charged += charge.fee
        order.fees = charge.accumulator
        order.record.status = OrderStatus.FILLED if order.remaining == 0 else OrderStatus.PARTIALLY_FILLED
        order.record.filled_quantity = float(order.filled)
        order.record.cumulative_notional = str(order.notional)
        order.record.average_fill_price = float(order.notional / order.filled)
        order.record.fees_paid = float(order.charged)
        order.record.reason = ''
        pool = self.books[(order.approved.exchange, order.approved.market_id)]
        execution = Execution(
            execution_id=self.ids(),
            order_id=order.record.order_id,
            exchange=order.approved.exchange,
            market_id=order.approved.market_id,
            outcome=order.approved.outcome,
            side=order.approved.side.value,
            quantity=str(quantity),
            price=str(outcome_price),
            notional=str(quantity * outcome_price),
            fee=str(charge.fee),
            credit=str(charge.credit),
            role=role,
            timestamp=self.clock(),
            book_revision=pool.revision,
            fee_version=charge.fee_version,
            decision_quote=order.decision_quote,
            arrival_quote=order.arrival_quote,
            decision_mid=order.decision_mid,
            fee_details=deepcopy(charge.accumulator.last_breakdown),
        )
        self.executions.append(execution)
        self._update_group_exposure(order)
        delay = self.config.venues[order.approved.exchange].execution_report.sample(self.rng)
        self._report(order, delay)
        if charge.credit:
            self.scheduler.schedule(self.clock() + delay, Scheduler.REPORT, self._credit,
                CashCredit(execution.execution_id + ':credit', order.approved.portfolio_id,
                    order.approved.exchange, str(charge.credit), order.approved.strategy,
                    order.record.order_id, self.clock()))
        self.log('execution', execution_id=execution.execution_id, order_id=order.record.order_id,
                 quantity=str(quantity), price=str(outcome_price), role=role,
                 report_delay=delay, book_revision=pool.revision)

    def _update_group_exposure(self, order):
        group = self.groups.get(order.approved.batch_id)
        if group is None:
            return
        filled_scales = [
            self.orders[order_id].filled / Decimal(ratio)
            for order_id, ratio in zip(group['ids'], group['batch'].quantity_ratios)
        ]
        unmatched = len(set(filled_scales)) > 1
        if unmatched and group['unmatched_since'] is None:
            group['unmatched_since'] = self.clock()
        elif not unmatched and group['unmatched_since'] is not None:
            group['unmatched_seconds'] += self.clock() - group['unmatched_since']
            group['unmatched_since'] = None
        self.log('group_execution', batch_id=order.approved.batch_id,
                 filled_scales=[str(scale) for scale in filled_scales], unmatched=unmatched)

    def _report(self, order, delay=0):
        order.record.revision += 1
        if order.record.status in TERMINAL_ORDER_STATUSES:
            order.record.remaining_fee_reserve = 0
        elif order.approved.fee_reserve is not None:
            order.record.remaining_fee_reserve = float(max(Decimal(0), dec(order.approved.fee_reserve) - order.charged))
        delivery_at = self.clock() + delay
        order.last_report_at = max(order.last_report_at or delivery_at, delivery_at)
        self.scheduler.schedule(delivery_at, Scheduler.REPORT, self._deliver,
                                deepcopy(order.record), order.approved)

    async def _deliver(self, record, approved):
        try:
            await self.coordinator.apply_paper_update(record, approved)
        except Exception:
            if len(self.outbox) >= self.config.max_outbox:
                raise BufferError('paper_report_outbox_overflow')
            self.outbox.append((record, approved))
            raise

    async def retry_reports(self):
        while self.outbox:
            record, approved = self.outbox[0]
            await self.coordinator.apply_paper_update(record, approved)
            self.outbox.popleft()
        while self.credit_outbox:
            await self.coordinator.apply_paper_credit(self.credit_outbox[0])
            self.credit_outbox.popleft()

    async def _credit(self, credit):
        try:
            await self.coordinator.apply_paper_credit(credit)
        except Exception:
            if len(self.credit_outbox) + len(self.outbox) >= self.config.max_outbox:
                raise BufferError('paper_credit_outbox_overflow')
            self.credit_outbox.append(credit)
            raise

    def _finish(self, order, status, reason):
        if order.phase == 'terminal':
            return
        order.phase = 'terminal'
        order.record.status = status
        order.record.reason = reason
        order.terminal_at = self.clock()
        order.record.remaining_fee_reserve = 0
        for queue in self.queues.values():
            queue.remove(order.record.order_id)
        self._report(order, self.config.venues[order.approved.exchange].execution_report.sample(self.rng))
        self.log('terminal', order_id=order.record.order_id, status=status.value, reason=reason)
        self._group_progress(order)

    def enqueue_cancel(self, order_id):
        order = self.orders.get(order_id)
        if order is None:
            return False
        rules = self.config.venues[order.approved.exchange]
        dispatch = self.budgets[order.approved.exchange].request(self.clock(), 'cancel')
        if dispatch is None:
            self.log('cancel_rejected', order_id=order_id, reason='venue_rate_limited')
            return False
        order.cancel_requested_at = self.clock()
        self.scheduler.schedule(dispatch + rules.cancel.sample(self.rng), Scheduler.CANCEL, self._cancel_arrive, order_id)
        self.log('cancel_requested', order_id=order_id)
        return True

    def _cancel_arrive(self, order_id):
        order = self.orders[order_id]
        if order.phase == 'terminal':
            rules = self.config.venues[order.approved.exchange]
            self._report(order, rules.execution_report.sample(self.rng))
        else:
            self._finish(order, OrderStatus.CANCELED, 'cancel_confirmed')

    def _expire(self, order_id):
        self._finish(self.orders[order_id], OrderStatus.EXPIRED, 'gtd_expired')

    def get_order(self, order_id):
        order = self.orders.get(order_id)
        return deepcopy(order.record) if order and order.phase in {'working', 'terminal'} else None

    async def on_public(self, envelope):
        if self.history_failed:
            raise RuntimeError('history_unavailable')
        event = envelope.event
        if self.history_store and isinstance(event, TradeEvent):
            identifier = event.trade_id or envelope.source_id
            if identifier and await self.history_store.acontains('public_prints', (event.exchange, event.market_id, identifier)):
                return
        if self.history_store and isinstance(event, OrderBookSnapshotEvent):
            if len(event.bids) + len(event.asks) > self.limits.get('max_event_bytes', 262144) // 64:
                raise BufferError('book_level_capacity_exceeded')
        self.log('public_data', exchange=event.exchange, ingest_index=envelope.ingest_index,
                 ingestion_delay=envelope.available_at-envelope.received_at,
                 timestamp_quality=envelope.timestamp_quality, depth_coverage=envelope.depth_coverage)
        key = (event.exchange, getattr(event, 'market_id', ''))
        if isinstance(event, PublicConnectionEvent):
            for (exchange, market_id), pool in self.books.items():
                if exchange == event.exchange and (not event.market_id or market_id == event.market_id):
                    pool.invalidate(event.reason if not event.connected else 'awaiting_resnapshot')
            self.log('data_connection', exchange=event.exchange, connected=event.connected, reason=event.reason)
            if not event.connected and self.config.venues[event.exchange].cancel_on_disconnect:
                for order in list(self.orders.values()):
                    if order.approved.exchange == event.exchange:
                        self._finish(order, OrderStatus.CANCELED, 'venue_cancel_on_disconnect')
            return
        if self.history_store and key not in self.books and len(self.books) >= self.limits.get('max_markets', 8):
            raise BufferError('max_markets_exceeded')
        pool = self.books.setdefault(key, BookPool())
        if isinstance(event, OrderBookSnapshotEvent):
            market = self.coordinator.state.markets.get(f'{event.exchange}:{event.market_id}')
            if market is None:
                pool.invalidate('unknown_market_rules')
                return
            reset = pool.snapshot(event, envelope, market)
            if self.history_store:
                pool.changed_at = {k: v for k, v in pool.changed_at.items() if k[1] in pool.observed[k[0]]}
            if reset is None:
                self.log('duplicate_book', exchange=event.exchange, source_id=envelope.source_id)
                return
            for queue_key, queue in self.queues.items():
                if queue_key[:2] == key:
                    visible = pool.observed[queue_key[2]].get(queue_key[3], 0)
                    if reset:
                        ids = [oid for oid, _ in queue.segments if oid is not None]
                        queue.reset(ids, visible)
                        self.log('queue_reset', exchange=event.exchange, market_id=event.market_id)
                    else:
                        queue.observe(visible)
            self._compact_queues()
        elif isinstance(event, LiteOrderBookSnapshotEvent):
            for side, quote in (('SELL', event.best_bid), ('BUY', event.best_ask)):
                if quote is not None and pool.quote(side, event.outcome) != float(quote):
                    pool.invalidate('lite_depth_disagreement')
        elif isinstance(event, MarketStatusEvent):
            pool.state = event.status
            if event.status in {'closed', 'settled', 'EXPIRED', 'MARKET_STATE_EXPIRED', 'TERMINATED'}:
                for order in list(self.orders.values()):
                    if (order.approved.exchange, order.approved.market_id) == key:
                        self._finish(order, OrderStatus.EXPIRED, 'market_closed')
        elif isinstance(event, TradeEvent):
            self._trade(event, envelope, pool)

    def _trade(self, event, envelope, pool):
        identifier = event.trade_id or envelope.source_id
        token = (event.exchange, event.market_id, identifier)
        if not identifier or not isinstance(event.side, Side) or not envelope.eligible_trade:
            if not identifier:
                pool.invalidate('ambiguous_trade_identity')
            return
        if token in self.seen_trades:
            return
        if dec(event.quantity) <= 0 or not 0 < dec(event.price) < 1:
            raise ValueError('Invalid public execution amount')
        self.seen_trades.add(token)
        if pool.executable(self.clock(), self.config.venues[event.exchange].max_book_age):
            return
        side, price = canonical_order(event.outcome, event.side.value, event.price)
        passive_side = 'SELL' if side == 'BUY' else 'BUY'
        queue = self.queues.get((event.exchange, event.market_id, passive_side, price))
        if queue:
            filled = Decimal(0)
            def remaining(oid):
                order = self.orders[oid]
                return order.remaining if order.phase == 'working' else Decimal(0)
            def fill(oid, quantity):
                nonlocal filled
                order = self.orders[oid]
                step = dec(order.market.quantity_step)
                quantity = quantity // step * step
                if not quantity:
                    return Decimal(0)
                try:
                    charge = self._preflight(order, [(price, quantity)], 'maker')[0]
                except ValueError as error:
                    # An accepted resting order does not disappear because its
                    # fee model/data became unusable. Retain its commitment.
                    pool.invalidate('paper_execution_suspended')
                    self.log('execution_suspended', order_id=oid, reason=str(error))
                    return Decimal(0)
                self._fill(order, quantity, price, 'maker', charge)
                filled += quantity
                if not order.remaining:
                    order.phase = 'terminal'
                    order.terminal_at = self.clock()
                    self._group_progress(order)
                return quantity
            queue.consume(dec(event.quantity), remaining, fill)
            self.log('passive_trade_budget', exchange=event.exchange, market_id=event.market_id,
                     source_id=identifier, public_quantity=str(event.quantity),
                     paper_quantity=str(filled), queue=deepcopy(queue.segments))
            if filled and filled / dec(event.quantity) >= dec(self.config.participation_alert):
                self.log('participation_alert', exchange=event.exchange, market_id=event.market_id,
                         basis='eligible_public_trade', quantity=str(filled),
                         reference_quantity=str(event.quantity), ratio=str(filled / dec(event.quantity)))
        if self.history_store and len(pool.quarantined) >= self.limits.get('max_event_bytes', 262144) // 64 and (passive_side, price) not in pool.quarantined:
            raise BufferError('quarantine_capacity_exceeded')
        pool.quarantined.add((passive_side, price))

    def register_group(self, batch, order_ids):
        self.groups[batch.batch_id] = dict(batch=batch, ids=list(order_ids), failed=False,
                                          unmatched_since=None, unmatched_seconds=0.0)
        if batch.dispatch_policy == 'sequential':
            for oid in order_ids[1:]:
                self.orders[oid].phase = 'held'
        if batch.execution_deadline is not None:
            self.scheduler.schedule(batch.execution_deadline, Scheduler.CANCEL, self._group_deadline, batch.batch_id)

    def _group_deadline(self, batch_id):
        group = self.groups[batch_id]
        group['failed'] = True
        for oid in group['ids']:
            if self.orders[oid].phase != 'terminal':
                self.enqueue_cancel(oid)
        self.log('group_deadline', batch_id=batch_id)

    def _group_progress(self, order):
        group = self.groups.get(order.approved.batch_id)
        if not group:
            return
        if order.record.status != OrderStatus.FILLED:
            if not group['failed']:
                self._group_deadline(order.approved.batch_id)
        elif group['batch'].dispatch_policy == 'sequential' and not group['failed']:
            index = group['ids'].index(order.record.order_id)
            if index + 1 < len(group['ids']):
                next_id = group['ids'][index + 1]
                if self.orders[next_id].phase == 'held':
                    self.orders[next_id].phase = 'pending'
                    self.scheduler.schedule(self.clock(), Scheduler.ARRIVAL, self._dispatch, next_id)

    def _timing(self, order):
        return dict(kind='order_timing', order_id=order.record.order_id,
            portfolio_id=order.approved.portfolio_id, exchange=order.approved.exchange,
            strategy=order.approved.strategy, market_id=order.approved.market_id,
            status=order.record.status.value, time_in_force=order.approved.tif.value,
            size=str(order.approved.size), filled_quantity=str(order.filled),
            submitted_at=order.submitted_at, dispatched_at=order.dispatched_at,
            arrived_at=order.arrived_at, first_fill_at=order.first_fill_at,
            last_fill_at=order.last_fill_at, terminal_at=order.terminal_at,
            cancel_requested_at=order.cancel_requested_at, last_report_at=order.last_report_at,
            revision=order.record.revision)

    def _group_detail(self, batch_id, group):
        seconds = group['unmatched_seconds']
        if group['unmatched_since'] is not None:
            seconds += self.clock() - group['unmatched_since']
        return dict(kind='group', batch_id=batch_id, failed=group['failed'],
            unmatched_seconds=seconds, legs=[dict(order_id=oid,
                exchange=self.orders[oid].approved.exchange,
                filled_quantity=str(self.orders[oid].filled),
                remaining_quantity=str(self.orders[oid].remaining),
                status=self.orders[oid].record.status.value) for oid in group['ids']])

    async def compact(self, *, force=False):
        """Persist bounded batches, then retire only resolved working evidence."""
        if self.history_store is None:
            return
        async with self.coordinator.lock:
            try:
                await self.coordinator.state.compact_history(force=force)
                await self._compact_engine(force=force)
            except BaseException:
                self.history_failed = True
                raise

    async def _compact_engine(self, *, force=False):
        store = self.history_store
        hot = self.limits.get('max_hot_history_records', 16)
        batch = min(hot, self.limits.get('max_archive_cache_records', 8))
        if (not force and len(self.executions) < batch and len(self.seen_trades) < batch
                and sum(order.phase == 'terminal' for order in self.orders.values()) < hot + batch):
            # All uncommitted executions/IDs/orders remain authoritative here.
            # A finite batch reduces sync latency; retirement happens only below
            # after its real SQLite commit and detail flush have succeeded.
            self._compact_queues()
            return
        # Only no-op timers are removed. Reports/credits remain authoritative.
        def obsolete(event):
            if getattr(event.callback, '__self__', None) is not self or not event.args:
                return False
            name = event.callback.__name__
            if name == '_group_deadline':
                group = self.groups.get(event.args[0])
                return group is None or all(self.orders[oid].phase == 'terminal' for oid in group['ids'])
            if name in {'_expire', '_dispatch', '_dispatch_reserved', '_arrive'}:
                order = self.orders.get(event.args[0])
                return order is None or order.phase == 'terminal'
            return False
        self.scheduler.discard(obsolete)
        pending = {event.args[0].order_id for event in self.scheduler.events
                   if getattr(event.callback, '__self__', None) is self
                   and event.callback.__name__ == '_deliver'}
        pending.update(event.args[0] for event in self.scheduler.events
                       if getattr(event.callback, '__self__', None) is self
                       and event.callback.__name__ == '_cancel_arrive')
        pending.update(record.order_id for record, _ in self.outbox)
        resolved = []
        for oid, order in self.orders.items():
            if order.phase != 'terminal' or oid in pending:
                continue
            portfolio = self.coordinator.state.portfolios[order.approved.portfolio_id]
            observed = portfolio.order_history.get(f'{order.approved.exchange}:{oid}')
            if observed is None:
                observed = await store.aget(f'orders:{order.approved.portfolio_id}', f'{order.approved.exchange}:{oid}')
            if observed is not None and observed.revision >= order.record.revision:
                resolved.append(oid)
        groups = [(bid, group) for bid, group in self.groups.items() if all(oid in resolved for oid in group['ids'])]
        fills = list(self.executions)
        prints = list(self.seen_trades)
        timing = [(oid, self._timing(self.orders[oid])) for oid in resolved]
        order_rows = [(oid, self._timing(order)) for oid, order in self.orders.items()]
        group_rows = [(bid, self._group_detail(bid, group)) for bid, group in groups]
        def commit(conn):
            for oid, row in order_rows:
                store._put(conn, 'engine_orders', oid, row, True)
            for execution in fills:
                if store._put(conn, 'execution_ids', execution.execution_id, True):
                    order = self.orders[execution.order_id]
                    row = dict(kind='execution', portfolio_id=order.approved.portfolio_id, strategy=order.approved.strategy, **asdict(execution))
                    store._append(conn, 'executions', row)
                    for horizon in self.config.markout_horizons:
                        identity = f'{execution.execution_id}:{horizon}'
                        if store._get(conn, 'markout_ids', identity, False):
                            continue
                        value = store._encode(dict(execution=execution, horizon=horizon))
                        conn.execute('INSERT OR IGNORE INTO pending_marks VALUES (?,?,?,?,?)', (identity, execution.timestamp+horizon, value, execution.exchange, execution.market_id))
            for token in prints:
                store._put(conn, 'public_prints', token, True)
            for oid, row in timing:
                if store._put(conn, 'timing_ids', oid, True):
                    store._append(conn, 'order_timings', row)
            for bid, row in group_rows:
                if store._put(conn, 'group_ids', bid, True):
                    store._append(conn, 'groups', row)
            conn.commit()
        await store.run(commit)
        del self.executions[:len(fills)]
        self.seen_trades.clear()
        for bid, _ in groups:
            self.groups.pop(bid)
        protected = {oid for group in self.groups.values() for oid in group['ids']}
        evict = [oid for oid in resolved if oid not in protected]
        for oid in evict[:-hot]:
            self.orders.pop(oid)
        self._compact_queues()

    def _compact_queues(self):
        if self.history_store is None:
            return
        for key, queue in list(self.queues.items()):
            compacted = []
            for oid, amount in queue.segments:
                if oid is not None and (oid not in self.orders or self.orders[oid].phase != 'working'):
                    continue
                if oid is None and compacted and compacted[-1][0] is None:
                    compacted[-1][1] += amount
                else:
                    compacted.append([oid, amount])
            if any(oid is not None for oid, _ in compacted):
                queue.segments = compacted
            else:
                self.queues.pop(key)
