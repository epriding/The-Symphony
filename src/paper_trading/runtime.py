"""Single-owner paper runtime; strategies never drive the account or scheduler."""
import asyncio
from copy import deepcopy
from dataclasses import dataclass, replace
import inspect
import json
import logging
import math
import os
from pathlib import Path
import time

from .clock import IdGenerator, RealClock, VirtualClock
from .recording import encode
from .runtime_config import (_public_queue_limit, _public_startup_buffer_bytes,
                             positive, retained_bytes, trading_config,
                             validate_bot_config, validate_output_directory)
from .scheduler import Scheduler
from .session import PaperSession
from ..strategies.context import StrategyContext
from ..utilities.classes import OrderIntent, OrderStatus, OrderType, Side, TimeInForce
from ..utilities.market_events import PublicEventEnvelope, ReceiptSequencer
from ..utilities.logger import BoundedLogHandler, SafeFormatter, redact


@dataclass
class _CallbackTiming:
    deadline: float
    completed_at: float | None = None


@dataclass
class _Slot:
    entry: dict
    context: object = None
    strategy: object = None
    task: object = None
    timing: _CallbackTiming | None = None
    pending: bool = False
    disabled: bool = False
    reason: str = ''
    dispatch: int = 0
    snapshot_bytes: int = 0


class _SessionClock(VirtualClock):
    """Freeze time during each serialized transition; sample only at driver boundaries."""
    def __init__(self, source):
        self.source = source
        super().__init__(source())

    def refresh(self):
        self.advance(max(self(), self.source()))


async def _create_public_feeds(settings, limits, *, timeout):
    from ..api.interfaces.kalshi import KalshiMarketData
    from ..api.kalshi.public_api import KalshiPublicAPI
    from ..api.interfaces.polymarket_us_public import PolymarketUSMarketData
    feeds, credentials = [], {}
    # Resolve every credential input before opening any transport owner.
    for exchange, options in settings.items():
        if exchange == 'KALSHI':
            key = os.environ.get(options.get('key_id_env', 'KALSHI_API_KEY_ID'))
            private_key = os.environ.get(options.get('private_key_env', 'KALSHI_PRIVATE_KEY_PATH'))
            if not key or not private_key:
                raise ValueError('Set the configured Kalshi public-feed credential environment variables')
            credentials[exchange] = key, private_key
        else:
            key = os.environ.get(options.get('key_id_env', 'POLYMARKET_US_KEY_ID'))
            secret = os.environ.get(options.get('secret_key_env', 'POLYMARKET_US_SECRET_KEY'))
            if not key or not secret:
                raise ValueError('Set the configured Polymarket US public-feed credential environment variables')
            credentials[exchange] = key, secret
    try:
        for exchange, options in settings.items():
            common = dict(max_event_bytes=limits['max_event_bytes'],
                          max_markets=limits['max_markets'],
                          max_book_levels=max(1, limits['max_event_bytes'] // 128))
            if exchange == 'KALSHI':
                key, private_key = credentials[exchange]
                api = KalshiPublicAPI(key, private_key, read_interval=options['read_interval'],
                                      max_event_bytes=limits['max_event_bytes'])
                feed = KalshiMarketData(options['markets'], api=api,
                             overrides=options.get('market_rules', {}), **common)
            else:
                key, secret = credentials[exchange]
                feed = PolymarketUSMarketData(options['markets'], key, secret,
                             overrides=options.get('market_rules', {}),
                             read_interval=options['read_interval'],
                             queue_limit=_public_queue_limit(options['markets']), **common)
            feeds.append(feed)
    except BaseException as error:
        for feed in feeds:
            if callable(getattr(feed, 'aclose', None)):
                try:
                    await asyncio.wait_for(feed.aclose(), timeout)
                except BaseException as cleanup_error:
                    error.add_note('partial feed cleanup: ' + redact(cleanup_error))
        raise
    return feeds


class BotRunner:
    def __init__(self, config, *, output_directory, feeds=None,
                 strategy_factories=None, clock=None, operator_controls=False,
                 operator_observer=None):
        self.config = validate_bot_config(config, require_runtime=True)
        self.output_directory = validate_output_directory(output_directory)
        self.runtime = self.config['runtime']
        self.limits = self.runtime['limits']
        from ..strategies.smoke import SmokeStrategy
        self.factories = {'smoke-v1': SmokeStrategy}
        self.factories.update(strategy_factories or {})
        self.slots = [_Slot(entry) for entry in self.config['strategies'] if entry['enabled']]
        for slot in self.slots:
            factory = self.factories.get(slot.entry['factory'])
            if not callable(factory):
                raise ValueError(f"Unregistered strategy factory: {slot.entry['factory']}")
        self.feeds = list(feeds) if feeds is not None else None
        if self.feeds is not None:
            expected = set(self.config['feeds'])
            if len(self.feeds) != len(expected) or {feed.exchange for feed in self.feeds} != expected:
                raise ValueError('Injected feeds must match the configured venues exactly')
            for feed in self.feeds:
                if set(feed.markets) != set(self.config['feeds'][feed.exchange]['markets']):
                    raise ValueError('Injected feed subscriptions must match configuration')
        self.clock = _SessionClock(clock or RealClock())
        self._used = False
        self._stop = asyncio.Event()
        self._operator_controls = operator_controls
        self._operator_observer = operator_observer
        self._operator_state = 'running'
        self._pause_requested = False
        self._resume_requested = False
        self._wake = asyncio.Event()
        self._session = None
        self._store = None
        self._tasks = []
        self._zombies = {}
        self._producer_error = None
        self._ended = set()
        self._revision = 0
        self._dispatch_id = 0
        self._initial_ready = False
        self._dirty = False
        self._stopping = False
        self._identity_mapping = {}
        self._last_status = None
        self._event_bytes = 0
        self._event_bytes_highwater = 0
        self._memory_highwater = 0
        self._base_bytes = retained_bytes(self.config) * 3
        # Parser/native queues, SQLite pages/cache, logging and finalization headroom.
        self._reserved_bytes = (self.limits['max_event_bytes'] * (16 + len(self.config['feeds']) * 8)
                                + _public_startup_buffer_bytes(self.config)
                                + self.limits['max_archive_cache_records'] * self.limits['max_event_bytes']
                                + 1024 * 1024)
        self._status = dict(phase='starting', reason='awaiting startup',
            strategies={entry['name']: dict(ready=False, reason='disabled' if not entry['enabled'] else 'awaiting startup')
                        for entry in self.config['strategies']},
            venues={venue: dict(ready=False, reason='awaiting verified depth') for venue in self.config['feeds']})
        self._system_ids = IdGenerator(self.config['execution'].get('session_id', 'paper') + '-risk')

    @property
    def status(self):
        return deepcopy(self._status)

    def request_stop(self):
        self._stop.set()
        self._wake.set()

    @property
    def operator_state(self):
        return self._operator_state

    def request_pause(self):
        if not self._operator_controls:
            raise ValueError('Operator controls require an explicitly enabled runner')
        if self._stop.is_set() or self._stopping:
            return
        self._resume_requested = False
        if self._operator_state == 'running':
            self._pause_requested = True
            self._operator_state = 'pausing'
            self._wake.set()

    def request_resume(self):
        if self._stop.is_set() or self._stopping:
            return
        if not self._operator_controls:
            raise ValueError('Operator controls require an explicitly enabled runner')
        if self._operator_state == 'pausing':
            raise ValueError('Pause has not quiesced')
        self._resume_requested = self._operator_state == 'paused'
        self._wake.set()

    async def _apply_operator_control(self):
        if self._stop.is_set() or self._stopping:
            return
        if self._operator_state == 'pausing':
            self._control('operator_pause')
            # Retain cancellation-resistant callbacks for failure cleanup too.
            for slot in self.slots:
                if slot.task is not None:
                    self._zombies[slot.task] = time.monotonic() + self.runtime['shutdown_grace_seconds']
            await self._stop_callbacks()
            self._operator_state = 'paused'
        if self._resume_requested:
            self._resume_requested = False
            self._pause_requested = False
            self._operator_state = 'running'
            self._dirty = True
            self._control('operator_resume')

    def _control(self, action, **payload):
        self._session.record('runtime_control', dict(action=action, **payload))
        self._observe_operator('control', dict(action=action, **payload))

    def _observe_operator(self, kind, payload):
        if self._operator_observer is not None:
            self._operator_observer(self, kind, payload)

    async def _after_public(self, envelope):
        self._revision += 1
        self._dirty = True

    async def _collect(self, feed):
        try:
            async for envelope in feed.stream():
                if not isinstance(envelope, PublicEventEnvelope):
                    raise ValueError('Public feed must yield PublicEventEnvelope')
                payload = json.dumps(encode(envelope), allow_nan=False, separators=(',', ':')).encode()
                if len(payload) > self.limits['max_event_bytes']:
                    raise BufferError('public_event_exceeds_max_event_bytes')
                charge = retained_bytes(envelope)
                self._check_memory(extra=charge)
                self._event_bytes += charge
                self._event_bytes_highwater = max(self._event_bytes_highwater, self._event_bytes)
                try:
                    await asyncio.wait_for(self._queue.put(('public', envelope, charge)),
                                           self.runtime['shutdown_grace_seconds'])
                except BaseException as error:
                    self._event_bytes -= charge
                    if isinstance(error, asyncio.TimeoutError):
                        raise BufferError('public_collector_overflow') from error
                    raise
                self._wake.set()
            await asyncio.wait_for(self._queue.put(('end', feed.exchange)),
                                   self.runtime['shutdown_grace_seconds'])
            self._wake.set()
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self._producer_error = error
            self._wake.set()

    async def _refresh(self, feed):
        try:
            while True:
                await asyncio.sleep(30)
                for identifier in feed.markets:
                    market = await feed.get_market(identifier)
                    identity = self._identity_mapping[f'{feed.exchange}:{identifier}']
                    if f'{market.exchange}:{market.market_id}' != identity:
                        raise ValueError('Refreshed metadata changed the subscribed market identity')
                    self._put(('market', market))
                async for envelope in feed.settlements():
                    self._put(('public', envelope))
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self._producer_error = error
            self._wake.set()

    def _put(self, item):
        if item[0] in ('public', 'market'):
            if item[0] == 'public' and not isinstance(item[1], PublicEventEnvelope):
                raise ValueError('Public feed must yield PublicEventEnvelope')
            encoded = json.dumps(encode(item[1]), allow_nan=False, separators=(',', ':')).encode()
            if len(encoded) > self.limits['max_event_bytes']:
                raise BufferError('public_event_exceeds_max_event_bytes')
            charge = retained_bytes(item[1])
            self._check_memory(extra=charge)
            item = (*item, charge)
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull as error:
            raise BufferError('public_collector_overflow') from error
        if len(item) == 3:
            self._event_bytes += item[2]
            self._event_bytes_highwater = max(self._event_bytes_highwater, self._event_bytes)
        self._wake.set()

    def _check_memory(self, *, extra=0):
        owned = 0
        if self._session:
            state, engine = self._session.state, self._session.engine
            owned = retained_bytes((state.markets, state.orderbooks, state.market_quotes,
                state.portfolios, engine.orders, engine.books, engine.groups, engine.executions,
                engine.outbox, engine.credit_outbox, engine.audit, engine.seen_trades))
        total = (self._base_bytes + self._reserved_bytes + self._event_bytes + owned + extra
                 + sum(slot.snapshot_bytes for slot in self.slots))
        self._memory_highwater = max(self._memory_highwater, total)
        if total > self.limits['memory_budget_bytes']:
            raise BufferError('runtime_memory_budget_exceeded; authoritative obligations retained')

    def _market_ready(self, identity):
        canonical = self._identity_mapping.get(identity, identity)
        venue, market_id = canonical.split(':', 1)
        market = self._session.state.markets.get(canonical)
        pool = self._session.engine.books.get((venue, market_id))
        if not market or any(not value for value in (market.tick_size, market.quantity_step, market.min_quantity)):
            return False, 'awaiting required metadata'
        if not pool or not pool.valid:
            return False, pool.reason if pool and pool.reason else 'awaiting verified depth'
        age = self.clock() - pool.received_at
        if age < 0 or age > self._session.engine.config.venues[venue].max_book_age:
            return False, 'stale depth'
        if not market.active:
            return False, 'market inactive'
        return True, ''

    def _readiness(self):
        for venue, options in self.config['feeds'].items():
            values = [self._market_ready(f'{venue}:{market}') for market in options['markets']]
            self._status['venues'][venue] = dict(ready=all(ready for ready, _ in values),
                reason=next((reason for ready, reason in values if not ready), ''))
        all_ready = all(value['ready'] for value in self._status['venues'].values())
        if all_ready and not self._initial_ready:
            self._initial_ready = True
            self._dirty = True
            self._control('startup_ready', revision=self._revision)
        for slot in self.slots:
            values = [self._market_ready(identity) for identity in slot.entry['required_markets']]
            ready = self._initial_ready and not slot.disabled and all(value[0] for value in values)
            reason = slot.reason if slot.disabled else next((reason for okay, reason in values if not okay), '')
            if not self._initial_ready and not reason:
                reason = 'awaiting all initial subscriptions'
            self._status['strategies'][slot.entry['name']] = dict(ready=ready, reason=reason)
        if not self._stopping:
            degraded = self._initial_ready and (not all_ready or any(slot.disabled for slot in self.slots))
            self._status['phase'] = 'degraded' if degraded else 'ready' if self._initial_ready else 'starting'
            self._status['reason'] = ('one or more strategies or venues unavailable' if degraded else
                                      '' if self._initial_ready else 'awaiting all initial subscriptions')
        current = self.status
        if current != self._last_status:
            self._logger.info('readiness %s', json.dumps(current, sort_keys=True))
            self._last_status = current

    async def _notify_and_dispatch(self):
        if (self._stopping or self._stop.is_set() or not self._initial_ready
                or self._operator_state != 'running'):
            return
        if self._dirty:
            self._dirty = False
            for slot in self.slots:
                if slot.disabled or not self._status['strategies'][slot.entry['name']]['ready']:
                    continue
                if slot.pending:
                    self._control('coalesced', strategy=slot.entry['name'], revision=self._revision)
                slot.pending = True
        base = None
        for slot in self.slots:
            if (slot.disabled or slot.task is not None or not slot.pending
                    or not self._status['strategies'][slot.entry['name']]['ready']):
                continue
            if base is None:
                base = await self._session.state.snapshot()
            charge = retained_bytes(base)
            self._check_memory(extra=charge * 2)
            slot.snapshot_bytes = charge
            self._dispatch_id += 1
            slot.dispatch = self._dispatch_id
            slot.pending = False
            self._control('dispatch', strategy=slot.entry['name'], dispatch=slot.dispatch,
                          revision=self._revision, timestamp=base.timestamp)
            slot.context._set_time(base.timestamp)
            snapshot = deepcopy(base)
            slot.timing = _CallbackTiming(time.monotonic() + self.runtime['strategy_timeout_seconds'])
            slot.task = asyncio.create_task(self._invoke_callback(
                slot.strategy, snapshot, slot.timing))
            slot.task.add_done_callback(lambda _: self._wake.set())

    @staticmethod
    async def _invoke_callback(strategy, snapshot, timing):
        # Stamp this dispatch inside the task, before unrelated driver I/O can
        # delay observation of its completion. Disabled tasks retain only their
        # own timing object if they resist cancellation.
        try:
            return await strategy.on_snapshot(snapshot)
        finally:
            timing.completed_at = time.monotonic()

    def _validate_result(self, slot, result):
        return self._validated_result(result, strategy_name=slot.entry['name'],
            markets=self._session.state.markets, limits=self.limits)

    @staticmethod
    def _validated_result(result, *, strategy_name, markets, limits):
        if result is None:
            return []
        if not isinstance(result, list) or len(result) > limits['max_intents_per_callback']:
            raise ValueError('Strategy result must be a bounded list of OrderIntent or None')
        identities = set()
        def finite(value):
            return (not isinstance(value, bool) and isinstance(value, (int, float))
                    and math.isfinite(value))
        for intent in result:
            if (not isinstance(intent, OrderIntent) or intent.strategy != strategy_name
                    or not isinstance(intent.side, Side) or intent.outcome not in ('YES', 'NO')
                    or not isinstance(intent.tif, TimeInForce) or not isinstance(intent.order_type, OrderType)
                    or type(intent.reduce_only) is not bool or type(intent.post_only) is not bool
                    or not isinstance(intent.reason, str)
                    or not isinstance(intent.exchange, str) or not isinstance(intent.market_id, str)
                    or not isinstance(intent.intent_id, str) or not intent.intent_id
                    or intent.intent_id in identities
                    or f'{intent.exchange}:{intent.market_id}' not in markets
                    or not finite(intent.price) or not 0 < intent.price < 1
                    or not finite(intent.created_at)
                    or not finite(intent.close_fraction) or not 0 < intent.close_fraction <= 1
                    or (intent.estimated_probability is not None and
                        (not finite(intent.estimated_probability) or not 0 <= intent.estimated_probability <= 1))
                    or any(value is not None and not finite(value) for value in
                           (intent.resolution_date, intent.expires_at, intent.good_till_date))
                    or (intent.slippage is not None and
                        (type(intent.slippage) is not int or intent.slippage < 0))):
                raise ValueError('Malformed or foreign strategy intent result')
            identities.add(intent.intent_id)
        if len(json.dumps(encode(result), allow_nan=False).encode()) > limits['max_event_bytes']:
            raise ValueError('Strategy decision exceeds max_event_bytes')
        return result

    async def _disable(self, slot, error):
        slot.disabled, slot.pending = True, False
        reason = redact(f'{type(error).__name__}: {error}')
        encoded = reason.encode('utf-8')
        if len(encoded) > 1024:
            marker = ' [truncated]'
            reason = encoded[:1024 - len(marker)].decode('utf-8', errors='ignore') + marker
        slot.reason = reason
        self._status['strategies'][slot.entry['name']] = dict(ready=False, reason=slot.reason)
        task, slot.task = slot.task, None
        slot.timing = None
        if task is not None:
            if not task.done():
                task.cancel()
                self._zombies[task] = time.monotonic() + self.runtime['shutdown_grace_seconds']
            else:
                # Retrieve a failed callback before releasing its only reference.
                if not task.cancelled():
                    task.exception()
                slot.snapshot_bytes = 0
        self._control('disabled', strategy=slot.entry['name'], reason=slot.reason)
        self._logger.error('strategy %s disabled: %s', slot.entry['name'], slot.reason)
        await self._cancel_orders(slot.entry['name'])
        if all(value.disabled for value in self.slots):
            raise RuntimeError('Every enabled strategy is disabled') from error

    async def _complete(self):
        for task in list(self._zombies):
            if task.done():
                if not task.cancelled():
                    task.exception()
                del self._zombies[task]
            elif time.monotonic() >= self._zombies[task]:
                raise RuntimeError('strategy cancellation exceeded shutdown grace')
        # A cancelled callback may still own its copy until it acknowledges cancellation.
        if not self._zombies:
            for slot in self.slots:
                if slot.disabled:
                    slot.snapshot_bytes = 0
        for slot in self.slots:
            task = slot.task
            if task is None:
                continue
            timing = slot.timing
            if ((timing.completed_at is not None and timing.completed_at >= timing.deadline)
                    or (not task.done() and time.monotonic() >= timing.deadline)):
                await self._disable(slot, TimeoutError('strategy callback deadline exceeded'))
                continue
            if not task.done():
                continue
            try:
                result = self._validate_result(slot, task.result())
            except (Exception, asyncio.CancelledError) as error:
                await self._disable(slot, error)
                continue
            slot.task = None
            slot.timing = None
            slot.snapshot_bytes = 0
            ready = self._status['strategies'][slot.entry['name']]['ready']
            self._control('completion', strategy=slot.entry['name'], dispatch=slot.dispatch,
                          decision=result, accepted=ready and not self._stopping and not self._stop.is_set()
                          and self._operator_state == 'running')
            if ready and not self._stopping and not self._stop.is_set() and self._operator_state == 'running':
                for intent in result:
                    await self._session.submit(intent)

    async def _risk(self):
        self._control('risk_tick')
        snapshot = await self._session.state.snapshot()
        self._check_memory(extra=retained_bytes(snapshot))
        for portfolio, manager in self._session.state.risk_managers.items():
            if not self._session.state.portfolios[portfolio].paper:
                continue
            for intent in manager.stop_loss_intents(snapshot):
                intent = replace(intent, intent_id=self._system_ids(), created_at=self.clock())
                await self._session.submit(intent)

    async def _compact(self):
        if hasattr(self._session.engine, 'compact'):
            await self._session.engine.compact()
        if hasattr(self._session.reporter, 'observe_async'):
            await self._session.reporter.observe_async()

    async def _drive(self, *, allow_work=True):
        self.clock.refresh()
        if allow_work:
            await self._apply_operator_control()
        if self._producer_error:
            raise self._producer_error
        # Bound each ingestion batch, then issue one latest notification. Timer
        # advances between commands retain all market/report causal boundaries.
        for _ in range(min(16, self._queue.maxsize)):
            if self._queue.empty():
                break
            item = self._queue.get_nowait()
            kind, payload = item[:2]
            try:
                if kind == 'public':
                    venue = payload.event.exchange
                    market = getattr(payload.event, 'market_id', '')
                    allowed = set(self._identity_mapping.values())
                    if venue not in self.config['feeds'] or (market and f'{venue}:{market}' not in allowed):
                        raise ValueError('Unsubscribed public event')
                    self._session.publish(payload)
                elif kind == 'market':
                    await self._session.add_market(payload)
                elif self.runtime['source'] == 'public':
                    raise RuntimeError(f'Public producer {payload} ended unexpectedly')
                else:
                    self._ended.add(payload)
                await self._session.advance(self.clock())
            finally:
                if len(item) == 3:
                    self._event_bytes -= item[2]
                self._queue.task_done()
        await self._session.advance(self.clock())
        self._observe_operator('tick', {})
        await self._compact()
        self._check_memory()
        self._readiness()
        if allow_work:
            await self._complete()
            await self._notify_and_dispatch()

    async def _cancel_orders(self, strategy=None):
        session = self._session
        for order_id, order in list(session.engine.orders.items()):
            if strategy is not None and order.approved.strategy != strategy:
                continue
            if order.phase == 'terminal':
                continue
            if order.phase in ('pending', 'held') and order.dispatched_at is None:
                session.record('abort', dict(order_id=order_id))
                session.engine._finish(order, OrderStatus.CANCELED, 'unsent_local_abort')
            elif order.cancel_requested_at is None:
                await session.cancel(order_id, order.approved.exchange, order.approved.portfolio_id)

    def _unresolved(self):
        engine = self._session.engine
        return (bool(engine.outbox or engine.credit_outbox)
                or any(order.phase != 'terminal' for order in engine.orders.values())
                or any(portfolio.open_orders or portfolio.reservations
                       for portfolio in self._session.state.portfolios.values() if portfolio.paper)
                or any(event.priority == Scheduler.REPORT for event in engine.scheduler.events))

    async def _stop_callbacks(self):
        tasks = set(self._zombies)
        for slot in self.slots:
            slot.pending = False
            if slot.task is not None:
                tasks.add(slot.task)
                slot.task = None
                slot.timing = None
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=self.runtime['shutdown_grace_seconds'])
            for task in done:
                if not task.cancelled():
                    task.exception()
            if pending:
                raise RuntimeError('strategy cancellation exceeded shutdown grace')
        for slot in self.slots:
            slot.snapshot_bytes = 0

    async def _shutdown(self, *, failed=False):
        self._stopping = True
        self._status['phase'] = 'stopping'
        self._control('stop', reason=self._status['reason'], failed=failed)
        await self._stop_callbacks()
        if self.runtime['source'] == 'public' and not failed:
            await self._cancel_orders()
            deadline = time.monotonic() + self.runtime['shutdown_grace_seconds']
            while self._unresolved() and time.monotonic() < deadline:
                await self._drive(allow_work=False)
                await self._session.engine.retry_reports()
                await asyncio.sleep(.002)
            if self._unresolved():
                raise RuntimeError('shutdown grace expired with unresolved orders or undelivered reports')
        self._control('drain', unresolved=bool(self._unresolved()), finite_boundary=self.runtime['source'] == 'synthetic')

    async def _close_producers(self):
        error = None
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            done, pending = await asyncio.wait(self._tasks, timeout=self.runtime['shutdown_grace_seconds'])
            for task in done:
                if not task.cancelled():
                    task.exception()
            if pending:
                error = RuntimeError('public producer cleanup exceeded shutdown grace')
        for feed in self.feeds or ():
            if callable(getattr(feed, 'aclose', None)):
                try:
                    await asyncio.wait_for(feed.aclose(), self.runtime['shutdown_grace_seconds'])
                except BaseException as caught:
                    if error is None:
                        error = caught
                    else:
                        error.add_note('feed close: ' + redact(caught))
        if error is not None:
            raise error
        if self._producer_error is not None:
            raise self._producer_error

    async def run(self, duration=None):
        if self._used:
            raise RuntimeError('BotRunner.run is single-use')
        if duration is not None:
            positive(duration, 'duration')
        self._used = True
        started = time.monotonic()
        error = None
        summary = None
        log_handler = None
        self._logger = logging.getLogger('paper-runtime-' + str(id(self)))
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        try:
            self.output_directory.mkdir(parents=True, exist_ok=False)
            log_handler = BoundedLogHandler(self.output_directory / 'runtime.log',
                max_records=min(64, self.config['execution'].get('max_outbox', 128)),
                max_bytes=min(self.limits['memory_budget_bytes'] // 32, self.limits['max_event_bytes']),
                max_record_bytes=min(16384, self.limits['max_event_bytes']),
                timeout=self.runtime['shutdown_grace_seconds'])
            log_handler.setFormatter(SafeFormatter('%(levelname)s %(message)s'))
            self._logger.addHandler(log_handler)
            from .history import HistoryStore
            self._store = HistoryStore(self.output_directory, limits=self.limits,
                page_size=max(1, min(64, self.limits['memory_budget_bytes'] // (self.limits['max_event_bytes'] * 64))),
                request_capacity=8, timeout=self.runtime['shutdown_grace_seconds'])
            config = trading_config(self.config)
            config.strategy_portfolios = {slot.entry['name']: 'PAPER' for slot in self.slots}
            self._session = PaperSession(config, self.clock, self.output_directory,
                history_store=self._store, limits=self.limits, runtime_config=self.config)
            if self._operator_controls:
                self._session.recorder.schema_version = 4
                self._session.recorder.manifest['schema_version'] = 4
            if self._operator_observer is not None:
                original_sink = self._session.engine.audit_sink
                def observe_execution(row):
                    original_sink(row)
                    self._observe_operator('execution', row)
                self._session.engine.audit_sink = observe_execution
            self._session.after_public = self._after_public
            if self.feeds is None:
                if self.runtime['source'] != 'public':
                    raise ValueError('Synthetic runs require explicit offline fixture feeds')
                self.feeds = await _create_public_feeds(self.config['feeds'], self.limits,
                    timeout=self.runtime['shutdown_grace_seconds'])
            sequencer = ReceiptSequencer()
            # Reserve finite buffers from the byte budget; no producer can enqueue unbounded events.
            queue_limit = min(config.paper_execution.max_pending_events,
                max(1, self.limits['memory_budget_bytes'] // 1024))
            self._queue = asyncio.Queue(maxsize=queue_limit)
            for feed in self.feeds:
                feed.sequencer = sequencer
                for identifier in feed.markets:
                    market = await asyncio.wait_for(feed.get_market(identifier), self.runtime['startup_timeout_seconds'])
                    if market.exchange != feed.exchange:
                        raise ValueError('Metadata venue does not match subscription')
                    self._identity_mapping[f'{feed.exchange}:{identifier}'] = f'{market.exchange}:{market.market_id}'
                    await self._session.add_market(market)
            self._session.recorder.manifest['identity_mapping'] = deepcopy(self._identity_mapping)
            for slot in self.slots:
                slot.context = StrategyContext(slot.entry['name'], 'PAPER', slot.entry['settings'],
                    self.clock, config.paper_execution.seed, config.paper_execution.session_id)
                slot.strategy = self.factories[slot.entry['factory']](slot.context)
                if not callable(getattr(slot.strategy, 'on_snapshot', None)) or not inspect.iscoroutinefunction(slot.strategy.on_snapshot):
                    raise ValueError('Strategy factory must return an async on_snapshot implementation')
            self._tasks = [asyncio.create_task(callback(feed))
                           for feed in self.feeds for callback in (self._collect, self._refresh)]
            # Transfer control to every newly owned producer before checking
            # deadlines that setup may already have consumed. Even an empty
            # driver pass can complete without yielding; cancelling an unstarted
            # collector would bypass its stream's initialization/finally path.
            await asyncio.sleep(0)
            next_risk = time.monotonic()
            while True:
                if self._stop.is_set():
                    self._status['reason'] = 'requested stop'
                    break
                if duration is not None and time.monotonic() - started >= duration:
                    self._status['reason'] = 'duration elapsed'
                    break
                await self._drive()
                now = time.monotonic()
                if not self._initial_ready and now - started >= self.runtime['startup_timeout_seconds']:
                    raise RuntimeError('startup timed out awaiting verified subscriptions')
                if self._stop.is_set():
                    self._status['reason'] = 'requested stop'
                    break
                if duration is not None and now - started >= duration:
                    self._status['reason'] = 'duration elapsed'
                    break
                if self._ended == set(self.config['feeds']) and self._queue.empty():
                    if not self._initial_ready:
                        raise RuntimeError('Synthetic data ended before startup readiness')
                    # Let callbacks already dispatched at the final input finish; no new input is invented.
                    if not any(slot.task is not None for slot in self.slots):
                        self._status['reason'] = 'synthetic data boundary'
                        break
                if now >= next_risk:
                    await self._risk()
                    next_risk = now + self.runtime['risk_interval_seconds']
                self._wake.clear()
                if not self._queue.empty():
                    await asyncio.sleep(0)
                else:
                    try:
                        await asyncio.wait_for(self._wake.wait(), min(.005, max(.001, next_risk-time.monotonic())))
                    except asyncio.TimeoutError:
                        pass
            await self._shutdown()
        except BaseException as caught:
            error = caught
            self._status['reason'] = redact(f'{type(caught).__name__}: {caught}')
            if log_handler is not None:
                try:
                    self._logger.exception('runtime failed')
                except Exception as logging_error:
                    caught.add_note('logging failure: ' + redact(logging_error))
            if self._session is not None and not self._stopping:
                try:
                    await self._shutdown(failed=True)
                except BaseException as cleanup_error:
                    caught.add_note('shutdown failure: ' + redact(cleanup_error))
        finally:
            try:
                await self._close_producers()
            except BaseException as caught:
                if error is None:
                    error = caught
                else:
                    error.add_note('producer cleanup: ' + redact(caught))
            if self._session is not None:
                try:
                    self._control('final_reports', failed=error is not None)
                    summary = await asyncio.wait_for(self._session.finish(self.output_directory,
                        finalize_capture=False), self.runtime['shutdown_grace_seconds'])
                except BaseException as caught:
                    if error is None:
                        error = caught
                    else:
                        error.add_note('artifact finalization: ' + redact(caught))
                    recorder = self._session.recorder
                    if recorder and not recorder.handle.closed:
                        try:
                            recorder.close(self.clock(), finalized=False)
                        except BaseException as close_error:
                            error.add_note('capture close: ' + redact(close_error))
                            try:
                                recorder.handle.close()
                            except BaseException as handle_error:
                                error.add_note('capture handle close: ' + redact(handle_error))
                    if self._session.audit_file and not self._session.audit_file.closed:
                        try:
                            self._session.audit_file.close()
                        except BaseException as close_error:
                            error.add_note('audit close: ' + redact(close_error))
            if self._store is not None:
                try:
                    await self._store.aclose()
                except BaseException as caught:
                    if error is None:
                        error = caught
                    else:
                        error.add_note('history close: ' + redact(caught))
            if log_handler is not None:
                self._logger.removeHandler(log_handler)
                try:
                    log_handler.close(timeout=self.runtime['shutdown_grace_seconds'])
                except BaseException as caught:
                    if error is None:
                        error = caught
                    else:
                        error.add_note('logging close: ' + redact(caught))
        self._status['phase'] = 'failed' if error is not None else 'stopped'
        if error is not None:
            self._status['reason'] = redact(f'{type(error).__name__}: {error}')
        outcome = dict(status=self.status, limits=deepcopy(self.limits),
                       source=self.runtime['source'], clean=error is None,
                       admitted_memory_highwater_bytes=self._memory_highwater,
                       collector_highwater_bytes=self._event_bytes_highwater)
        try:
            if summary is not None:
                summary['runtime'] = outcome
                (self.output_directory / 'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
            (self.output_directory / 'outcome.json').write_text(json.dumps(outcome, indent=2, allow_nan=False) + '\n')
            if error is None and self._session and self._session.recorder:
                self._session.recorder.finalize()
        except BaseException as caught:
            if error is None:
                error = caught
                self._status.update(phase='failed', reason=redact(caught))
                outcome.update(status=self.status, clean=False)
                try:
                    (self.output_directory / 'outcome.json').write_text(json.dumps(outcome, indent=2, allow_nan=False) + '\n')
                    if summary is not None:
                        summary['runtime'] = outcome
                        (self.output_directory / 'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
                except BaseException as outcome_error:
                    error.add_note('failed outcome write: ' + redact(outcome_error))
            else:
                error.add_note('outcome write: ' + redact(caught))
        if error is not None:
            raise error
        return summary
