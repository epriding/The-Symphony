"""Reproducible two-venue paper trading, archive/replay and memory measurements.

Synthetic limits and zero fees are fixtures, never calibrated deployment targets.
Run each increasing workload in a fresh process/output directory for RSS evidence.
"""
import argparse
import asyncio
from collections import deque
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import resource
import sys
import time
import tracemalloc

# Also supports direct `python examples/bot_trading_benchmark.py` invocation.
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bot import BotRunner, replay_bot
from src.paper_trading.clock import VirtualClock
from src.utilities.classes import (MarketInfo, OrderBookSnapshotEvent, TradeEvent,
    OrderIntentBatch, Side, TimeInForce)
from src.utilities.market_events import PublicEventEnvelope, ReceiptSequencer

VENUES = ('POLYMARKET_US', 'KALSHI')
START = 1_800_000_000.0


def benchmark_config():
    venues = {venue: dict(fees=dict(version='synthetic-zero', model='flat', taker='0',
        maker='0', balance_precision='.01', effective_from=0), max_book_age=5,
        outbound={'seconds': 0}, acknowledgement={'seconds': 0},
        execution_report={'seconds': 0}, cancel={'seconds': 0}, request_rate=1000,
        request_capacity=1000, rate_policy='reject', netted_outcomes=True) for venue in VENUES}
    return dict(strategy_portfolios={'workload': 'PAPER'},
        starting_balances={'PAPER': {venue: 1000 for venue in VENUES}},
        risk={'PAPER': dict(diversification={'other': 1}, max_position_exposure=.5,
             max_total_exposure=10, max_quote_age_seconds=5, stop_loss_pct=100)},
        execution=dict(seed=7, session_id='bounded-trading-benchmark',
             assumptions='Synthetic zero-fee workload; uncalibrated',
             processing_delay={'seconds': 0}, max_pending_events=64, max_outbox=32,
             markout_horizons=[.01, .05], venues=venues),
        feeds={venue: dict(markets=['m'], read_interval=.01,
             market_rules={'m': dict(tick_size='.01', quantity_step='1', min_quantity='1')}) for venue in VENUES},
        strategies=[dict(name='workload', factory='workload', enabled=True,
             required_markets=[f'{v}:m' for v in VENUES], settings={})],
        runtime=dict(source='synthetic', observe_only=False, startup_timeout_seconds=2,
             strategy_timeout_seconds=2, risk_interval_seconds=.02,
             shutdown_grace_seconds=.5, limits=dict(max_strategies=2, max_markets=2,
             max_active_orders=8, max_positions=4, max_intents_per_callback=4,
             max_hot_history_records=4, max_archive_cache_records=4,
             max_event_bytes=262144, memory_budget_bytes=64*1024*1024)))


class _Feed:
    def __init__(self, venue, clock):
        self.exchange, self.clock = venue, clock
        self.markets, self.sequencer = ['m'], ReceiptSequencer()
        self.queue, self.started = asyncio.Queue(maxsize=8), asyncio.Event()
        self.emitted = 0

    async def get_market(self, market_id):
        return MarketInfo(market_id, self.exchange, market_id, {}, True,
            {'YES': True, 'NO': True}, tick_size=.01, quantity_step=1,
            min_quantity=1, fee_per_contract=0, resolution_date=START+10_000_000)

    async def stream(self):
        self.started.set()
        while True:
            event = await self.queue.get()
            try:
                if event is None:
                    return
                self.emitted += 1
                receipt = self.sequencer.receipt()
                receipt['received_at'] = self.clock()
                yield PublicEventEnvelope(event=event, available_at=self.clock(),
                    **receipt, source_id=f'{self.exchange}-{self.emitted}', connection_epoch=0)
            finally:
                self.queue.task_done()

    async def settlements(self):
        if False:
            yield None


class _RoundTrips:
    def __init__(self, cycles):
        self.target, self.calls = cycles, 0
        self.counts = {venue: 0 for venue in VENUES}
        self.legs = {venue: 'buy' for venue in VENUES}
        self.context = None

    def factory(self, context):
        self.context = context
        return self

    @property
    def done(self):
        return all(n == self.target for n in self.counts.values())

    async def on_snapshot(self, snapshot):
        self.calls += 1
        account = snapshot.portfolios[self.context.portfolio_id]
        results = []
        for venue in VENUES:
            position = account.positions.get(f'{venue}:m:YES')
            pending = any(order.exchange == venue for order in account.pending_orders)
            leg = self.legs[venue]
            if leg == 'await_buy' and position is not None and not pending:
                self.legs[venue] = 'sell'
            elif leg == 'await_sell' and position is None and not pending:
                self.counts[venue] += 1
                self.legs[venue] = 'buy'
            if self.counts[venue] >= self.target:
                continue
            values = dict(market_id='m', exchange=venue, outcome='YES')
            if self.legs[venue] == 'buy' and position is None and not pending:
                results.append(self.context.new_intent(**values, side=Side.BUY,
                    price=.5, estimated_probability=1,
                    resolution_date=self.context.now()+3600))
                self.legs[venue] = 'await_buy'
            elif self.legs[venue] == 'sell' and position is not None and not pending:
                results.append(self.context.new_intent(**values, side=Side.SELL,
                    price=.48, estimated_probability=None, resolution_date=None,
                    reduce_only=True))
                self.legs[venue] = 'await_sell'
        return results


def _rss():
    with Path('/proc/self/statm').open() as source:
        import os
        return int(source.read().split()[1]) * os.sysconf('SC_PAGE_SIZE')


def _memory():
    current, peak = tracemalloc.get_traced_memory()
    return dict(python_current_bytes=current, python_peak_bytes=peak, rss_bytes=_rss(),
                rss_peak_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)


def _observe(runner, high):
    session = runner._session
    if session is None:
        return
    engine, state = session.engine, session.state
    accounts = list(state.portfolios.values())
    values = dict(
        active_orders=sum(len(p.open_orders)+len(p.reservations) for p in accounts),
        positions=sum(len(p.positions) for p in accounts),
        hot_history_records=sum(sum(o.status.value in ('FILLED','CANCELED','CANCELLED','REJECTED','EXPIRED')
            for o in p.order_history.values()) for p in accounts),
        archive_cache_records=len(getattr(runner._store, '_cache', {})),
        completed_markout_cache_records=len(getattr(session.reporter, 'markouts', {})),
        archive_queued_requests=runner._store._requests.qsize(),
        pending_events=len(engine.scheduler.events),
        collector_events=runner._queue.qsize(),
        outbox=len(engine.outbox) + len(engine.credit_outbox),
        engine_orders=len(engine.orders), engine_executions=len(engine.executions),
        engine_groups=len(engine.groups), passive_queues=len(engine.queues),
        passive_segments=sum(len(queue.segments) for queue in engine.queues.values()),
        pending_reports=sum(getattr(event.callback, '__name__', '') == '_deliver'
                            for event in engine.scheduler.events),
        due_timer_backlog=sum(event.timestamp_ns <= int(session.clock()*1e9)
                              for event in engine.scheduler.events),
        timer_lag_seconds=max(0, session.clock() - (engine.scheduler.next_time or session.clock())),
        processed_ids=sum(len(getattr(p, name, ())) for p in accounts for name in
                          ('seen_executions', 'processed_intents', 'processed_batches',
                           'cash_credits', 'settlements')),
        book_levels=sum(sum(len(side) for side in book.observed.values()) for book in engine.books.values()))
    # Report each supported owner's retained container cardinality separately.
    # These samples supplement the source/capacity audit; they do not prove a
    # hard bound for arbitrary Python strategies or external transport SDKs.
    owners = [('runner', runner), ('session', session), ('engine', engine), ('state', state),
              ('reporter', session.reporter), ('scheduler', engine.scheduler),
              ('archive', runner._store)]
    owners.extend((f'portfolio.{p.portfolio_id}', p) for p in accounts)
    owners.extend((f'book.{venue}.{market}', book) for (venue, market), book in engine.books.items())
    for prefix, owner in owners:
        for name, value in vars(owner).items():
            if isinstance(value, (dict, list, tuple, set, frozenset, deque)):
                values[f'{prefix}.{name}'] = len(value)
    values['strategy_position_members'] = sum(len(items) for p in accounts
                                             for items in p.strategy_positions.values())
    values['feed_queued_events'] = sum(feed.queue.qsize() for feed in runner.feeds)
    for key, value in values.items():
        high[key] = max(high.get(key, 0), value)


async def benchmark(output_directory, *, cycles=1000):
    if type(cycles) is not int or cycles <= 0:
        raise ValueError('Positive integer cycle count required')
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=False)
    config = benchmark_config()
    clock = VirtualClock(START)
    feeds = [_Feed(venue, clock) for venue in VENUES]
    scenario = _RoundTrips(cycles)
    runner = BotRunner(config, output_directory=output/'capture', feeds=feeds,
        clock=clock, strategy_factories={'workload': scenario.factory})
    owned_trace = not tracemalloc.is_tracing()
    if owned_trace:
        tracemalloc.start()
    baseline, _ = tracemalloc.get_traced_memory()
    tracemalloc.reset_peak()
    rss_baseline = _rss()
    started = time.monotonic()
    high, samples = {}, {}
    task = asyncio.create_task(runner.run())
    try:
        await asyncio.wait_for(asyncio.gather(*(feed.started.wait() for feed in feeds)), 5)
        samples['warmup'] = _memory()
        for wave in range(cycles*16+64):
            if scenario.done:
                break
            prior = scenario.calls
            clock.advance(clock()+.05)
            for feed in feeds:
                # Reliable IDs repeat well beyond the hot-history window. IOC
                # round trips do not get additional passive liquidity from them.
                if wave % 3 == 0:
                    await feed.queue.put(TradeEvent('m','YES',clock(),feed.exchange,
                        .49,1,Side.SELL,f'print-{wave % 31}'))
                await feed.queue.put(OrderBookSnapshotEvent('m','YES',clock(),feed.exchange,
                    {.48:1000},{.50:1000}))
            deadline = time.monotonic()+5
            def settled():
                # A callback-entry count is not evidence that its intents or
                # outstanding source wave have completed. Wait for actual
                # ingestion and the serialized completion boundary before
                # advancing synthetic time or producing another input wave.
                return (scenario.calls > prior
                    and all(feed.queue.empty() for feed in feeds)
                    and runner._revision >= sum(feed.emitted for feed in feeds)
                    and runner._queue.empty() and not runner._dirty
                    and all(slot.task is None and not slot.pending for slot in runner.slots))
            while not settled() and not task.done():
                if time.monotonic() > deadline:
                    raise TimeoutError('Benchmark strategy observation deadline exceeded')
                await asyncio.sleep(.001)
                _observe(runner, high)
            if task.done():
                task.result()
                raise RuntimeError('Benchmark ended before completing economic workload')
            _observe(runner, high)
            if wave % 128 == 0:
                samples['trading'] = _memory()
        else:
            raise RuntimeError('Benchmark round trips did not complete in finite waves')
        samples['trading'] = _memory()
        for feed in feeds:
            await feed.queue.put(None)
        summary = await asyncio.wait_for(asyncio.shield(task), 30)
        samples['report'] = _memory()
        high['summary_bytes'] = len(json.dumps(summary).encode())
        replayed = await asyncio.wait_for(replay_bot(output/'capture', output/'replay'), max(30,cycles*.1))
        samples['replay'] = _memory()
        for key in ('submitted','full_fills','partial_fills','zero_fills','fees','accounts'):
            if summary[key] != replayed[key]:
                raise AssertionError(f'Replay differs in {key}')
        if summary['submitted'] != 4*cycles or summary['full_fills'] != 4*cycles:
            raise AssertionError('Workload failed to execute all intended round trips')
        current, peak = tracemalloc.get_traced_memory()
        rss_final = _rss()
        kernel_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
        # Linux statm and getrusage counters can differ slightly at a sampling
        # boundary. The observed peak includes every actual resident sample,
        # including baseline, rather than reporting less than measured residency.
        observed_rss_peak = max(kernel_peak, rss_baseline, rss_final,
            *(sample['rss_bytes'] for sample in samples.values()),
            *(sample['rss_peak_bytes'] for sample in samples.values()))
        result = dict(cycles=cycles, summary=summary, replay_summary=replayed,
            memory=dict(budget_bytes=config['runtime']['limits']['memory_budget_bytes'],
                python_baseline_bytes=baseline, python_peak_delta_bytes=peak-baseline,
                rss_baseline_bytes=rss_baseline, rss_peak_bytes=observed_rss_peak,
                kernel_peak_rss_bytes=kernel_peak, rss_final_bytes=rss_final,
                rss_measurement_sources='Linux statm resident samples and getrusage peak, maximum observed',
                samples=samples), high_water=high,
            archive_bytes=sum(p.stat().st_size for p in output.rglob('history.sqlite3')),
            detail_records={item['kind']:item['records'] for item in summary['history']['details']},
            elapsed_seconds=time.monotonic()-started)
        (output/'measurements.json').write_text(json.dumps(result,indent=2)+'\n')
        return result
    finally:
        if not task.done():
            runner.request_stop()
            done, pending = await asyncio.wait({task},timeout=3)
            if pending:
                task.cancel()
                done, pending = await asyncio.wait({task},timeout=3)
                if pending:
                    raise RuntimeError('Benchmark cleanup deadline exceeded')
            await asyncio.gather(*done,return_exceptions=True)
        if owned_trace:
            tracemalloc.stop()


class _DiagnosticDriver(BotRunner):
    """Serialize offline diagnostic commands with the real runtime driver.

    The registered strategy only observes. The harness supplies explicit batch,
    cancel and cumulative-report inputs through existing session/coordinator
    APIs; this is an execution/accounting workload, not a trading algorithm.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commands = asyncio.Queue(maxsize=1)

    async def _drive(self, *, allow_work=True):
        await super()._drive(allow_work=allow_work)
        if allow_work and self._initial_ready and not self.commands.empty():
            callback, future = self.commands.get_nowait()
            try:
                result = await callback(self._session)
            except BaseException as error:
                if not future.done():
                    future.set_exception(error)
                raise
            else:
                if not future.done():
                    future.set_result(result)
            finally:
                self.commands.task_done()


class _ObserveWorkload:
    async def on_snapshot(self, snapshot):
        return None


async def stress_benchmark(output_directory, *, cycles=1000):
    """Measure bounded two-venue execution, archived corrections and replay.

    Every cycle executes a two-leg group, shallow IOC partial fills, confirmed
    passive-order cancellations, delayed private reports and reductions that
    retain one contract per venue. Cumulative cost reports correct archived
    group entries after those reductions; repeated reports and old IDs check
    idempotency. All economic mutations are recorded causal session inputs.
    """
    if type(cycles) is not int or cycles <= 0:
        raise ValueError('Positive integer cycle count required')
    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=False)
    config = benchmark_config()
    config['execution']['session_id'] = 'bounded-stress-benchmark'
    config['execution']['assumptions'] = 'Synthetic diagnostic driver; zero-fee execution and cost corrections, uncalibrated'
    config['risk']['PAPER'].update(max_position_exposure=10, max_total_exposure=25)
    for settings in config['execution']['venues'].values():
        settings.update(outbound={'seconds': .01}, acknowledgement={'seconds': .10},
                        execution_report={'seconds': .10}, cancel={'seconds': .05})
    # Archive queries and finalization have finite deadlines without tying the
    # synthetic workload's allowable duration to the legacy short CI preset.
    config['runtime']['shutdown_grace_seconds'] = 5
    clock = VirtualClock(START)
    feeds = [_Feed(venue, clock) for venue in VENUES]
    runner = _DiagnosticDriver(config, output_directory=output/'capture', feeds=feeds,
        clock=clock, strategy_factories={'workload': lambda context: _ObserveWorkload()})
    owned_trace = not tracemalloc.is_tracing()
    if owned_trace:
        tracemalloc.start()
    baseline, _ = tracemalloc.get_traced_memory()
    tracemalloc.reset_peak()
    rss_baseline, started = _rss(), time.monotonic()
    high, samples = {}, {}
    counts = dict(groups=0, partial_orders=0, cancel_requests=0, cancel_confirmations=0, corrections=0,
                  duplicate_corrections=0, duplicate_intents=0, duplicate_batches=0,
                  duplicate_prints=0, archived_correction_targets=0)
    task = asyncio.create_task(runner.run())
    wave_number, first_batch = 0, None

    async def settled():
        deadline = time.monotonic() + 10
        while True:
            if task.done():
                task.result()
                raise RuntimeError('Stress driver ended before its workload completed')
            _observe(runner, high)
            if (all(feed.queue.empty() for feed in feeds)
                    and runner._queue.empty()
                    and runner._revision >= sum(feed.emitted for feed in feeds)
                    and not runner._dirty
                    and all(slot.task is None and not slot.pending for slot in runner.slots)
                    and (runner._session.engine.scheduler.next_time is None
                         or runner._session.engine.scheduler.next_time > runner.clock())):
                return
            if time.monotonic() >= deadline:
                raise TimeoutError('Stress driver transition deadline exceeded')
            await asyncio.sleep(.001)

    async def command(callback):
        future = asyncio.get_running_loop().create_future()
        await runner.commands.put((callback, future))
        runner._wake.set()
        done, _ = await asyncio.wait({future, task}, timeout=10,
                                     return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            task.result()
            raise RuntimeError('Stress command lost its runtime owner')
        if future not in done:
            raise TimeoutError('Stress command deadline exceeded')
        result = future.result()
        await settled()
        return result

    async def wave(depth=100, *, delta=.05, duplicate=False):
        nonlocal wave_number
        clock.advance(clock()+delta)
        wave_number += 1
        for feed in feeds:
            await feed.queue.put(TradeEvent('m', 'YES', clock(), feed.exchange,
                .8, 1, Side.BUY, 'stress-print-1' if duplicate else f'stress-print-{wave_number}'))
            await feed.queue.put(OrderBookSnapshotEvent('m', 'YES', clock(), feed.exchange,
                {.49: 100} if depth else {}, {.50: depth} if depth else {}))
        await settled()
        if duplicate:
            counts['duplicate_prints'] += len(feeds)

    def entry(session, venue, *, tif=TimeInForce.FOK, price=.5):
        return session.new_intent(strategy='workload', market_id='m', exchange=venue,
            outcome='YES', side=Side.BUY, price=price, estimated_probability=1,
            resolution_date=session.clock()+3600, tif=tif)

    async def batch_command(session):
        batch = OrderIntentBatch(legs=[entry(session, venue) for venue in VENUES],
            quantity_ratios=[1, 1], capital_ceiling=2,
            batch_id=f'stress-group-{counts["groups"]}',
            execution_policy='paper_independent_legs', execution_deadline=session.clock()+1)
        result = await session.submit_batch(batch)
        if not result.decision.approved or len(result.submission_ids) != 2:
            raise AssertionError(f'Stress group rejected: {result.decision.reason}')
        return batch, tuple(zip(VENUES, result.submission_ids))

    async def submit_pair(session, *, passive=False):
        orders = []
        for venue in VENUES:
            intent = entry(session, venue, tif=TimeInForce.GTC if passive else TimeInForce.IOC,
                           price=.47 if passive else .5)
            result = await session.submit(intent)
            if not result.decision.approved or len(result.submission_ids) != 1:
                raise AssertionError(f'Stress entry rejected: {result.decision.reason}')
            orders.append((venue, result.submission_ids[0]))
        return orders

    async def reduce(session):
        for venue in VENUES:
            held = session.state.portfolios['PAPER'].positions[f'{venue}:m:YES']
            intent = session.new_intent(strategy='workload', market_id='m', exchange=venue,
                outcome='YES', side=Side.SELL, price=.49, reduce_only=True,
                close_fraction=(held.size-1)/held.size)
            result = await session.submit(intent)
            if not result.decision.approved:
                raise AssertionError(f'Stress reduction rejected: {result.decision.reason}')

    try:
        await asyncio.wait_for(asyncio.gather(*(feed.started.wait() for feed in feeds)), 5)
        await wave()
        samples['startup'] = _memory()
        for cycle in range(cycles):
            # Withdraw/replenish depth to give the next group a known fresh
            # quantity, rather than replenishing already consumed liquidity by
            # repeating an unchanged snapshot.
            await wave(0)
            await wave(100)
            batch, targets = await command(batch_command)
            if first_batch is None:
                first_batch = batch
            counts['groups'] += 1
            await wave(100, delta=.05)
            await wave(100, delta=.15)
            await wave(0)
            await wave(2)
            partials = await command(submit_pair)
            await wave(2, delta=.05)
            await wave(2, delta=.15)
            async def verify_partial(session):
                for venue, oid in partials:
                    identity = f'{venue}:{oid}'
                    record = session.state.portfolios['PAPER'].order_history.get(identity)
                    if record is None:
                        record = await runner._store.aget('orders:PAPER', identity)
                    if not record or not 0 < record.filled_quantity < record.size:
                        raise AssertionError('Shallow IOC did not produce a real partial fill')
            await command(verify_partial)
            counts['partial_orders'] += len(partials)
            passive = await command(lambda session: submit_pair(session, passive=True))
            await wave(2, delta=.05, duplicate=True)
            async def cancel_pair(session):
                for venue, oid in passive:
                    if session.engine.orders[oid].phase != 'working':
                        raise AssertionError('Passive cancellation target never rested')
                    if not await session.cancel(oid, venue, 'PAPER'):
                        raise AssertionError('Stress cancel request was rejected')
            await command(cancel_pair)
            counts['cancel_requests'] += len(passive)
            await wave(2, delta=.10)
            await wave(2, delta=.15)
            async def verify_cancels(session):
                for venue, oid in passive:
                    identity = f'{venue}:{oid}'
                    record = session.state.portfolios['PAPER'].order_history.get(identity)
                    if record is None:
                        record = await runner._store.aget('orders:PAPER', identity)
                    if not record or record.status.value != 'CANCELED' or record.reason != 'cancel_confirmed' or record.filled_quantity:
                        raise AssertionError('Passive cancellation was not confirmed without a fill')
                    counts['cancel_confirmations'] += 1
            await command(verify_cancels)
            await command(reduce)
            await wave(2, delta=.05)
            await wave(2, delta=.15)
            samples['trading'] = _memory()

            async def correct_archived(session):
                for venue, oid in targets:
                    identity = f'{venue}:{oid}'
                    if identity in session.state.portfolios['PAPER'].order_history:
                        raise AssertionError('Correction target did not leave the hot-history window')
                    record = await runner._store.aget('orders:PAPER', identity)
                    if record is None or record.filled_quantity != 2:
                        raise AssertionError('Archived group entry was not filled')
                    correction = replace(record, revision=record.revision+1,
                        average_fill_price=.48, cumulative_notional=str(Decimal('0.48')*Decimal(str(record.filled_quantity))))
                    await session._apply_cumulative_update(correction)
                    await session._apply_cumulative_update(correction)
                    counts['archived_correction_targets'] += 1
                    counts['corrections'] += 1
                    counts['duplicate_corrections'] += 1
                duplicate = await session.submit_batch(first_batch)
                if duplicate.decision.reason != 'duplicate_batch':
                    raise AssertionError('Old grouped ID was not rejected idempotently')
                counts['duplicate_batches'] += 1
                duplicate = await session.submit(first_batch.legs[0])
                if duplicate.decision.reason != 'duplicate_intent':
                    raise AssertionError('Old intent ID was not rejected idempotently')
                counts['duplicate_intents'] += 1
                account = session.state.portfolios['PAPER']
                if len(account.positions) != 2 or any(p.size != 1 for p in account.positions.values()):
                    raise AssertionError('Stress reductions did not retain exactly one contract per venue')
            await command(correct_archived)
            samples['correction'] = _memory()
            if cycle == 0:
                samples['warmup'] = _memory()
            if cycle == cycles // 2:
                samples['midpoint'] = _memory()
        await wave(2, delta=.2)

        async def query(session):
            await session.engine.compact(force=True)
            result = dict(executions=0, corrections=0, groups=0, max_page_records=0)
            async for row in runner._store.aiter_records('executions'):
                result['corrections' if row['kind'] == 'correction' else 'executions'] += 1
                _observe(runner, high)
            async for page in runner._store.aiter_pages('orders:PAPER'):
                result['max_page_records'] = max(result['max_page_records'], len(page))
                _observe(runner, high)
            result['groups'] = await runner._store.acount('groups')
            return result
        queries = await command(query)
        if queries['executions'] != 6*cycles or queries['corrections'] != 2*cycles or queries['groups'] != cycles:
            raise AssertionError('Paged queries differ from committed execution/correction/group totals')
        samples['query'] = _memory()
        _observe(runner, high)
        scheduler_metrics = runner._session.engine.scheduler.processing_metrics()
        for feed in feeds:
            await feed.queue.put(None)
        summary = await asyncio.wait_for(asyncio.shield(task), max(30, cycles*.1))
        samples['report'] = _memory()
        high['summary_bytes'] = len(json.dumps(summary).encode())
        from src.paper_trading.replay import _replay_runtime_session
        # Traced diagnostic replay includes archival and cost reconstruction.
        # The measured eight-cycle run takes about eight seconds here; retain
        # finite headroom as workload grows. Runtime/worker deadlines stay fixed.
        replay_session, replayed = await asyncio.wait_for(
            _replay_runtime_session(output/'capture', output/'replay'), max(30, cycles*2))
        samples['replay'] = _memory()
        def inventory(session):
            return {key: dict(size=position.size, entry_price=position.entry_price)
                    for key, position in session.state.portfolios['PAPER'].positions.items()}
        retained, replay_inventory = inventory(runner._session), inventory(replay_session)
        if retained != replay_inventory:
            raise AssertionError('Stress replay differs in retained inventory cost basis')
        for key in ('submitted', 'full_fills', 'partial_fills', 'zero_fills', 'fees', 'accounts', 'history'):
            if summary[key] != replayed[key]:
                raise AssertionError(f'Stress replay differs in {key}')
        if (summary['submitted'], summary['full_fills'], summary['partial_fills'], summary['zero_fills']) != (8*cycles, 4*cycles, 2*cycles, 2*cycles):
            raise AssertionError('Stress workload execution totals differ from its economic scenarios')
        current, peak = tracemalloc.get_traced_memory()
        rss_final = _rss()
        kernel_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
        elapsed = time.monotonic()-started
        result = dict(workload='diagnostic-stress', cycles=cycles, summary=summary,
            replay_summary=replayed, retained_inventory=retained,
            replay_inventory=replay_inventory, workload_counts=counts, queries=queries,
            memory=dict(budget_bytes=config['runtime']['limits']['memory_budget_bytes'],
                python_baseline_bytes=baseline, python_peak_delta_bytes=peak-baseline,
                rss_baseline_bytes=rss_baseline, rss_final_bytes=rss_final,
                kernel_peak_rss_bytes=kernel_peak,
                rss_peak_bytes=max(kernel_peak, rss_baseline, rss_final,
                    *(max(sample['rss_bytes'], sample['rss_peak_bytes']) for sample in samples.values())),
                rss_measurement_sources='Linux statm resident samples and getrusage peak, maximum observed',
                samples=samples), high_water=high,
            archive_bytes=sum(path.stat().st_size for path in output.rglob('history.sqlite3')),
            detail_records={item['kind']: item['records'] for item in summary['history']['details']},
            scheduler_metrics=scheduler_metrics, elapsed_seconds=elapsed,
            cycles_per_second=cycles/elapsed)
        (output/'measurements.json').write_text(json.dumps(result, indent=2)+'\n')
        return result
    finally:
        if not task.done():
            runner.request_stop()
            done, pending = await asyncio.wait({task}, timeout=5)
            if pending:
                task.cancel()
                done, pending = await asyncio.wait({task}, timeout=5)
                if pending:
                    raise RuntimeError('Stress benchmark cleanup deadline exceeded')
            await asyncio.gather(*done, return_exceptions=True)
        if owned_trace:
            tracemalloc.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--cycles', type=int, default=1000)
    parser.add_argument('--workload', choices=('roundtrip', 'stress'), default='roundtrip')
    args = parser.parse_args()
    workload = stress_benchmark if args.workload == 'stress' else benchmark
    result = asyncio.run(workload(args.output,cycles=args.cycles))
    print(json.dumps({key:result[key] for key in ('cycles','memory','high_water','archive_bytes','elapsed_seconds')},indent=2))


if __name__ == '__main__':
    main()
