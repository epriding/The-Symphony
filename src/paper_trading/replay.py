"""Streaming replay of driver steps and the decisions made within those steps."""
from .clock import VirtualClock
from .recording import read_capture, decode
from .session import PaperSession
from ..utilities.classes import TradingConfig


class _ReplayInputs:
    """One-record lookahead lets callbacks consume their recorded decisions."""
    def __init__(self, records):
        self.records = iter(records)
        self.next_record = next(self.records, None)

    def pop(self):
        record = self.next_record
        self.next_record = next(self.records, None)
        return record


async def _apply_input(session, record):
    kind = record["kind"]
    payload = record["payload"]
    if kind == "public":
        session.publish(payload)
    elif kind == "public_immediate":
        await session.apply_public_immediately(payload)
    elif kind == "market":
        await session.add_market(payload)
    elif kind == "intent":
        await session.submit(payload)
    elif kind == "batch":
        await session.submit_batch(payload)
    elif kind == 'order_update':
        await session._apply_cumulative_update(payload)
    elif kind == "cancel":
        await session.cancel(**payload)
    elif kind == "advance":
        await session.advance(**payload)
    elif kind == 'abort':
        from ..utilities.classes import OrderStatus
        order = session.engine.orders.get(payload['order_id'])
        if not order or order.phase not in ('pending', 'held') or order.dispatched_at is not None:
            raise ValueError('Captured unsent abort does not match replay state')
        session.record('abort', payload)
        session.engine._finish(order, OrderStatus.CANCELED, 'unsent_local_abort')
    else:
        raise ValueError(f"Unsupported capture input {kind}")


class _RecordedDecisions:
    def __init__(self, session, inputs):
        self.session = session
        self.inputs = inputs

    async def __call__(self):
        while self.inputs.next_record is not None:
            record = self.inputs.next_record
            if record.get("origin") != "strategy" or record.get("cause") != self.session._strategy_event:
                break
            self.inputs.pop()
            self.session.clock.advance(max(self.session.clock(), record["at"]))
            await _apply_input(self.session, record)


async def replay(directory, output_directory=None, strategy=None, execution_config=None, strategy_factory=None):
    manifest, records = read_capture(directory)
    if manifest['schema_version'] == 3:
        if strategy is not None or strategy_factory is not None or execution_config is not None:
            raise ValueError('Schema 3 fresh strategies use the private runtime replay registry')
        if output_directory is None:
            raise ValueError('Schema 3 replay requires a new output directory')
        return await _replay_runtime_session(directory, output_directory, manifest=manifest, records=records)
    settings = decode(manifest["settings"])
    config = TradingConfig()
    for key, value in settings.items():
        setattr(config, key, value)
    if execution_config is not None:
        config.paper_execution = execution_config
    session = PaperSession(config, VirtualClock(manifest["started_at"]), audit_directory=output_directory)
    if strategy is not None and strategy_factory is not None:
        raise ValueError("Use a strategy instance or factory, not both")
    session.strategy = strategy_factory(session) if strategy_factory else strategy
    replay_decisions = session.strategy is None
    if manifest["schema_version"] == 1:
        # Older captures have no driver boundaries. Honor their recorded file
        # order instead of letting later same-time public rows overtake intents.
        for record in records:
            await session.advance(record["at"])
            if replay_decisions or record["kind"] not in {"intent", "batch", "cancel"}:
                await _apply_input(session, record)
        await session.advance(manifest["ended_at"])
    else:
        inputs = _ReplayInputs(records)
        if replay_decisions:
            session._replay_decisions = _RecordedDecisions(session, inputs)
        while inputs.next_record is not None:
            record = inputs.pop()
            if record.get("origin") == "strategy":
                if replay_decisions:
                    raise ValueError("Recorded decision has no matching public callback")
                continue  # A fresh strategy supplies its own decisions.
            session.clock.advance(max(session.clock(), record["at"]))
            await _apply_input(session, record)
        # The explicit advances already ran all determined work. Do not create
        # a final pump that could execute arrivals left queued at capture end.
        session.clock.advance(max(session.clock(), manifest["ended_at"]))
        if replay_decisions:
            session._replay_decisions = None
    summary = await session.finish(output_directory)
    return session, summary


async def replay_runtime(capture_directory, output_directory, *, strategy_factories=None, pacing=None):
    """Private causal replay seam; public replay_bot uses recorded decisions only.

    ``pacing(record)`` may await observation delays but cannot change session time.
    Supplying a registry enables fresh callbacks and compares their answers at
    recorded completion boundaries before applying any of those decisions.
    """
    _, summary = await _replay_runtime_session(capture_directory, output_directory,
        strategy_factories=strategy_factories, pacing=pacing)
    return summary


async def _replay_runtime_session(directory, output_directory, *, strategy_factories=None,
                                  pacing=None, manifest=None, records=None):
    import asyncio
    from collections import deque
    from copy import deepcopy
    from dataclasses import replace
    import inspect
    import json
    from pathlib import Path
    from .history import HistoryStore
    from .recording import encode
    from .runtime import BotRunner
    from .runtime_config import COMPATIBILITY_LIMITS, validate_output_directory
    from ..strategies.context import StrategyContext
    from ..utilities.logger import redact

    if manifest is None:
        manifest, records = read_capture(directory)
    output = validate_output_directory(output_directory)
    runtime_config = deepcopy(manifest.get('runtime_config'))
    limits = (runtime_config['runtime']['limits'] if runtime_config else dict(COMPATIBILITY_LIMITS))
    if strategy_factories is not None and runtime_config is None:
        raise ValueError('Fresh runtime strategy replay requires a schema 3 capture')
    settings = decode(manifest['settings'])
    config = TradingConfig()
    for key, value in settings.items():
        setattr(config, key, value)
    if runtime_config is None:
        # Older captures keep their driver/file-order semantics, with finite archive budgets.
        runtime_config = None
    else:
        for entry in runtime_config['strategies']:
            if entry['enabled'] and strategy_factories is not None and not callable(strategy_factories.get(entry['factory'])):
                raise ValueError('Fresh replay requires every recorded strategy factory')
    output.mkdir(parents=True, exist_ok=False)
    store = None
    session = None
    tasks = {}
    contexts, strategies = {}, {}
    pending_intents = deque()
    error = None
    summary = None
    grace = runtime_config['runtime']['shutdown_grace_seconds'] if runtime_config else 5.0
    timeout = runtime_config['runtime']['strategy_timeout_seconds'] if runtime_config else 5.0
    try:
        store = HistoryStore(output, limits=limits, timeout=grace)
        session = PaperSession(config, VirtualClock(manifest['started_at']),
            capture_directory=output if runtime_config else None, audit_directory=output,
            history_store=store, limits=limits, runtime_config=runtime_config)
        if runtime_config:
            session.recorder.manifest['identity_mapping'] = deepcopy(manifest.get('identity_mapping', {}))
        if strategy_factories is not None:
            for entry in runtime_config['strategies']:
                if not entry['enabled']:
                    continue
                name = entry['name']
                context = StrategyContext(name, config.strategy_portfolios[name], entry['settings'],
                    session.clock, config.paper_execution.seed, config.paper_execution.session_id)
                contexts[name] = context
                strategies[name] = strategy_factories[entry['factory']](context)

        async def compact():
            if hasattr(session.engine, 'compact'):
                await session.engine.compact()
            if hasattr(session.reporter, 'observe_async'):
                await session.reporter.observe_async()

        if manifest['schema_version'] in (1, 2):
            if manifest['schema_version'] == 1:
                for record in records:
                    if pacing:
                        result = pacing(record)
                        if inspect.isawaitable(result):
                            await result
                    await session.advance(record['at'])
                    await _apply_input(session, record)
                    await compact()
                await session.advance(manifest['ended_at'])
            else:
                inputs = _ReplayInputs(records)
                session._replay_decisions = _RecordedDecisions(session, inputs)
                while inputs.next_record is not None:
                    record = inputs.pop()
                    if record.get('origin') == 'strategy':
                        raise ValueError('Recorded decision has no matching public callback')
                    if pacing:
                        result = pacing(record)
                        if inspect.isawaitable(result):
                            await result
                    session.clock.advance(max(session.clock(), record['at']))
                    await _apply_input(session, record)
                    await compact()
                session._replay_decisions = None
        else:
            for record in records:
                if pacing:
                    result = pacing(record)
                    if inspect.isawaitable(result):
                        await result
                session.clock.advance(max(session.clock(), record['at']))
                payload = record['payload']
                if record['kind'] == 'runtime_control':
                    action = payload['action']
                    if strategy_factories is not None:
                        name = payload.get('strategy')
                        if action == 'dispatch':
                            if name in tasks:
                                raise ValueError('Overlapping callback in causal capture')
                            snapshot = await session.state.snapshot()
                            snapshot = replace(snapshot, timestamp=payload['timestamp'])
                            contexts[name]._set_time(payload['timestamp'])
                            tasks[name] = asyncio.create_task(strategies[name].on_snapshot(snapshot))
                        elif action == 'completion':
                            task = tasks.pop(name, None)
                            if task is None:
                                raise ValueError('Completion without dispatch')
                            done, pending = await asyncio.wait({task}, timeout=timeout)
                            if pending:
                                task.cancel()
                                tasks[name] = task
                                raise ValueError('Fresh replay callback failed to complete at causal boundary')
                            actual = BotRunner._validated_result(task.result(), strategy_name=name,
                                markets=session.state.markets, limits=limits)
                            if encode(actual) != encode(payload['decision']):
                                raise ValueError(f'Fresh strategy decision diverged for {name}')
                            if payload['accepted']:
                                pending_intents.extend(actual)
                        elif action == 'disabled':
                            task = tasks.pop(name, None)
                            if task is None:
                                raise ValueError('Disabled callback without dispatch')
                            if not task.done() and not payload['reason'].startswith('TimeoutError:'):
                                done, pending = await asyncio.wait({task}, timeout=timeout)
                                if pending:
                                    tasks[name] = task
                                    raise ValueError('Fresh strategy did not fail at recorded boundary')
                            if task.done():
                                try:
                                    BotRunner._validated_result(task.result(), strategy_name=name,
                                        markets=session.state.markets, limits=limits)
                                except (Exception, asyncio.CancelledError) as failure:
                                    reason = redact(f'{type(failure).__name__}: {failure}')
                                    if reason != payload['reason']:
                                        raise ValueError('Fresh strategy failure reason diverged') from failure
                                else:
                                    raise ValueError('Fresh strategy did not reproduce recorded failure')
                            task.cancel()
                            done, pending = await asyncio.wait({task}, timeout=grace)
                            if pending:
                                tasks[name] = task
                                raise RuntimeError('Fresh strategy cancellation exceeded grace')
                            if not task.cancelled():
                                task.exception()
                    session.record('runtime_control', payload)
                elif record['kind'] == 'intent' and pending_intents:
                    actual = pending_intents.popleft()
                    if encode(actual) != encode(payload):
                        raise ValueError('Fresh intent submission order diverged')
                    await session.submit(actual)
                else:
                    await _apply_input(session, record)
                await compact()
                await asyncio.sleep(0)
            if pending_intents:
                raise ValueError('Capture ended before all fresh decisions were submitted')
        session.clock.advance(max(session.clock(), manifest['ended_at']))
        summary = await asyncio.wait_for(session.finish(output, finalize_capture=False), grace)
    except BaseException as caught:
        error = caught
        if session is not None:
            try:
                await asyncio.wait_for(session.finish(output, finalize_capture=False), grace)
            except BaseException as cleanup_error:
                caught.add_note('replay finalization: ' + redact(cleanup_error))
    finally:
        for task in tasks.values():
            task.cancel()
        if tasks:
            done, pending = await asyncio.wait(set(tasks.values()), timeout=grace)
            for task in done:
                if not task.cancelled():
                    task.exception()
            if pending:
                cleanup_error = RuntimeError('Fresh replay callback cleanup exceeded grace')
                if error is None:
                    error = cleanup_error
                else:
                    error.add_note(str(cleanup_error))
        if session and session.recorder and not session.recorder.handle.closed:
            try:
                session.recorder.close(session.clock(), finalized=False)
            except BaseException as cleanup_error:
                if error is None:
                    error = cleanup_error
                else:
                    error.add_note('capture close: ' + redact(cleanup_error))
        if session and session.audit_file and not session.audit_file.closed:
            try:
                session.audit_file.close()
            except BaseException as cleanup_error:
                if error is None:
                    error = cleanup_error
                else:
                    error.add_note('audit close: ' + redact(cleanup_error))
        if store:
            try:
                await store.aclose()
            except BaseException as cleanup_error:
                if error is None:
                    error = cleanup_error
                else:
                    error.add_note('history close: ' + redact(cleanup_error))
        if callable(getattr(records, 'close', None)):
            records.close()
    outcome = dict(clean=error is None, source='replay', limits=deepcopy(limits),
        compatibility_profile=manifest['schema_version'] in (1, 2),
        status=dict(phase='stopped' if error is None else 'failed',
                    reason='recorded data boundary' if error is None else redact(error),
                    strategies={}, venues={}))
    try:
        if summary is not None:
            summary['runtime'] = outcome
            (output / 'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
        (output / 'outcome.json').write_text(json.dumps(outcome, indent=2, allow_nan=False) + '\n')
        if error is None and session.recorder:
            session.recorder.finalize()
    except BaseException as caught:
        if error is None:
            error = caught
            outcome['clean'] = False
            outcome['status'].update(phase='failed', reason=redact(caught))
            try:
                (output / 'outcome.json').write_text(json.dumps(outcome, indent=2, allow_nan=False) + '\n')
                if summary is not None:
                    (output / 'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
            except BaseException as outcome_error:
                error.add_note('failed outcome write: ' + redact(outcome_error))
        else:
            error.add_note('outcome write: ' + redact(caught))
    if error is not None:
        raise error
    return session, summary
