"""Operator controls exercise the real single-owner runner and causal replay."""
import asyncio
import json

import pytest

from src.bot import BotRunner, replay_bot
from src.paper_trading.clock import VirtualClock
from src.paper_trading.replay import replay_runtime
from src.utilities.classes import Side
from bot_v1_support import (
    START, US, VENUES, ControlledFeed, eventually, strategy_config, valid_config,
)


def cancellation_result_factory(context):
    class LateResult:
        calls = 0

        async def on_snapshot(self, snapshot):
            self.calls += 1
            if self.calls != 1:
                return None
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # Returning an intent instead of respecting cancellation must
                # never bypass the operator barrier, even after resume.
                return [context.new_intent(
                    market_id='m', exchange=US, outcome='YES', side=Side.BUY,
                    price=.5, estimated_probability=1,
                    resolution_date=context.now() + 3600)]
    return LateResult()


@pytest.mark.asyncio
async def test_pause_discards_late_result_and_replays_cancellation_boundary(tmp_path):
    config = valid_config(strategies=[strategy_config('alpha', factory='late')])
    clock = VirtualClock(START)
    feeds = [ControlledFeed(venue, clock=clock) for venue in VENUES]
    runner = BotRunner(config, output_directory=tmp_path / 'capture',
        feeds=feeds, clock=clock, strategy_factories={'late': cancellation_result_factory},
        operator_controls=True)
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: all(f.started.is_set() for f in feeds) or task.done())
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: runner.slots[0].task is not None or task.done())
        runner.request_pause()
        await eventually(lambda: runner.operator_state == 'paused' or task.done())
        assert not task.done(), task.exception() if task.done() else ''
        assert runner.operator_state == 'paused'
        assert not runner._session.engine.orders
        prior_revision = runner._revision
        clock.advance(START + .1)
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: runner._revision > prior_revision)
        assert runner.slots[0].strategy.calls == 1
        runner.request_resume()
        await eventually(lambda: runner.slots[0].strategy.calls >= 2 or task.done())
        assert not runner._session.engine.orders
        runner.request_stop()
        summary = await asyncio.wait_for(task, 3)
        assert summary['submitted'] == 0
        assert json.loads((tmp_path / 'capture/manifest.json').read_text())['schema_version'] == 4
        recorded = await replay_bot(tmp_path / 'capture', tmp_path / 'recorded')
        fresh = await replay_runtime(tmp_path / 'capture', tmp_path / 'fresh',
            strategy_factories={'late': cancellation_result_factory})
        assert recorded['accounts'] == fresh['accounts'] == summary['accounts']
        assert recorded['submitted'] == fresh['submitted'] == 0
    finally:
        if not task.done():
            runner.request_stop()
            await asyncio.wait_for(task, 3)


@pytest.mark.asyncio
async def test_pause_allows_existing_orders_and_risk_ticks_to_progress(tmp_path):
    config = valid_config(strategies=[strategy_config('alpha', factory='entry')])
    clock = VirtualClock(START)
    feeds = [ControlledFeed(venue, clock=clock) for venue in VENUES]

    def factory(context):
        class Entry:
            done = False
            async def on_snapshot(self, snapshot):
                if self.done:
                    return None
                self.done = True
                return [context.new_intent(market_id='m', exchange=US,
                    outcome='YES', side=Side.BUY, price=.5,
                    estimated_probability=1, resolution_date=context.now()+3600)]
        return Entry()

    runner = BotRunner(config, output_directory=tmp_path / 'capture', feeds=feeds,
        clock=clock, strategy_factories={'entry': factory}, operator_controls=True)
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: all(f.started.is_set() for f in feeds) or task.done())
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: bool(runner._session.engine.orders) or task.done())
        runner.request_pause()
        await eventually(lambda: runner.operator_state == 'paused' or task.done())
        for increment in (.05, .10, .15):
            clock.advance(START + increment)
            prior = runner._revision
            for feed in feeds:
                await feed.push_book()
            await eventually(lambda: runner._revision > prior or task.done())
        await eventually(lambda: bool(runner._session.state.portfolios['PAPER'].positions)
                         or task.done())
        assert runner._session.state.portfolios['PAPER'].positions
        assert runner.operator_state == 'paused'
        # The bounded wait observes actual periodic risk control evidence.
        capture = tmp_path / 'capture' / 'inputs.jsonl'
        await eventually(lambda: capture.exists() and 'risk_tick' in capture.read_text())
        runner.request_stop()
        runner.request_resume()  # stop remains authoritative
        summary = await asyncio.wait_for(task, 3)
        assert summary['submitted'] == 1
        assert summary['accounts']['PAPER']
    finally:
        if not task.done():
            runner.request_stop()
            await asyncio.wait_for(task, 3)


@pytest.mark.asyncio
async def test_noncooperative_pause_callback_fails_visibly(tmp_path):
    config = valid_config(strategies=[strategy_config('alpha', factory='stubborn')])
    config['runtime']['shutdown_grace_seconds'] = .03
    clock = VirtualClock(START)
    feeds = [ControlledFeed(v, clock=clock) for v in VENUES]
    entered, release = asyncio.Event(), asyncio.Event()

    def factory(context):
        class Stubborn:
            async def on_snapshot(self, snapshot):
                entered.set()
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        pass
                return None
        return Stubborn()

    runner = BotRunner(config, output_directory=tmp_path / 'capture', feeds=feeds,
        clock=clock, strategy_factories={'stubborn': factory}, operator_controls=True)
    task = asyncio.create_task(runner.run())
    try:
        await eventually(lambda: all(f.started.is_set() for f in feeds) or task.done())
        for feed in feeds:
            await feed.push_book()
        await asyncio.wait_for(entered.wait(), 2)
        runner.request_pause()
        with pytest.raises(RuntimeError, match='cancellation exceeded'):
            await asyncio.wait_for(task, 3)
        assert runner.status['phase'] == 'failed'
    finally:
        release.set()
        if not task.done():
            runner.request_stop()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_service_stop_retains_uncooperative_callback_as_unresolved_cleanup(tmp_path):
    from contextlib import suppress
    from test_bot_service import service_type,connect,rpc
    config=valid_config(strategies=[strategy_config('alpha',factory='stubborn')])
    config['runtime']['shutdown_grace_seconds']=.03
    clock=VirtualClock(START)
    feeds=[ControlledFeed(v,clock=clock) for v in VENUES]
    entered,release=asyncio.Event(),asyncio.Event()
    def strategy(context):
        class Stubborn:
            async def on_snapshot(self,snapshot):
                entered.set()
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        pass
        return Stubborn()
    def factory(config,**kwargs):
        return BotRunner(config,**kwargs,feeds=feeds,clock=clock,
            strategy_factories={'stubborn':strategy})
    service=service_type()(config,output_root=tmp_path/'runs',runner_factory=factory)
    await service.start(tmp_path/'private/control.sock')
    pair=None
    try:
        pair=await connect(service)
        await rpc(*pair,'start',service_id=service.service_id)
        await eventually(lambda: all(f.started.is_set() for f in feeds))
        for feed in feeds:
            await feed.push_book()
        await asyncio.wait_for(entered.wait(),2)
        pair[1].close()
        await pair[1].wait_closed()
        with pytest.raises(RuntimeError,match='cleanup'):
            await service.close()
        assert any(not task.done() for task in service.runner._zombies)
        assert service._cleanup_blocked
        assert not json.loads((service.runner.output_directory/'manifest.json').read_text())['finalized']
    finally:
        release.set()
        if pair:
            pair[1].close()
        if service.runner:
            await asyncio.gather(*service.runner._zombies,return_exceptions=True)
        with suppress(RuntimeError):
            await service.close()
