"""Shared observations must describe actual paper execution, not cached guesses."""
import asyncio
from copy import deepcopy

import pytest

from src.bot import BotRunner
from src.paper_trading.clock import VirtualClock
from src.utilities.classes import Side
from bot_v1_support import START, VENUES, ControlledFeed, eventually, strategy_config, valid_config
from test_bot_service import service_type, connect, rpc


@pytest.mark.asyncio
async def test_finished_smoke_exposes_real_fills_positions_and_paper_mode(tmp_path):
    service = service_type()(output_root=tmp_path/'runs', smoke=True)
    await service.start(tmp_path/'private/control.sock')
    try:
        reader,writer = await connect(service)
        start = await rpc(reader,writer,'start',service_id=service.service_id)
        assert start['ok']
        await eventually(lambda: service.session_task.done())
        target=dict(service_id=service.service_id,session_id=service.session_id)
        summary = await rpc(reader,writer,**target)
        assert summary['data']['position_count'] == 2
        assert summary['data']['active_order_count'] == 0
        rows = await rpc(reader,writer,'strategies',**target)
        assert rows['ok'], rows
        strategy = rows['data']['items'][0]
        assert strategy['name'] == 'smoke' and strategy['execution_mode'] == 'PAPER'
        assert strategy['position_count'] == 2
        fills = await rpc(reader,writer,'strategy',params={'name':'smoke','collection':'fills'},**target)
        assert fills['ok'], fills
        assert len(fills['data']['items']) == 2
        assert {f['exchange'] for f in fills['data']['items']} == set(VENUES)
        assert all(f['strategy']=='smoke' and f['quantity'] != '0' for f in fills['data']['items'])
        assert fills['data']['scope'] == 'recent'
        positions = await rpc(reader,writer,'strategy',params={'name':'smoke','collection':'positions'},**target)
        assert len(positions['data']['items']) == 2
        assert positions['data']['scope'] == 'active'
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_multiple_named_strategies_and_disabled_entry_match_the_ledger(tmp_path):
    config = valid_config(strategies=[strategy_config(name, factory='entry',settings={'venue':venue})
        for name,venue in zip(('alpha','beta'),VENUES)])
    disabled = strategy_config('disabled',factory='not-registered')
    disabled['enabled'] = False
    config['strategies'].append(disabled)
    clock=VirtualClock(START)
    feeds=[ControlledFeed(v,clock=clock) for v in VENUES]

    def strategy(context):
        class Entry:
            done=False
            async def on_snapshot(self,snapshot):
                if self.done:
                    return None
                self.done=True
                return [context.new_intent(market_id='m',exchange=context.settings['venue'],
                    outcome='YES',side=Side.BUY,price=.5,estimated_probability=1,
                    resolution_date=context.now()+3600)]
        return Entry()

    def factory(config,**kwargs):
        return BotRunner(config,**kwargs,feeds=feeds,clock=clock,
            strategy_factories={'entry':strategy})

    service=service_type()(config,output_root=tmp_path/'runs',runner_factory=factory)
    await service.start(tmp_path/'private/control.sock')
    try:
        reader,writer=await connect(service)
        assert (await rpc(reader,writer,'start',service_id=service.service_id))['ok']
        await eventually(lambda: all(f.started.is_set() for f in feeds))
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: len(service.runner._session.engine.orders)==2)
        for tick in (.05,.1,.15):
            clock.advance(START+tick)
            prior=service.runner._revision
            for feed in feeds:
                await feed.push_book()
            await eventually(lambda: service.runner._revision>prior)
        service.runner.request_stop()
        await eventually(lambda: service.session_task.done())
        target=dict(service_id=service.service_id,session_id=service.session_id)
        rows=await rpc(reader,writer,'strategies',**target)
        assert rows['ok'],rows
        by_name={r['name']:r for r in rows['data']['items']}
        assert set(by_name)=={'alpha','beta','disabled'}
        assert by_name['disabled']['enabled'] is False
        assert by_name['disabled']['ready'] is False
        for name in ('alpha','beta'):
            account=service.runner._session.state.portfolios['PAPER']
            original=deepcopy(account.strategy_positions[name])
            assert by_name[name]['position_count']==len(original)==1
            response=await rpc(reader,writer,'strategy',params={'name':name,'collection':'positions'},**target)
            assert response['ok'],response
            row=response['data']['items'][0]
            position=next(iter(original.values()))
            assert float(row['quantity'])==position.size
            assert row['strategy']==name and row['exchange']==position.exchange
            assert 'settings' not in by_name[name]
            assert account.strategy_positions[name]==original
        unknown=await rpc(reader,writer,'strategy',params={'name':'unknown'},**target)
        assert unknown['error']['code']=='not_found'
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()
