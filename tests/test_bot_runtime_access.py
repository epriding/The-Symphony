"""Shared observations must describe actual paper execution, not cached guesses."""
import asyncio
from copy import deepcopy
import json

import pytest

from src.bot import BotRunner
from src.paper_trading.clock import VirtualClock
from src.utilities.classes import Side
from bot_v1_support import START, VENUES, ControlledFeed, eventually, strategy_config, valid_config
from test_bot_service import service_type, connect, rpc


@pytest.mark.asyncio
async def test_findings_are_shared_plain_text_with_separate_artifact_evidence(tmp_path):
    service=service_type()(output_root=tmp_path/'runs',smoke=True)
    await service.start(tmp_path/'private/control.sock')
    try:
        reader,writer=await connect(service)
        await rpc(reader,writer,'start',service_id=service.service_id)
        await eventually(lambda: service.session_task.done())
        target=dict(service_id=service.service_id,session_id=service.session_id)
        inputs=service.runner.output_directory/'inputs.jsonl'
        before=inputs.read_bytes()
        result=await rpc(reader,writer,'publish_finding',params={
            'author':'coding-agent','severity':'warning','message':'\x1b[31mCheck this\x1b[0m',
            'name':'smoke','evidence':'tests/example.py:12'},**target)
        assert result['ok'],result
        finding=result['data']
        assert finding['strategy']=='smoke' and finding['author']=='coding-agent'
        assert '\x1b' not in finding['message']
        listing=await rpc(reader,writer,'findings',params={'name':'smoke'},**target)
        assert listing['data']['items']==[finding]
        view=await rpc(reader,writer,**target)
        assert view['data']['recent_findings']==[finding]
        events=await rpc(reader,writer,'strategy',params={'name':'smoke','collection':'events'},**target)
        assert events['data']['items']
        assert inputs.read_bytes()==before
        artifact=service.runner.output_directory/'operator_findings.jsonl'
        assert [json.loads(line) for line in artifact.read_text().splitlines()]==[finding]
        assert artifact.stat().st_mode & 0o777 == 0o600
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_finding_append_failure_is_visible_without_mutating_the_account(tmp_path,monkeypatch):
    service=service_type()(output_root=tmp_path/'runs',smoke=True)
    await service.start(tmp_path/'private/control.sock')
    try:
        reader,writer=await connect(service)
        await rpc(reader,writer,'start',service_id=service.service_id)
        await eventually(lambda: service.session_task.done())
        target=dict(service_id=service.service_id,session_id=service.session_id)
        original=deepcopy(service.runner._session.state.portfolios['PAPER'].positions)
        def fail(row):
            raise OSError('private failure detail must not be published')
        monkeypatch.setattr(service,'_append_finding',fail)
        result=await rpc(reader,writer,'publish_finding',params={
            'author':'agent','severity':'error','message':'explanation'},**target)
        assert result['ok'] is False
        assert 'private failure' not in result['error']['message']
        listing=await rpc(reader,writer,'findings',**target)
        assert listing['data']['items']==[]
        view=await rpc(reader,writer,**target)
        assert view['data']['finding_publication_available'] is False
        assert service.runner._session.state.portfolios['PAPER'].positions==original
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_finding_ring_eviction_and_invalid_input_are_explicit(tmp_path):
    service=service_type()(output_root=tmp_path/'runs',smoke=True)
    await service.start(tmp_path/'private/control.sock')
    try:
        reader,writer=await connect(service)
        await rpc(reader,writer,'start',service_id=service.service_id)
        await eventually(lambda: service.session_task.done())
        target=dict(service_id=service.service_id,session_id=service.session_id)
        for i in range(130):
            result=await rpc(reader,writer,'publish_finding',params={
                'author':'agent','severity':'info','message':f'finding {i}'},**target)
            assert result['ok'],result
        listing=await rpc(reader,writer,'findings',**target)
        assert len(listing['data']['items'])==50
        assert listing['data']['evicted'] and listing['data']['retained_start']==3
        assert len(service.observations.findings)==128
        for params in ({'author':'agent','severity':'fatal','message':'bad'},
                       {'author':'agent','severity':'info','message':'x'*2049},
                       {'author':'agent','severity':'info','message':'bad','name':'foreign'}):
            result=await rpc(reader,writer,'publish_finding',params=params,**target)
            assert result['ok'] is False
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_strategy_pages_bind_cursors_to_service_session_and_filter(tmp_path):
    config=valid_config(strategies=[strategy_config(f'quiet-{i:02}') for i in range(5)])
    service=service_type()(config,output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    try:
        reader,writer=await connect(service)
        target=dict(service_id=service.service_id)
        first=await rpc(reader,writer,'strategies',params={'limit':2},**target)
        assert first['ok'],first
        assert [r['name'] for r in first['data']['items']]==['quiet-00','quiet-01']
        cursor=first['data']['next_cursor']
        assert cursor and first['data']['has_more']
        second=await rpc(reader,writer,'strategies',params={'limit':2,'cursor':cursor},**target)
        assert [r['name'] for r in second['data']['items']]==['quiet-02','quiet-03']
        wrong_filter=await rpc(reader,writer,'strategy',
            params={'name':'quiet-00','collection':'orders','cursor':cursor},**target)
        assert wrong_filter['ok'] is False
        for limit in (0,101,True,'2'):
            invalid=await rpc(reader,writer,'strategies',params={'limit':limit},**target)
            assert invalid['ok'] is False
        malformed=await rpc(reader,writer,'strategies',params={'cursor':'not a cursor'},**target)
        assert malformed['ok'] is False
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


def test_control_projection_admission_rejects_unrepresentable_configuration(tmp_path):
    config=valid_config()
    config['execution']['session_id']='x'*257
    with pytest.raises(ValueError,match='identifier|projection'):
        service_type()(config,output_root=tmp_path/'runs')


def test_observation_model_and_fill_retention_remain_bounded():
    from src.operator.observations import Observations
    from types import SimpleNamespace
    config=valid_config()
    observations=Observations(config)
    approved=SimpleNamespace(strategy='one',portfolio_id='PAPER',exchange=VENUES[0],
        market_id='m',outcome='YES',side=Side.BUY)
    runner=SimpleNamespace(_session=SimpleNamespace(engine=SimpleNamespace(
        orders={'order':SimpleNamespace(approved=approved)})))
    for index in range(300):
        observations.observe(runner,'execution',dict(event='execution',
            execution_id=str(index),order_id='order',quantity='1',price='.5',timestamp=START))
    assert len(observations.fills)==256
    assert observations.storage_bytes()<3*1024*1024
    page=observations.read('one','fills')
    assert len(page['items'])==50 and page['has_more']
    assert page['retained_start']==45 and page['retained_end']==300
    assert page['evicted'] is True


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
