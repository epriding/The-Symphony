import asyncio
import pytest
from src.bot import BotRunner,replay_bot
from src.paper_trading.clock import VirtualClock
from src.paper_trading.replay import replay_runtime
from src.strategies.immediate_complement import ImmediateComplementStrategy
from src.utilities.market_events import SettlementEvent
from bot_v1_support import valid_config, ControlledFeed, VENUES, eventually, stop_and_join, START
from test_bot_loop_v1 import dispose,wait_for_feeds


async def captured_pair(tmp_path):
    clock=VirtualClock(START+1)
    config=valid_config()
    settings=config['strategies'][0]['settings']
    settings.update(markets={v:'m' for v in VENUES},
        terms={v:['BTC','BRTI',START,START+900,'100','2','gte','mean60'] for v in VENUES},
        max_pair_spend='1',max_quantity='1',entry_cutoff=1)
    feeds=[ControlledFeed(v,clock=clock) for v in VENUES]
    runner=BotRunner(config,output_directory=tmp_path/'capture',feeds=feeds,
                     strategy_factories={'one':ImmediateComplementStrategy},clock=clock)
    task=asyncio.create_task(runner.run())
    try:
        await wait_for_feeds(feeds)
        for feed in feeds:
            bid,ask=(.39,.4) if feed.exchange=='KALSHI' else (.5,.51)
            await feed.push_book(bids={bid:100},asks={ask:100})
        await eventually(lambda: len(runner._session.engine.orders)==2
                         and all(o.phase=='dispatched' for o in runner._session.engine.orders.values()))
        clock.advance(START+1.1)
        await eventually(lambda: all(o.phase=='terminal' for o in runner._session.engine.orders.values()))
        clock.advance(START+1.2)
        await eventually(lambda: len(runner._session.state.portfolios['PAPER'].positions)==2)
        for feed in feeds:
            await feed.push(SettlementEvent(exchange=feed.exchange,market_id='m',timestamp=clock(),
                settlement_id=f'settle-{feed.exchange}',payouts={'YES':'1','NO':'0'}))
        await eventually(lambda: not runner._session.state.portfolios['PAPER'].positions)
        summary=await stop_and_join(runner,task)
        assert summary['submitted']==2
        assert summary['accounts']['PAPER']['cash']['KALSHI']==pytest.approx(1000.6)
        assert summary['accounts']['PAPER']['cash']['POLYMARKET_US']==pytest.approx(999.5)
        return summary
    finally:await dispose(task)


@pytest.mark.asyncio
async def test_real_strategy_pair_capture_replays_recorded_and_fresh_decisions(tmp_path):
    summary=await captured_pair(tmp_path)
    recorded=await replay_bot(tmp_path/'capture',tmp_path/'recorded')
    fresh=await replay_runtime(tmp_path/'capture',tmp_path/'fresh',
                              strategy_factories={'one':ImmediateComplementStrategy})
    for replayed in (recorded,fresh):
        for key in ('submitted','full_fills','fees','accounts','history'):
            assert replayed[key]==summary[key],key


@pytest.mark.asyncio
async def test_rolling_retirement_capture_replays_without_discovery_network(tmp_path):
    import json
    from test_complement_rolling import test_background_discovery_rolls_three_windows_without_manual_market_config
    await test_background_discovery_rolls_three_windows_without_manual_market_config(tmp_path)
    summary=json.loads((tmp_path/'rolling/summary.json').read_text())
    class Observer:
        def __init__(self,context):self.context=context
        async def on_snapshot(self,snapshot):return None
    for name,registry in (('recorded-rolling',None),('fresh-rolling',{'one':Observer})):
        result=await replay_runtime(tmp_path/'rolling',tmp_path/name,strategy_factories=registry)
        for key in ('accounts','submitted','history'):
            assert result[key]==summary[key]
