from copy import deepcopy
from dataclasses import replace
import logging
import pytest
from src.paper_trading.runtime import BotRunner
from src.paper_trading.runtime_config import trading_config
from src.paper_trading.session import PaperSession
from src.paper_trading.clock import VirtualClock
from src.strategies.context import StrategyContext
from src.strategies.immediate_complement import ImmediateComplementStrategy
from src.utilities.classes import OrderBookSnapshotEvent
from src.utilities.market_events import PublicEventEnvelope
from bot_v1_support import valid_config, ControlledFeed, VENUES
from test_complement_discovery import normalize_window, raw


def payload(index=0):
    pair=[]
    for venue in sorted(VENUES):
        w=normalize_window(venue,raw(venue)); terms=list(w.terms)
        terms[2]+=index*900;terms[3]+=index*900
        market=replace(w.market,market_id=f'm{index}',slug=f's{index}',
                       resolution_date=terms[3]+300)
        pair.append(dict(market=market,subscription=f's{index}',terms=terms))
    return pair


async def setup_runner():
    config=valid_config()
    config['rolling']={'discovery_interval_seconds':15,'polymarket_series_id':375,'max_pages':3,'page_size':20}
    config['risk']['PAPER']['diversification']={'crypto':1.,'other':0.}
    config['strategies'][0]['factory']='complement'
    feeds=[ControlledFeed(v) for v in VENUES]
    for f in feeds:
        f.set_markets=lambda markets,f=f: setattr(f,'markets',list(markets))
    clock=VirtualClock(payload()[0]['terms'][2]+1)
    runner=BotRunner(config,output_directory='/tmp/not-used-rolling',feeds=feeds,
                     strategy_factories={'complement':ImmediateComplementStrategy},clock=clock)
    runner._logger=logging.getLogger('rolling-test')
    execution=trading_config(config)
    runner._session=PaperSession(execution,runner.clock)
    for slot in runner.slots:
        slot.context=StrategyContext('one','PAPER',{},runner.clock,7,'rolling',execution=execution.paper_execution)
        slot.strategy=ImmediateComplementStrategy(slot.context)
    return runner,clock


async def books(runner,pair):
    for i,row in enumerate(pair):
        m=row['market']; now=runner.clock()
        event=OrderBookSnapshotEvent(m.market_id,'YES',now,m.exchange,{.49:100},{.5:100})
        runner._session.publish(PublicEventEnvelope(event=event,received_at=now,available_at=now,
                                                  ingest_index=i+1,source_id=f'{m.market_id}-{i}'))
    await runner._session.advance(runner.clock())


@pytest.mark.asyncio
async def test_three_windows_activate_only_with_fresh_depth_and_keep_account_state():
    runner,clock=await setup_runner()
    try:
        account=runner._session.state.portfolios['PAPER']
        account.daily_day=42
        for index in range(3):
            clock.advance(payload(index)[0]['terms'][2]+1); runner.clock.refresh()
            await runner._install_windows([payload(index)],{})
            runner._activate_window()
            assert runner.slots[0].context.settings.get('markets',{}) != {v:f'm{index}' for v in VENUES}
            await books(runner,payload(index)); runner._activate_window()
            assert runner.slots[0].context.settings['markets']=={v:f'm{index}' for v in VENUES}
            assert runner.slots[0].entry['required_markets']==[f'{v}:m{index}' for v in sorted(VENUES)]
            assert runner._session.state.portfolios['PAPER'] is account and account.daily_day==42
    finally:
        await runner._session.finish()


@pytest.mark.asyncio
async def test_rolling_never_changes_context_while_callback_owns_snapshot():
    runner,_=await setup_runner()
    try:
        await runner._install_windows([payload()],{}); await books(runner,payload())
        runner.slots[0].task=object()
        runner._activate_window()
        assert 'markets' not in runner.slots[0].context.settings
        runner.slots[0].task=None;runner._activate_window()
        assert runner.slots[0].context.settings['markets']
    finally:
        await runner._session.finish()


@pytest.mark.asyncio
async def test_background_discovery_rolls_three_windows_without_manual_market_config(tmp_path):
    import asyncio
    from datetime import datetime,timezone
    from bot_v1_support import eventually,stop_and_join
    from test_bot_loop_v1 import dispose
    base=payload()[0]['terms'][2]
    clock=VirtualClock(base+1)
    def iso(stamp):return datetime.fromtimestamp(stamp,timezone.utc).isoformat().replace('+00:00','Z')
    class Feed(ControlledFeed):
        def __init__(self,venue):super().__init__(venue,markets=[],clock=clock)
        def set_markets(self,markets):self.markets=list(markets)
        async def discover_windows(self,**kwargs):
            i=int((clock()-base)//900);r=raw(self.exchange)
            if self.exchange=='KALSHI':
                r.update(ticker=f'KXBTC15M-test-{i}',event_ticker=f'KXBTC15M-test-{i}',
                         open_time=iso(base+i*900),close_time=iso(base+(i+1)*900))
            else:
                r.update(id=str(100+i),slug=f's{i}');r.pop('feeCoefficient',None)
                r['assetPriceTerms'].update(windowStart=iso(base+i*900),windowEnd=iso(base+(i+1)*900))
            return [r],None
    seen=[]
    class Observer:
        needs_account_ticks=True
        def __init__(self,context):self.context=context
        async def on_snapshot(self,snapshot):
            markets=self.context.settings.get('markets')
            if markets and markets not in seen:seen.append(dict(markets))
    config=valid_config();config['rolling']={'discovery_interval_seconds':.01}
    for options in config['feeds'].values():options['markets']=[]
    config['strategies'][0]['required_markets']=[]
    feeds=[Feed(v) for v in VENUES]
    runner=BotRunner(config,output_directory=tmp_path/'rolling',feeds=feeds,
                     strategy_factories={'one':Observer},clock=clock)
    task=asyncio.create_task(runner.run())
    try:
        for i in range(3):
            clock.advance(base+i*900+1)
            await eventually(lambda: all((f's{i}' if f.exchange=='POLYMARKET_US' else f'KXBTC15M-test-{i}') in f.markets for f in feeds))
            for f in feeds:
                await f.push_book(market_id=str(100+i) if f.exchange=='POLYMARKET_US' else f'KXBTC15M-test-{i}')
            await eventually(lambda: len(seen)>=i+1)
        await stop_and_join(runner,task)
        assert len(seen)==3
    finally:await dispose(task)
