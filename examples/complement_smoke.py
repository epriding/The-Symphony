"""Deterministic synthetic acceptance run; uses the real bot, risk, engine and ledger.

Run: python -m examples.complement_smoke --output /tmp/complement-proof
No network transport, private client, or historical backtest is constructed.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path

from src.bot import BotRunner, replay_bot
from src.paper_trading.clock import VirtualClock
from src.paper_trading.window_discovery import normalize_window
from src.utilities.classes import OrderBookSnapshotEvent
from src.utilities.market_events import PublicEventEnvelope, ReceiptSequencer, SettlementEvent

ROOT = Path(__file__).resolve().parents[1]
START = 1_800_000_000.0
VENUES = ('KALSHI','POLYMARKET_US')


def _iso(stamp):
    return datetime.fromtimestamp(stamp,timezone.utc).isoformat().replace('+00:00','Z')


class FixtureFeed:
    def __init__(self, venue, clock, scenario):
        self.exchange, self.clock, self.scenario = venue,clock,scenario
        self.markets, self.identities, self.overrides = [],{},{}
        self.sequencer = ReceiptSequencer()
        self.queue = asyncio.Queue(maxsize=64)
        self.started = asyncio.Event()
        name='kalshi-market' if venue=='KALSHI' else 'poly-market'
        path=ROOT/'docs/validation/bitcoin-15m-2026-11-14'/f'{name}.json'
        self.template=json.loads(path.read_text())['body']['market']

    def raw(self,index):
        raw=json.loads(json.dumps(self.template))
        if self.exchange=='KALSHI':
            raw.update(ticker=f'KXBTC15M-SYNTH-{index}-15',event_ticker=f'KXBTC15M-SYNTH-{index}',
                open_time=_iso(START+index*900),close_time=_iso(START+(index+1)*900),
                expected_expiration_time=_iso(START+(index+1)*900+300))
        else:
            raw.update(id=f'synthetic-us-{index}',slug=f'synthetic-btc-{index}',
                       endDate=_iso(START+(index+1)*900+300))
            raw['assetPriceTerms'].update(windowStart=_iso(START+index*900),windowEnd=_iso(START+(index+1)*900))
        return raw

    def subscription(self,index):
        return self.raw(index)['ticker' if self.exchange=='KALSHI' else 'slug']

    def identity(self,index):
        return self.raw(index)['ticker' if self.exchange=='KALSHI' else 'id']

    def set_markets(self,markets):
        self.markets=list(markets)

    async def discover_windows(self,**kwargs):
        index=int((self.clock()-START)//900)
        rows=[self.raw(i) for i in (index,index+1)
              if not (self.scenario in ('rolling','rolling_daily') and i==1 and self.exchange=='POLYMARKET_US')]
        return rows,None

    async def get_market(self,subscription):
        for index in range(max(0,int((self.clock()-START)//900)-3),int((self.clock()-START)//900)+2):
            if self.subscription(index)==subscription:
                return normalize_window(self.exchange,self.raw(index)).market
        raise ValueError('Unknown synthetic subscription')

    async def settlements(self):
        if False:yield None

    async def push_book(self,index,*,bid,ask,age=0):
        await self.queue.put(OrderBookSnapshotEvent(self.identity(index),'YES',self.clock()-age,self.exchange,
                                                   {bid:1000},{ask:1000}))

    async def settle(self,index):
        await self.queue.put(SettlementEvent(exchange=self.exchange,market_id=self.identity(index),
            timestamp=self.clock(),settlement_id=f'synthetic-{self.exchange}-{index}',payouts={'YES':'1','NO':'0'}))

    async def stream(self):
        self.started.set()
        while True:
            event=await self.queue.get()
            receipt=self.sequencer.receipt();receipt['received_at']=self.clock()
            yield PublicEventEnvelope(event=event,available_at=self.clock(),**receipt,
                source_id=f'synthetic:{self.exchange}:{receipt["ingest_index"]}')


async def _wait(predicate,timeout=3):
    async def wait():
        while not predicate():await asyncio.sleep(.002)
    await asyncio.wait_for(wait(),timeout)


async def run_fixture(output_directory,*,scenario='paired',windows=1):
    if scenario not in ('paired','recovery','no_trade','rolling','daily_stop','neither','stale','rolling_daily') or not 1<=windows<=10:
        raise ValueError('Unknown synthetic scenario or window count')
    output=Path(output_directory)
    config=json.loads((ROOT/'examples/complement_config.example.json').read_text())
    config['runtime'].update(source='synthetic',startup_timeout_seconds=5,
        strategy_timeout_seconds=1,risk_interval_seconds=.01,shutdown_grace_seconds=5)
    config['rolling']['discovery_interval_seconds']=.01
    config['execution'].update(session_id=f'complement-proof-{scenario}',processing_delay={'seconds':0},
        assumptions='Synthetic depth and deterministic nonzero delays; standard example fee models; not calibrated or evidence of a live edge')
    config['strategies'][0]['settings'].update(max_quantity='1',max_pair_spend='1',entry_cutoff=1)
    if scenario in ('no_trade','stale'):config['strategies'][0]['settings']['max_pair_spend']='2'
    if scenario in ('daily_stop','rolling_daily'):
        config['strategies'][0]['settings'].update(max_pair_spend='15',max_quantity='15')
        config['risk']['PAPER'].update(max_position_exposure=10,max_total_exposure=20)
    for venue,rules in config['execution']['venues'].items():
        rules.update(outbound={'seconds':.02 if venue=='KALSHI' else .2},
            acknowledgement={'seconds':.01},execution_report={'seconds':.02},cancel={'seconds':.01})
    clock=VirtualClock(START+1)
    feeds=[FixtureFeed(v,clock,scenario) for v in VENUES]
    runner=BotRunner(config,output_directory=output/'capture',feeds=feeds,clock=clock)
    task=asyncio.create_task(runner.run())
    async def pump_until(predicate,steps=150):
        for _ in range(steps):
            if task.done():return await task
            if predicate():return
            clock.advance(clock()+.02);runner._wake.set()
            await asyncio.sleep(.003)
        raise AssertionError('Synthetic lifecycle did not complete within its finite clock budget')
    try:
        await _wait(lambda: all(f.started.is_set() for f in feeds))
        expected_pairs=0
        for index in range(windows):
            if clock()<START+index*900+1:
                clock.advance(START+index*900+1);runner._wake.set()
            skipped=scenario in ('rolling','rolling_daily') and index==1
            if not skipped:
                await _wait(lambda: all(f.subscription(index) in f.markets for f in feeds))
                for f in feeds:
                    bid,ask=((.47,.48) if scenario in ('no_trade','stale') else (.39,.4)) if f.exchange=='KALSHI' else (.5,.51)
                    await f.push_book(index,bid=bid,ask=ask)
                if scenario=='daily_stop' and index>0:
                    await _wait(lambda: runner.slots[0].strategy.last_window==tuple(sorted({f.exchange:f.identity(index) for f in feeds}.items())))
                    await pump_until(lambda: runner.slots[0].strategy.active is None)
                elif scenario in ('no_trade','stale'):
                    reason='no_net_edge'
                    await _wait(lambda: runner.slots[0].strategy.last_reason==reason)
                    for _ in range(100):
                        report=await runner._store.aget('_summary','complement',{})
                        if report.get('decision_reasons',{}).get(reason,0):break
                        await asyncio.sleep(.003)
                    else:raise AssertionError('No-trade reason did not reach the report')
                    if scenario=='stale':
                        clock.advance(clock()+10);runner._wake.set()
                        await _wait(lambda: runner.slots[0].strategy.last_reason=='stale_depth')
                        for _ in range(100):
                            report=await runner._store.aget('_summary','complement',{})
                            if report.get('decision_reasons',{}).get('stale_depth',0):break
                            await asyncio.sleep(.003)
                        else:raise AssertionError('Stale-depth reason did not reach the report')
                else:
                    expected_pairs+=1
                    await _wait(lambda: len(runner._session.engine.orders)>=expected_pairs*2)
                    if scenario=='neither':
                        await _wait(lambda: all(o.dispatched_at is not None for o in runner._session.engine.orders.values()))
                        await feeds[0].push_book(index,bid=.59,ask=.6)
                        await feeds[1].push_book(index,bid=.3,ask=.31)
                        await _wait(lambda: min(runner._session.state.orderbooks[f'{feeds[0].exchange}:{feeds[0].identity(index)}:YES'].asks)==.6)
                    if scenario in ('recovery','daily_stop') or (scenario=='rolling_daily' and index==2):
                        await pump_until(lambda: any(o.approved.market_id==feeds[0].identity(index) and o.filled>0
                                                     for o in runner._session.engine.orders.values()))
                        await feeds[1].push_book(index,bid=.3,ask=.31)
                        if scenario in ('daily_stop','rolling_daily'):await feeds[0].push_book(index,bid=.01,ask=.4)
                    await pump_until(lambda: (not runner._session.state.portfolios['PAPER'].reservations
                        and not runner._session.state.portfolios['PAPER'].open_orders
                        and runner.slots[0].strategy.active is None))
            if skipped:
                await _wait(lambda: runner._discovery_ready)
                # Release the prior window's delayed counterleg while discovery skips this window.
                await feeds[1].settle(index-1)
                await pump_until(lambda: not runner._session.state.portfolios['PAPER'].positions)
            clock.advance(max(clock(),START+(index+1)*900+1));runner._wake.set()
            if not skipped:
                for f in feeds:
                    if not (scenario in ('rolling','rolling_daily') and index==0 and f.exchange=='POLYMARKET_US'):
                        await f.settle(index)
                await pump_until(lambda: all(p.exchange=='POLYMARKET_US' for p in
                    runner._session.state.portfolios['PAPER'].positions.values()) if scenario in ('rolling','rolling_daily') and index==0
                    else not runner._session.state.portfolios['PAPER'].positions)
        runner.request_stop()
        summary=await asyncio.wait_for(task,5)
    finally:
        if not task.done():
            task.cancel();await asyncio.gather(task,return_exceptions=True)
    replayed=await replay_bot(output/'capture',output/'replay')
    for key in ('accounts','fees','submitted','complement','history'):
        if summary[key]!=replayed[key]:raise AssertionError(f'Synthetic replay differs in {key}')
    return dict(scenario=scenario,visited_windows=windows,summary=summary,replay=replayed)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True)
    parser.add_argument('--scenario',choices=('paired','recovery','no_trade','rolling','daily_stop','neither','stale','rolling_daily'),default='rolling')
    parser.add_argument('--windows',type=int,default=3)
    args=parser.parse_args()
    result=asyncio.run(run_fixture(args.output,scenario=args.scenario,windows=args.windows))
    print(json.dumps(dict(scenario=result['scenario'],windows=result['visited_windows'],
                         complement=result['summary']['complement']),indent=2))


if __name__=='__main__':main()
