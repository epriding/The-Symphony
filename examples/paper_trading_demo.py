"""Deterministic two-venue demo. All data is synthetic; no credentials/network."""
import argparse
import asyncio
from dataclasses import replace
import json

from src.paper_trading.clock import VirtualClock
from src.paper_trading.models import Delay, FeeSchedule, PaperExecutionConfig
from src.paper_trading.session import PaperSession
from src.paper_trading.venue_rules import fixture_venue
from src.utilities.classes import (
    MarketInfo, OrderBookSnapshotEvent, OrderIntent, OrderIntentBatch,
    RiskConfig, Side, TimeInForce, TradeEvent, TradingConfig,
)
from src.utilities.market_events import PublicEventEnvelope

START = 1_800_000_000.0


async def run_demo(output=None):
    config = TradingConfig()
    config.strategy_portfolios = {name: 'PAPER' for name in ('taker', 'maker', 'arb')}
    config.paper_starting_balances = {'PAPER': {'POLYMARKET_US': 1000, 'KALSHI': 1000}}
    config.risk_configs['PAPER'] = RiskConfig(diversification={'other': 1},
        max_position_exposure=1000, max_total_exposure=2000, capital_allocation=100)
    config.paper_execution = PaperExecutionConfig({
        venue: replace(fixture_venue(venue), outbound=Delay(.025), acknowledgement=Delay(.004),
                       execution_report=Delay(.006), cancel=Delay(.020), fees=fees)
        for venue, fees in (
            ('POLYMARKET_US', FeeSchedule('synthetic-us-example', 'polymarket_us', '.0695')),
            ('KALSHI', FeeSchedule('synthetic-kalshi-example', 'kalshi', '.07', balance_precision='.0001')),
        )}, seed=7, assumptions='Synthetic books and timing; formula examples, not calibrated execution')
    session = PaperSession(config, VirtualClock(START), output)
    counter = 0
    for venue in config.paper_execution.venues:
        await session.add_market(MarketInfo('demo', venue, 'demo', {}, True,
            {'YES': True, 'NO': True}, resolution_date=START+3600,
            tick_size=.01, quantity_step=1, min_quantity=1))

    async def feed(event, offset):
        nonlocal counter
        counter += 1
        at = START + offset
        session.publish(PublicEventEnvelope(event=event, received_at=at, available_at=at,
            ingest_index=counter, source_id=f'demo-source-{counter}'))
        await session.advance(at)

    def intent(name, offset, **changes):
        values = dict(market_id='demo', exchange='POLYMARKET_US', outcome='YES', side=Side.BUY,
            strategy=name, price=.51, estimated_probability=.9, created_at=START+offset,
            intent_id=f'{name}-{offset}', resolution_date=START+3600)
        values.update(changes)
        return OrderIntent(**values)

    for venue in config.paper_execution.venues:
        await feed(OrderBookSnapshotEvent('demo', 'YES', START, venue, {.49:5}, {.50:40, .51:60}), 0)
    await session.advance(START+.01)
    entry = await session.submit(intent('taker', .01))
    await session.advance(START+.1)
    await session.advance(START+.2)
    maker = await session.submit(intent('maker', .2, price=.49, estimated_probability=.8,
                                        tif=TimeInForce.GTC, post_only=True))
    await session.advance(START+.25)
    await feed(TradeEvent('demo', 'YES', START+.3, 'POLYMARKET_US', .49, 20,
                          Side.SELL, 'demo-trade-1'), .3)
    await session.advance(START+.32)
    if maker.submission_ids:
        await session.cancel(maker.submission_ids[0], 'POLYMARKET_US')
    await session.advance(START+.4)
    await feed(OrderBookSnapshotEvent('demo', 'YES', START+.4, 'KALSHI', {.49:5}, {.50:1}), .4)
    group = await session.submit_batch(OrderIntentBatch(
        legs=[intent('arb', .4, estimated_probability=None),
              intent('arb', .4, exchange='KALSHI', intent_id='arb-peer', estimated_probability=None)],
        quantity_ratios=[1,1], capital_ceiling=20, execution_policy='paper_independent_legs',
        execution_deadline=START+.8, batch_id='demo-linked-legs',
        market_equivalence='synthetic identical demo payoff'))
    await session.advance(START+1)
    await feed(OrderBookSnapshotEvent('demo', 'YES', START+1.2, 'POLYMARKET_US',
                                      {.53:200}, {.55:200}), 1.2)
    summary = await session.finish(output)
    return session, summary


async def main(output=None):
    session, summary = await run_demo(output)
    print(json.dumps({key: summary[key] for key in
        ('submitted', 'counts', 'fees', 'accounts', 'assumptions')}, indent=2))
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', help='New directory for capture and audit artifacts')
    args = parser.parse_args()
    asyncio.run(main(args.output))
