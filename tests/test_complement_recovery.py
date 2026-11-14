from dataclasses import replace
from decimal import Decimal

import pytest

from src.paper_trading.models import Delay
from src.paper_trading.venue_rules import fixture_venue
from src.strategies.context import StrategyContext
from src.strategies.immediate_complement import ImmediateComplementStrategy
from src.utilities.classes import OrderIntentBatch
from test_paper_trading import make_session, book, START


async def configured(*, slow_b=False):
    rules = {'A': fixture_venue('A'),
             'B': fixture_venue('B', outbound=Delay(.1 if slow_b else 0))}
    session = await make_session(rules=rules)
    await book(session, exchange='A', bids={.39:100}, asks={.4:100})
    await book(session, exchange='B', bids={.5:100}, asks={.51:100})
    terms = ['BTC','BRTI',START,START+900,'100','2','gte']
    settings = dict(markets={'A':'m','B':'m'}, terms={'A':terms,'B':terms},
                    max_pair_spend='1', max_quantity='1', min_net_profit_per_pair='.02',
                    entry_timeout=.5, recovery_timeout=1, recovery_attempts=2,
                    entry_cutoff=1, max_book_age=5, max_quote_skew=1)
    context = StrategyContext('one','PAPER',settings,session.clock,7,'test',
                              execution=session.engine.config)
    return session, ImmediateComplementStrategy(context)


async def step(session, strategy):
    actions = await strategy.on_snapshot(await session.state.snapshot())
    for action in actions or []:
        if isinstance(action, OrderIntentBatch):
            await session.submit_batch(action)
        else:
            await session.submit(action)
    return actions


@pytest.mark.asyncio
async def test_actual_strategy_submits_once_and_holds_equal_fills():
    session, strategy = await configured()
    actions = await step(session, strategy)
    assert len(actions) == 1 and isinstance(actions[0], OrderIntentBatch)
    await session.advance(START+.05)
    assert await step(session, strategy) is None
    account = session.state.portfolios['PAPER']
    assert sorted(p.size for p in account.positions.values()) == [1,1]
    assert await step(session, strategy) is None
    assert len(session.engine.orders) == 2
    await session.finish()


@pytest.mark.asyncio
async def test_failed_hedge_sells_only_confirmed_excess_and_records_loss():
    session, strategy = await configured(slow_b=True)
    await step(session,strategy)
    await book(session,exchange='B',bids={.3:100},asks={.31:100},at=START+.02)
    assert await step(session,strategy) is None  # The second entry is still unresolved.
    await session.advance(START+.25)
    actions = await step(session,strategy)
    assert len(actions)==1 and actions[0].reduce_only
    await session.advance(START+.3)
    assert await step(session,strategy) is None
    account=session.state.portfolios['PAPER']
    assert not account.positions and not account.reservations
    assert account.balances['A']['USD'].cash == pytest.approx(9999.99)
    assert len(session.engine.orders)==3
    await session.finish()


@pytest.mark.asyncio
async def test_missing_recovery_depth_keeps_inventory_and_blocks_reentry():
    session,strategy=await configured(slow_b=True)
    await step(session,strategy)
    await book(session,exchange='B',bids={.3:100},asks={.31:100},at=START+.02)
    await session.advance(START+.25)
    session.state.orderbooks['A:m:YES'].bids={}
    assert await step(session,strategy) is None
    assert strategy.active is not None
    assert len(session.state.portfolios['PAPER'].positions)==1
    session.clock.advance(START+3)
    assert await step(session,strategy) is None
    assert len(session.engine.orders)==2
    await session.finish()


@pytest.mark.asyncio
async def test_pending_recovery_cannot_submit_duplicate_sales():
    session,strategy=await configured(slow_b=True)
    await step(session,strategy)
    await book(session,exchange='B',bids={.3:100},asks={.31:100},at=START+.02)
    await session.advance(START+.25)
    assert await step(session,strategy)
    assert await step(session,strategy) is None
    await session.finish()


@pytest.mark.asyncio
async def test_opted_in_strategy_receives_timer_notifications_without_new_books(tmp_path):
    import asyncio
    from src.bot import BotRunner
    from bot_v1_support import valid_config, ControlledFeed, VENUES, eventually, stop_and_join
    from test_bot_loop_v1 import dispose, ready_books
    calls = []
    class TimerStrategy:
        needs_account_ticks = True
        def __init__(self, context):
            assert context.execution is not None
        async def on_snapshot(self, snapshot):
            calls.append(snapshot.timestamp)
    feeds = [ControlledFeed(venue) for venue in VENUES]
    runner = BotRunner(valid_config(), output_directory=tmp_path/'timer', feeds=feeds,
                       strategy_factories={'one': TimerStrategy})
    task = asyncio.create_task(runner.run())
    try:
        await ready_books(feeds)
        await eventually(lambda: len(calls) >= 3)
        await stop_and_join(runner, task)
    finally:
        await dispose(task)


@pytest.mark.asyncio
async def test_partial_settlement_before_next_callback_never_sells_matched_remainder():
    from src.utilities.market_events import SettlementEvent
    from test_paper_trading import envelope
    session, strategy = await configured(slow_b=False)
    await step(session,strategy)
    await session.advance(START+.05)
    session.publish(envelope(session, SettlementEvent(exchange='A',market_id='m',
        timestamp=START+.1,settlement_id='a',payouts={'YES':'1','NO':'0'}), START+.1))
    await session.advance(START+.15)
    assert await strategy.on_snapshot(session.state._snapshot()) is None
    assert 'B:m:NO' in session.state.portfolios['PAPER'].positions
    assert len(session.engine.orders) == 2
    await session.finish()
