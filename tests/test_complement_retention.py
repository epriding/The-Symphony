import pytest
from src.utilities.market_events import SettlementEvent
from test_paper_trading import book, envelope, START
from test_complement_risk import pair
from test_complement_rolling import setup_runner,payload,books


@pytest.mark.asyncio
async def test_retirement_refuses_inventory_and_frees_only_completed_window():
    runner,clock=await setup_runner()
    session=runner._session
    try:
        rows=payload()
        await runner._install_windows([rows],{}); await books(runner,rows)
        # An unfilled window can retire once it is no longer an entry window.
        assert not await session.retire_market(rows[0]['market'].exchange,'m0',not_before=clock()+900)
        clock.advance(clock()+901);runner.clock.refresh()
        assert await session.retire_market(rows[0]['market'].exchange,'m0',not_before=clock()-1)
        assert f"{rows[0]['market'].exchange}:m0" not in session.state.markets
        assert (rows[0]['market'].exchange,'m0') not in session.engine.books
    finally:await session.finish()


@pytest.mark.asyncio
async def test_more_windows_than_market_limit_remain_bounded_without_obligations():
    runner,clock=await setup_runner()
    try:
        runner.limits['max_markets']=4
        for i in range(12):
            clock.advance(payload(i)[0]['terms'][2]+1);runner.clock.refresh()
            await runner._install_windows([payload(i)],{})
            await books(runner,payload(i));runner._activate_window()
            assert not runner._rolling_paused
            assert len(runner._session.state.markets)<=4
            assert sum(len(f.markets) for f in runner.feeds)<=4
        assert not runner._session.state.portfolios['PAPER'].positions
    finally:await runner._session.finish()


@pytest.mark.asyncio
async def test_capacity_exhaustion_preserves_real_matched_holdings_and_pauses_next_window():
    from src.utilities.classes import OrderBookSnapshotEvent
    from src.utilities.market_events import PublicEventEnvelope
    runner,clock=await setup_runner()
    session=runner._session
    try:
        runner.limits['max_markets']=2
        rows=payload();await runner._install_windows([rows],{})
        for i,row in enumerate(rows):
            m=row['market']; bid,ask=(.39,.4) if i==0 else (.5,.51)
            session.publish(PublicEventEnvelope(event=OrderBookSnapshotEvent(m.market_id,'YES',clock(),m.exchange,{bid:100},{ask:100}),
                received_at=clock(),available_at=clock(),ingest_index=i+1,source_id=f'book{i}'))
        await session.advance(clock());runner._activate_window()
        strategy=runner.slots[0].strategy
        strategy.options['max_quantity']='1'
        decisions=await strategy.on_snapshot(session.state._snapshot())
        assert decisions
        await session.submit_batch(decisions[0]);await session.advance(clock())
        clock.advance(clock()+.1)
        await session.advance(clock())
        assert len(session.state.portfolios['PAPER'].positions)==2
        clock.advance(payload(1)[0]['terms'][2]+1);runner.clock.refresh()
        await runner._install_windows([payload(1)],{})
        assert runner._rolling_paused
        assert len(session.state.markets)==2
        assert len(session.state.portfolios['PAPER'].positions)==2
        assert session.state.portfolios['PAPER'].complement_pairs
    finally:await session.finish()


@pytest.mark.asyncio
async def test_archived_retirement_blocks_duplicate_old_public_state_after_hot_tail(tmp_path):
    from src.paper_trading.history import HistoryStore
    from src.paper_trading.runtime_config import trading_config
    from src.paper_trading.session import PaperSession
    from src.utilities.classes import OrderBookSnapshotEvent
    from src.utilities.market_events import PublicEventEnvelope
    runner,clock=await setup_runner()
    await runner._session.finish()
    store=HistoryStore(tmp_path/'archive',limits=runner.limits)
    runner._store=store
    runner._session=PaperSession(trading_config(runner.config),runner.clock,history_store=store,limits=runner.limits)
    try:
        first=payload()[0]['market']
        runner.limits['max_markets']=4
        for i in range(20):
            clock.advance(payload(i)[0]['terms'][2]+1);runner.clock.refresh()
            await runner._install_windows([payload(i)],{})
        assert await store.acontains('retired_markets',(first.exchange,first.market_id))
        event=OrderBookSnapshotEvent(first.market_id,'YES',clock(),first.exchange,{.4:1},{.5:1})
        runner._session.publish(PublicEventEnvelope(event=event,received_at=clock(),available_at=clock(),ingest_index=99,source_id='old'))
        await runner._session.advance(runner.clock())
        assert f'{first.exchange}:{first.market_id}' not in runner._session.state.markets
        assert (first.exchange,first.market_id) not in runner._session.engine.books
        assert len(runner._session._market_keys)<=4
        assert len(runner._session.retired_markets)<=16
    finally:
        await runner._session.finish()
        await store.aclose()
