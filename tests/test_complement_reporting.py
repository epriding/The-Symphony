import pytest
from test_complement_recovery import configured,step
from test_paper_trading import START, book


@pytest.mark.asyncio
async def test_successful_group_stays_successful_after_entry_deadline():
    session,strategy=await configured(slow_b=False)
    try:
        await step(session,strategy)
        await session.advance(START+.6)
        group=next(iter(session.engine.groups.values()))
        assert not group['failed']
    finally:await session.finish()


@pytest.mark.asyncio
async def test_unhedged_duration_stops_at_actual_recovery_fill():
    session,strategy=await configured(slow_b=True)
    try:
        await step(session,strategy)
        await book(session, exchange='B', bids={.3:100},asks={.31:100},at=START+.02)
        await session.advance(START+.25)
        await step(session,strategy)
        await session.advance(START+.3)
        group=next(iter(session.engine.groups.values()))
        seconds=session.engine._group_detail(group['batch'].batch_id,group)['unmatched_seconds']
        assert seconds == pytest.approx(.25,abs=.02)
        await session.advance(START+1)
        assert session.engine._group_detail(group['batch'].batch_id,group)['unmatched_seconds'] == seconds
    finally:await session.finish()


@pytest.mark.asyncio
async def test_summary_separates_projected_actual_and_unresolved_results(tmp_path):
    from test_complement_replay import captured_pair
    from src.bot import replay_bot
    summary=await captured_pair(tmp_path)
    result=summary['complement']
    assert result['paired']==1 and result['attempted']==1 and result['failed']==0
    assert result['projected_surplus']=='.1' or float(result['projected_surplus'])==pytest.approx(.1)
    assert result['realized_net_pnl']==pytest.approx(.1)
    assert result['held_capital']==0 and result['unresolved_positions']==0
    assert result['entry_fees']=='0' and result['recovery_fees']=='0'
    replayed=await replay_bot(tmp_path/'capture',tmp_path/'report-replay')
    assert replayed['complement']==result
