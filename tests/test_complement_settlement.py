import pytest
from src.utilities.classes import Side
from src.utilities.market_events import SettlementEvent
from test_complement_risk import pair
from test_paper_trading import make_session, book, START, envelope


@pytest.mark.asyncio
async def test_confirmed_matched_holdings_survive_single_leg_stop_and_partial_settlement():
    session = await make_session()
    await book(session, exchange='A', bids={.39:100}, asks={.4:100})
    await book(session, exchange='B', bids={.5:100}, asks={.51:100})
    batch = pair(session, max_quantity=1)
    await session.submit_batch(batch)
    await session.advance(START+.05)
    await book(session, exchange='A', bids={.1:100}, asks={.11:100}, at=START+.1)
    manager = session.state.risk_managers['PAPER']
    account = session.state.portfolios['PAPER']
    assert not manager.stop_loss_intents(session.state._snapshot())
    session.publish(envelope(session, SettlementEvent(exchange='A', market_id='m', timestamp=START+.2, settlement_id='a', payouts={'YES':'1','NO':'0'}), START+.2))
    await session.advance(START+.25)
    assert account.balances['A']['USD'].cash == pytest.approx(10000.6)
    assert 'B:m:NO' in account.positions
    await book(session, exchange='B', bids={.8:100}, asks={.9:100}, at=START+.3)
    assert not manager.stop_loss_intents(session.state._snapshot())
    event = SettlementEvent(exchange='B', market_id='m', timestamp=START+.4, settlement_id='b', payouts={'YES':'1','NO':'0'})
    session.publish(envelope(session,event,START+.4))
    await session.advance(START+.45)
    # Both venues can resolve YES; actual NO loss remains visible, no guaranteed payout credited.
    assert account.balances['B']['USD'].cash == pytest.approx(9999.5)
    assert not account.positions
    before = account.balances['B']['USD'].cash
    session.publish(envelope(session,event,START+.5))
    await session.advance(START+.55)
    assert account.balances['B']['USD'].cash == before
    assert not account.complement_pairs
    await session.finish()


@pytest.mark.asyncio
async def test_single_filled_leg_and_fake_tag_get_no_matched_exemption():
    from test_paper_trading import submit_size
    session = await make_session()
    await book(session, bids={.49:100}, asks={.5:100})
    await submit_size(session, 1, reason='immediate_complement')
    await session.advance(START+.05)
    await book(session, bids={.1:100}, asks={.2:100}, at=START+.1)
    exits = session.state.risk_managers['PAPER'].stop_loss_intents(session.state._snapshot())
    assert len(exits) == 1 and exits[0].side == Side.SELL
    await session.finish()
