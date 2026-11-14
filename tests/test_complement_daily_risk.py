from datetime import datetime, timezone
import pytest
from src.utilities.classes import RiskConfig, Position, Side
from test_paper_trading import make_session, book, signal, START


def enable(session, pct=10):
    manager = session.state.risk_managers['PAPER']
    manager.config.daily_loss_limit_pct = pct
    manager.config.validate()
    manager.update_daily_risk(session.state._risk_view())
    return manager, session.state.portfolios['PAPER']


@pytest.mark.asyncio
async def test_exact_daily_threshold_latches_until_next_utc_day():
    session = await make_session()
    await book(session)
    manager, account = enable(session)
    initial = sum(v['USD'].cash for v in account.balances.values())
    account.balances['A']['USD'].cash -= initial*.1
    manager.update_daily_risk(session.state._risk_view())
    assert account.daily_halted
    assert manager.evaluate(signal(session), session.state._risk_view()).reason == 'daily_loss_limit'
    account.balances['A']['USD'].cash += initial*.1
    manager.update_daily_risk(session.state._risk_view())
    assert account.daily_halted
    highwater = account.equity_high_water
    session.clock.advance((int(START)//86400+1)*86400)
    manager.update_daily_risk(session.state._risk_view())
    assert not account.daily_halted and account.daily_start_equity == initial
    assert account.equity_high_water == highwater
    await session.finish()


@pytest.mark.asyncio
async def test_daily_values_full_executable_depth_and_refuses_stale_inventory():
    session = await make_session()
    manager, account = enable(session)
    position = Position(market_id='m', exchange='A', outcome='YES', size=10,
                        entry_price=.5, side=Side.BUY, strategy='one', category='other')
    account.positions['A:m:YES'] = position
    await book(session, bids={.4:2,.2:8}, asks={.5:100})
    value = manager.daily_equity(session.state._risk_view())
    assert value == 20002.4
    session.clock.advance(START+6)
    assert manager.daily_equity(session.state._risk_view()) is None
    await book(session, exchange='B')
    assert manager.evaluate(signal(session, exchange='B'), session.state._risk_view()).reason == 'daily_valuation_unavailable'
    await session.finish()


@pytest.mark.parametrize('pct',[9,16,float('nan'),True])
def test_daily_percentage_rejects_outside_approved_range(pct):
    with pytest.raises(ValueError):
        RiskConfig(daily_loss_limit_pct=pct).validate()


@pytest.mark.asyncio
async def test_halted_day_allows_reduce_only_and_subtracts_exit_fee_once():
    from test_paper_trading import submit_size
    session = await make_session()
    await book(session)
    manager, account = enable(session)
    await submit_size(session, 1)
    await session.advance(START+.05)
    account.daily_halted = True
    key = 'A:m:YES'
    assert key in account.positions
    gross = sum(v['USD'].cash for v in account.balances.values())+.49
    manager.fee_reserve_provider = lambda *args: .01
    assert manager.daily_equity(session.state._risk_view()) == pytest.approx(gross-.01)
    result = manager.evaluate(signal(session, side=Side.SELL, price=.49, reduce_only=True),
                              session.state._risk_view())
    assert result.approved
    await session.finish()


@pytest.mark.asyncio
async def test_fresh_unsellable_inventory_is_zero_valued_and_still_trips_daily_stop():
    session=await make_session()
    manager,account=enable(session)
    account.positions['A:m:YES']=Position('m','A','YES',10000,.5,Side.BUY,'one','PAPER')
    account.balances['A']['USD'].cash-=5000
    await book(session,bids={.1:1},asks={.5:100})
    assert manager.daily_equity(session.state._risk_view()) == 15000.1
    assert not account.daily_valuation_complete
    manager.update_daily_risk(session.state._risk_view())
    assert account.daily_halted
    await session.finish()


@pytest.mark.asyncio
async def test_zero_daily_baseline_cannot_authorize_entries():
    session=await make_session();await book(session)
    manager,account=enable(session)
    account.daily_start_equity=0
    assert manager.evaluate(signal(session),session.state._risk_view()).reason=='daily_baseline_unavailable'
    await session.finish()


@pytest.mark.asyncio
async def test_missing_midnight_quotes_cannot_shift_baseline_down_to_erase_gap_loss():
    from test_paper_trading import submit_size
    session=await make_session();await book(session)
    manager,account=enable(session)
    await submit_size(session,1)
    await session.advance(START+.05)
    manager.update_daily_risk(session.state._risk_view())
    boundary_anchor=manager.daily_equity(session.state._risk_view())
    midnight=(int(START)//86400+1)*86400
    session.clock.advance(midnight+1)
    assert manager.update_daily_risk(session.state._risk_view())=='daily_valuation_unavailable'
    account.balances['A']['USD'].cash-=2000
    await book(session)
    manager.update_daily_risk(session.state._risk_view())
    assert account.daily_start_equity==boundary_anchor
    assert account.daily_halted
    await session.finish()
