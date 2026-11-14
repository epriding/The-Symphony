from decimal import Decimal
import pytest
from examples.complement_smoke import run_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario,expected', [('paired',Decimal('50.06')),
    ('recovery',Decimal('49.95')),('no_trade',Decimal('50'))])
async def test_configured_fee_aware_runtime_reconciles_to_independent_cash_totals(tmp_path,scenario,expected):
    result=await run_fixture(tmp_path/scenario,scenario=scenario,windows=1)
    summary=result['summary']
    cash=sum(Decimal(str(value)) for value in summary['accounts']['PAPER']['cash'].values())
    assert cash==expected
    assert summary['complement']['unresolved_positions']==0
    assert summary['accounts']==result['replay']['accounts']
    assert summary['complement']==result['replay']['complement']
    if scenario=='paired':
        assert summary['complement']['paired']==1
        assert Decimal(summary['complement']['entry_fees'])==Decimal('.04')
    if scenario=='recovery':
        assert summary['complement']['failed']==1
        assert Decimal(summary['complement']['recovery_fees'])==Decimal('.02')
    if scenario=='no_trade':
        assert summary['submitted']==0
        assert summary['complement']['decision_reasons']['no_net_edge']>0


@pytest.mark.asyncio
async def test_automatic_roll_skip_and_delayed_old_settlement_replay(tmp_path):
    result=await run_fixture(tmp_path/'rolling',scenario='rolling',windows=3)
    summary=result['summary']
    assert result['visited_windows']==3
    assert summary['complement']['paired']==2
    assert summary['complement']['activated_windows']==2
    assert sum(summary['accounts']['PAPER']['cash'].values())==pytest.approx(50.12)
    assert summary['history']==result['replay']['history']


@pytest.mark.asyncio
async def test_daily_stop_cancels_entry_allows_recovery_and_blocks_next_window(tmp_path):
    result=await run_fixture(tmp_path/'daily',scenario='daily_stop',windows=2)
    summary=result['summary']
    account=summary['accounts']['PAPER']
    assert account['daily']['halted'] and account['daily']['overshoot']>0
    assert summary['complement']['daily_stops']==1
    assert summary['complement']['failed']==1 and summary['complement']['rejected']>=1
    assert summary['complement']['unresolved_positions']==0
    assert sum(account['cash'].values())<45
    assert Decimal(summary['complement']['recovery_fees'])==Decimal('.02')


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario',['neither','stale'])
async def test_rejected_or_unfilled_opportunity_cannot_create_free_paper_profit(tmp_path,scenario):
    result=await run_fixture(tmp_path/scenario,scenario=scenario,windows=1)
    summary=result['summary']
    assert sum(summary['accounts']['PAPER']['cash'].values())==50
    assert summary['complement']['unresolved_positions']==0
    if scenario=='neither':
        assert summary['zero_fills']==2 and summary['complement']['unfilled']==1
    else:
        assert summary['submitted']==0 and summary['complement']['decision_reasons']['stale_depth']>0


@pytest.mark.asyncio
async def test_prepared_future_window_is_not_an_early_entry():
    from test_complement_recovery import configured
    from test_paper_trading import START
    session,strategy=await configured()
    try:
        for terms in strategy.context.settings['terms'].values():
            terms[2]=START+300;terms[3]=START+1200
        assert await strategy.on_snapshot(session.state._snapshot()) is None
        assert not session.engine.orders
    finally:await session.finish()


@pytest.mark.asyncio
async def test_three_window_checkpoint_keeps_delayed_settlement_and_daily_state(tmp_path):
    result=await run_fixture(tmp_path/'three-window-stop',scenario='rolling_daily',windows=3)
    summary=result['summary']
    assert summary['complement']['paired']==1 and summary['complement']['failed']==1
    assert summary['complement']['activated_windows']==2
    assert summary['complement']['daily_stops']==1
    assert summary['accounts']['PAPER']['daily']['start_equity']==50
    assert sum(summary['accounts']['PAPER']['cash'].values())==pytest.approx(44.75)
    assert summary['complement']['unresolved_positions']==0
