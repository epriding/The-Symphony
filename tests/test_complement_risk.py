from dataclasses import replace

import pytest

from src.paper_trading.models import FeeSchedule
from src.paper_trading.venue_rules import fixture_venue
from src.utilities.classes import OrderIntentBatch
from test_paper_trading import make_session, book, signal, START


def pair(session, **changes):
    values = dict(legs=[signal(session, exchange='A', outcome='YES', price=.4,
                              estimated_probability=None),
                        signal(session, exchange='B', outcome='NO', price=.5,
                              estimated_probability=None)],
                  quantity_ratios=[1, 1], capital_ceiling=5,
                  execution_policy='paper_independent_legs',
                  execution_deadline=session.clock()+1, expires_at=session.clock()+1,
                  max_quantity=5, min_net_profit_per_pair=.02, market_equivalence='test-window')
    values.update(changes)
    return OrderIntentBatch(**values)


@pytest.mark.asyncio
async def test_pair_approves_equal_quantities_without_directional_probabilities():
    session = await make_session()
    await book(session, exchange='A', bids={.39:100}, asks={.4:100})
    await book(session, exchange='B', bids={.5:100}, asks={.51:100})
    result = await session.submit_batch(pair(session))
    assert result.decision.approved
    assert [order.size for order in result.decision.orders] == [5, 5]
    assert len(session.state.portfolios['PAPER'].reservations) == 2
    await session.finish()


@pytest.mark.asyncio
async def test_resize_that_erases_net_edge_rejects_both_legs():
    rules = {'A':fixture_venue('A', fees=FeeSchedule('us','polymarket_us','.095')),
             'B':fixture_venue('B')}
    session = await make_session(rules=rules, position_cap=.6)
    await book(session, exchange='A', bids={.39:100}, asks={.4:100})
    await book(session, exchange='B', bids={.43:100}, asks={.44:100})
    batch = pair(session, min_net_profit_per_pair=.005,
                 legs=[signal(session, exchange='A', price=.4, estimated_probability=None),
                       signal(session, exchange='B', outcome='NO', price=.57, estimated_probability=None)])
    result = await session.submit_batch(batch)
    assert not result.decision.approved and result.decision.reason == 'pair_edge_erased_by_sizing'
    assert not session.engine.orders
    assert not session.state.portfolios['PAPER'].reservations
    await session.finish()


@pytest.mark.asyncio
async def test_stale_or_unaffordable_pair_leaves_no_partial_submission():
    session = await make_session()
    await book(session, exchange='A', bids={.39:100}, asks={.4:100})
    await book(session, exchange='B', bids={.5:100}, asks={.51:100})
    session.clock.advance(START+6)
    result = await session.submit_batch(pair(session))
    assert not result.decision.approved
    assert not session.engine.orders
    assert not session.state.portfolios['PAPER'].reservations
    await session.finish()


@pytest.mark.asyncio
async def test_pair_metadata_cannot_claim_two_yes_legs_or_nonfinite_profit():
    session = await make_session()
    batch = pair(session)
    invalid = [replace(batch, min_net_profit_per_pair=float('nan')),
               replace(batch, legs=[batch.legs[0], replace(batch.legs[1], outcome='YES')])]
    manager = session.state.risk_managers['PAPER']
    for item in invalid:
        assert manager.evaluate_batch(item, session.state._snapshot()).reason == 'invalid_complement_pair'
    await session.finish()
