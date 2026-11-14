import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.paper_trading.clock import VirtualClock
from src.paper_trading.recording import encode, decode
from src.paper_trading.runtime import BotRunner
from src.strategies.context import StrategyContext
from src.utilities.classes import Side
from bot_v1_support import valid_config, market, START


def decision():
    context = StrategyContext('pair', 'PAPER', {}, VirtualClock(START), 7, 'test')
    legs = [context.new_intent(exchange=venue, market_id='m', outcome=outcome,
             side=Side.BUY, price=.4) for venue, outcome in [('KALSHI', 'YES'), ('POLYMARKET_US', 'NO')]]
    batch = context.new_batch(legs=legs, quantity_ratios=[1, 1], capital_ceiling=1,
                              execution_policy='paper_independent_legs',
                              execution_deadline=START+1, expires_at=START+1)
    return context, batch


def validate(result, limit=8):
    limits = valid_config()['runtime']['limits'] | {'max_intents_per_callback': limit}
    markets = {f'{v}:m': market(v) for v in ('KALSHI', 'POLYMARKET_US')}
    return BotRunner._validated_result(result, strategy_name='pair', markets=markets, limits=limits)


def test_pair_context_is_deterministic_and_serializable():
    _, batch = decision()
    assert encode(batch) == encode(decision()[1])
    assert validate([batch]) == [batch]
    assert decode(encode(batch)) == batch


def test_limits_count_legs_and_duplicates_across_batches():
    _, batch = decision()
    with pytest.raises(ValueError):
        validate([batch], limit=1)
    with pytest.raises(ValueError):
        validate([batch, batch.legs[0]])
    with pytest.raises(ValueError):
        validate([batch, replace(batch, batch_id='different')])


@pytest.mark.parametrize('changes', [
    {'capital_ceiling': float('nan')}, {'quantity_ratios': [1]},
    {'execution_policy': 'paper_all_or_none'}, {'execution_deadline': None},
])
def test_invalid_batch_is_rejected_before_submission(changes):
    with pytest.raises(ValueError):
        validate([replace(decision()[1], **changes)])


def test_context_cannot_forge_foreign_strategy_or_batch_id():
    context, batch = decision()
    with pytest.raises(ValueError):
        context.new_batch(legs=[replace(batch.legs[0], strategy='foreign')],
                          quantity_ratios=[1], capital_ceiling=1)
    with pytest.raises(ValueError):
        context.new_batch(legs=batch.legs, quantity_ratios=[1, 1], capital_ceiling=1, batch_id='forged')


@pytest.mark.asyncio
async def test_runtime_routes_whole_pair_to_existing_batch_gateway():
    _, batch = decision()
    runner = object.__new__(BotRunner)
    runner._zombies, runner._stopping = {}, False
    runner._stop = asyncio.Event()
    runner._status = {'strategies': {'pair': {'ready': True}}}
    runner._session = SimpleNamespace(submit=AsyncMock(), submit_batch=AsyncMock())
    runner._control = lambda *a, **k: None
    runner._validate_result = lambda slot, result: validate(result)
    future = asyncio.get_running_loop().create_future()
    future.set_result([batch])
    slot = SimpleNamespace(entry={'name': 'pair'}, disabled=False, task=future, dispatch=1,
                           timing=SimpleNamespace(completed_at=1, deadline=2), snapshot_bytes=0)
    runner.slots = [slot]
    await runner._complete()
    runner._session.submit_batch.assert_awaited_once_with(batch)
    runner._session.submit.assert_not_called()
