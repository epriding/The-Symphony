"""Behavioral acceptance coverage for the event-driven engine and ledger."""
from dataclasses import replace, asdict
from decimal import Decimal
import asyncio
import json
import pytest

from src.paper_trading.clock import VirtualClock
from src.paper_trading.fees import FeeAccumulator, FeeModel
from src.paper_trading.models import Delay, FeeSchedule, PaperExecutionConfig, CashCredit
from src.paper_trading.venue_rules import fixture_venue
from src.paper_trading.session import PaperSession
from src.paper_trading.replay import replay
from src.utilities.classes import (
    MarketInfo, OrderIntent, OrderIntentBatch, OrderBookSnapshotEvent,
    LiteOrderBookSnapshotEvent, TradeEvent, Side, TimeInForce, OrderStatus,
    TradingConfig, RiskConfig,
)
from src.utilities.market_events import PublicEventEnvelope, PublicConnectionEvent, SettlementEvent

START = 1_800_000_000.0


async def make_session(*, rules=None, processing=0, capture=None, position_cap=1000):
    config = TradingConfig()
    config.strategy_portfolios = {'one': 'PAPER', 'two': 'PAPER'}
    config.paper_starting_balances = {'PAPER': {'A': 10000, 'B': 10000}}
    config.risk_configs['PAPER'] = RiskConfig(diversification={'other': 1},
        max_position_exposure=position_cap, max_total_exposure=20000)
    config.paper_execution = PaperExecutionConfig(rules or {'A': fixture_venue('A'), 'B': fixture_venue('B')},
                                                 processing_delay=Delay(processing), seed=42)
    session = PaperSession(config, VirtualClock(START), capture)
    for exchange in config.paper_execution.venues:
        await session.add_market(MarketInfo('m', exchange, 'm', {}, True, {'YES': True, 'NO': True},
            tick_size=.01, quantity_step=1, min_quantity=1, fee_per_contract=0,
            resolution_date=START+3600))
    session.test_index = 0
    return session


def signal(session, **changes):
    session.test_index += 1
    base = dict(market_id='m', exchange='A', side=Side.BUY, outcome='YES', price=.51,
        strategy='one', estimated_probability=1, created_at=session.clock(),
        intent_id=f'intent-{session.test_index}', resolution_date=START+3600)
    base.update(changes)
    return OrderIntent(**base)


def envelope(session, event, at=None, **changes):
    session.test_index += 1
    at = session.clock() if at is None else at
    base = dict(event=event, received_at=at, available_at=at,
                ingest_index=session.test_index, source_id=f'source-{session.test_index}')
    base.update(changes)
    return PublicEventEnvelope(**base)


async def book(session, bids=None, asks=None, at=None, exchange='A', **changes):
    at = session.clock() if at is None else at
    event = OrderBookSnapshotEvent('m', 'YES', at, exchange,
        bids or {.49: 100}, asks if asks is not None else {.50: 40, .51: 60})
    session.publish(envelope(session, event, at, **changes))
    await session.advance(at)


async def submit_size(session, quantity, **changes):
    intent = signal(session, **changes)
    manager = session.state.risk_managers['PAPER']
    portfolio = session.state.portfolios['PAPER']
    market = session.state.markets[f'{intent.exchange}:m']
    reserve = manager.fee_reserve_provider(intent, market, quantity, intent.price)
    existing = manager.get_exposure(session.state._snapshot()).by_market.get(f'{intent.exchange}:m', 0)
    manager.config.max_position_exposure = existing + quantity * intent.price + reserve
    result = await session.submit(intent)
    assert result.status == 'pending', result.decision
    assert result.decision.orders[0].size == quantity
    return result.submission_ids[0]


async def trade(session, quantity, price=.49, side=Side.SELL, trade_id=None, at=None, **changes):
    at = session.clock() if at is None else at
    event = TradeEvent('m', 'YES', at, 'A', price, quantity, side, trade_id or f'trade-{session.test_index}')
    session.publish(envelope(session, event, at, **changes))
    await session.advance(at)


def portfolio(session):
    return session.state.portfolios['PAPER']


def record(session, order_id, exchange='A'):
    return portfolio(session).order_history[f'{exchange}:{order_id}']


@pytest.mark.asyncio
@pytest.mark.parametrize('tif,bound,filled,status', [
    (TimeInForce.FOK, .51, 70, OrderStatus.FILLED),
    (TimeInForce.IOC, .50, 40, OrderStatus.CANCELED),
    (TimeInForce.FOK, .50, 0, OrderStatus.CANCELED),
])
async def test_a01_a03_depth_and_fok_transaction(tif, bound, filled, status):
    session = await make_session()
    await book(session)
    oid = await submit_size(session, 70, price=bound, tif=tif)
    assert portfolio(session).reservations and not portfolio(session).positions
    await session.advance(session.clock())
    result = record(session, oid)
    assert result.size == 70 and result.filled_quantity == filled and result.status == status
    assert not portfolio(session).reservations and not portfolio(session).open_orders
    notional = Decimal('35.30') if filled == 70 else Decimal(filled) * Decimal('.50')
    assert portfolio(session).balances['A']['USD'].cash == float(Decimal(10000)-notional)
    assert sum(Decimal(e.notional) for e in session.engine.executions) == notional
    if filled:
        assert result.average_fill_price == pytest.approx(float(notional/filled))
    else:
        assert session.engine.books[('A', 'm')].available['SELL'][Decimal('.5')] == 40


@pytest.mark.asyncio
async def test_a04_sell_depth_and_cash_round_trip():
    session = await make_session()
    await book(session)
    oid = await submit_size(session, 70)
    await session.advance(session.clock())
    await book(session, bids={.53: 30, .52: 40}, asks={.55: 100})
    result = await session.submit(signal(session, side=Side.SELL, reduce_only=True,
        estimated_probability=None, resolution_date=None, price=.52))
    await session.advance(session.clock())
    sold = record(session, result.submission_ids[0])
    assert sold.cumulative_notional == '36.70'
    assert not portfolio(session).positions
    assert portfolio(session).balances['A']['USD'].cash == pytest.approx(10001.4)
    assert portfolio(session).realized_pnl == pytest.approx(1.4)


@pytest.mark.asyncio
async def test_a05_a06_shared_depth_and_replenishment():
    session = await make_session()
    await book(session, asks={.5: 100})
    first = await submit_size(session, 70, price=.5, tif=TimeInForce.IOC)
    await session.advance(session.clock())
    await book(session, asks={.5: 100})
    second = await submit_size(session, 70, price=.5, tif=TimeInForce.IOC, strategy='two')
    await session.advance(session.clock())
    assert record(session, first).filled_quantity == 70
    assert record(session, second).filled_quantity == 30
    await book(session, asks={.5: 120})
    assert session.engine.books[('A', 'm')].available['SELL'][Decimal('.5')] == 20
    await book(session, asks={})
    await book(session, asks={.5: 100})
    assert session.engine.books[('A', 'm')].available['SELL'][Decimal('.5')] == 100


@pytest.mark.asyncio
async def test_a08_arrival_book_obeys_price_bound():
    session = await make_session(rules={'A': fixture_venue('A', outbound=Delay(.2))})
    await book(session, asks={.5: 100})
    oid = await submit_size(session, 10, price=.5)
    await session.advance(START+.01)  # dispatch before the market moves
    await book(session, asks={.55: 100}, at=START+.1)
    await session.advance(START+.3)
    assert record(session, oid).filled_quantity == 0
    assert record(session, oid).reason == 'insufficient_depth_fok'


@pytest.mark.asyncio
@pytest.mark.parametrize('processing,outbound,expected', [(3, 0, 0), (0, 3, 10)])
async def test_a09_approval_lifetime_ends_at_dispatch(processing, outbound, expected):
    session = await make_session(processing=processing,
        rules={'A': fixture_venue('A', outbound=Delay(outbound))})
    await book(session, asks={.5: 100})
    oid = await submit_size(session, 10, price=.5)
    await session.advance(START+3.1)
    assert record(session, oid).filled_quantity == expected


@pytest.mark.asyncio
async def test_a10_a11_queue_requires_real_volume_and_no_fill_on_touch():
    session = await make_session()
    await book(session, bids={.49: 100}, asks={.51: 100})
    oid = await submit_size(session, 10, price=.49, tif=TimeInForce.GTC)
    await session.advance(session.clock())
    await book(session, bids={.49: 0}, asks={.50: 100})  # cancellations don't improve queue
    await trade(session, 60, trade_id='one')
    assert record(session, oid).filled_quantity == 0
    await trade(session, 45, trade_id='two')
    assert record(session, oid).filled_quantity == 5
    assert portfolio(session).open_orders[f'A:{oid}'].remaining_quantity == 5
    await trade(session, 100, price=.48, trade_id='through')
    assert record(session, oid).filled_quantity == 5


@pytest.mark.asyncio
async def test_a12_trade_budget_shared_between_local_orders():
    session = await make_session()
    await book(session, bids={.49: 10}, asks={.51: 100})
    one = await submit_size(session, 10, price=.49, tif=TimeInForce.GTC)
    await session.advance(session.clock())
    two = await submit_size(session, 10, price=.49, tif=TimeInForce.GTC, strategy='two')
    await session.advance(session.clock())
    await trade(session, 25)
    assert record(session, one).filled_quantity == 10
    assert record(session, two).filled_quantity == 5


@pytest.mark.asyncio
async def test_a13_cancel_loses_to_fill_and_gtd_expires():
    session = await make_session(rules={'A': fixture_venue('A', cancel=Delay(.2))})
    await book(session, bids={.49: 1}, asks={.51: 100})
    oid = await submit_size(session, 10, price=.49, tif=TimeInForce.GTC)
    await session.advance(session.clock())
    assert await session.cancel(oid, 'A')
    assert portfolio(session).open_orders[f'A:{oid}'].remaining_quantity == 10
    await trade(session, 11, at=START+.1)
    await session.advance(START+.3)
    assert record(session, oid).status == OrderStatus.FILLED
    expiry = await submit_size(session, 10, strategy='two', price=.48, tif=TimeInForce.GTD,
                               good_till_date=START+.4)
    await session.advance(START+.5)
    assert record(session, expiry).status == OrderStatus.EXPIRED


@pytest.mark.asyncio
async def test_a14_taker_then_maker_same_order():
    session = await make_session()
    await book(session, bids={.49: 1}, asks={.50: 5, .52: 100})
    oid = await submit_size(session, 10, price=.50, tif=TimeInForce.GTC)
    await session.advance(session.clock())
    await trade(session, 5, price=.50)
    assert record(session, oid).status == OrderStatus.FILLED
    assert [fill.role for fill in session.engine.executions] == ['taker', 'maker']


@pytest.mark.asyncio
async def test_a15_reports_before_ack_and_duplicate_reports():
    session = await make_session(rules={'A': fixture_venue('A', acknowledgement=Delay(.4))})
    await book(session)
    oid = await submit_size(session, 70)
    await session.advance(START+.1)
    saved = session.engine.get_order(oid)
    cash = portfolio(session).balances['A']['USD'].cash
    await session.coordinator.resolve_paper_submission(oid)
    await session.advance(START+.5)
    assert portfolio(session).balances['A']['USD'].cash == cash
    assert record(session, oid).status == OrderStatus.FILLED
    await session.coordinator.apply_paper_update(saved, session.engine.orders[oid].approved)
    assert portfolio(session).positions['A:m:YES'].size == 70


@pytest.mark.asyncio
async def test_a16_gap_invalidates_both_outcomes_and_resets_queue():
    session = await make_session()
    await book(session, bids={.49: 100}, asks={.51: 100})
    oid = await submit_size(session, 10, price=.49, tif=TimeInForce.GTC)
    await session.advance(session.clock())
    event = PublicConnectionEvent(exchange='A', timestamp=session.clock(), connected=False)
    session.publish(envelope(session, event))
    await session.advance(session.clock())
    assert 'A:m:YES' not in session.state.orderbooks and 'A:m:NO' not in session.state.orderbooks
    await trade(session, 1000)
    assert record(session, oid).filled_quantity == 0
    await book(session, bids={.49: 10}, asks={.51: 100}, connection_epoch=1)
    await trade(session, 15)
    assert record(session, oid).filled_quantity == 5
    assert any(row['event'] == 'queue_reset' for row in session.engine.audit)


@pytest.mark.asyncio
async def test_a18_fee_credit_and_a24_settlement_are_idempotent():
    fee = FeeSchedule('us-example', 'polymarket_us', '.0695', maker_rebate='.0035')
    session = await make_session(rules={'A': fixture_venue('A', fees=fee)})
    await book(session, bids={.49: 1}, asks={.5: 100})
    oid = await submit_size(session, 100, price=.5)
    await session.advance(session.clock())
    assert record(session, oid).fees_paid == 1.74
    credit = CashCredit('earned', 'PAPER', 'A', '.25', 'one', oid, session.clock())
    await session.coordinator.apply_paper_credit(credit)
    await session.coordinator.apply_paper_credit(credit)
    settlement = SettlementEvent(exchange='A', market_id='m', timestamp=session.clock(),
                                 settlement_id='settled-once', payouts={'YES': '1', 'NO': '0'})
    session.publish(envelope(session, settlement))
    await session.advance(session.clock())
    session.publish(envelope(session, settlement))
    await session.advance(session.clock())
    assert not portfolio(session).positions
    assert portfolio(session).balances['A']['USD'].cash == 10048.51
    assert portfolio(session).realized_pnl == pytest.approx(48.51)


def test_kalshi_account_rounding_example_and_fragmentation():
    model = FeeModel(FeeSchedule('example', 'kalshi', '.07', balance_precision='.01'))
    acc = FeeAccumulator()
    fee, _ = model.charge('1', '.055', 'BUY', 'taker', acc)
    assert fee == Decimal('.005')
    assert Decimal('-.055') - fee == Decimal('-.060')
    assert model.reserve('1', '.01', 'BUY', '.9') >= fee


@pytest.mark.asyncio
async def test_a20_independent_legs_keep_successful_inventory():
    session = await make_session()
    await book(session, asks={.5: 100})
    await book(session, asks={.5: 1}, exchange='B')
    legs = [signal(session, price=.5, estimated_probability=None),
            signal(session, exchange='B', price=.5, estimated_probability=None)]
    result = await session.submit_batch(OrderIntentBatch(legs=legs, quantity_ratios=[1,1],
        capital_ceiling=10, execution_policy='paper_independent_legs', execution_deadline=START+.5))
    assert result.status == 'pending'
    await session.advance(START+.6)
    assert portfolio(session).positions['A:m:YES'].size == 10
    assert 'B:m:YES' not in portfolio(session).positions
    assert not portfolio(session).reservations


@pytest.mark.asyncio
async def test_a21_rate_limiting_does_not_stall_other_venue():
    session = await make_session(rules={'A': fixture_venue('A', request_capacity=1, request_rate=1),
                                       'B': fixture_venue('B')})
    await book(session, asks={.5: 1000})
    await book(session, asks={.5: 1000}, exchange='B')
    first = await submit_size(session, 10, price=.5)
    second = await submit_size(session, 10, price=.5, strategy='two')
    other_result = await session.submit(signal(session, price=.5, exchange='B'))
    assert other_result.status == 'pending'
    other = other_result.submission_ids[0]
    await session.advance(session.clock())
    assert record(session, first).status == OrderStatus.FILLED
    assert record(session, second).reason == 'venue_rate_limited'
    assert record(session, other, 'B').status == OrderStatus.FILLED


@pytest.mark.asyncio
async def test_a22_capture_replay_exact_and_a23_no_future_fill(tmp_path):
    capture = tmp_path / 'capture'
    session = await make_session(capture=capture, position_cap=35.7)
    await book(session)
    await session.advance(START+.01)
    result = await session.submit(signal(session))
    await session.advance(START+.02)
    await book(session, bids={.53: 100}, asks={.55: 100}, at=START+.1)
    original = await session.finish()
    restored, summary = await replay(capture)
    assert summary['accounts'] == original['accounts']
    assert [asdict(e) for e in restored.engine.executions] == [asdict(e) for e in session.engine.executions]
    again, _ = await replay(capture)
    assert again.engine.audit == restored.engine.audit
    assert summary['missing_markouts'] > 0


@pytest.mark.asyncio
async def test_a23_capture_end_does_not_execute_future_arrival():
    session = await make_session(rules={'A': fixture_venue('A', outbound=Delay(1))})
    await book(session)
    oid = await submit_size(session, 10)
    await session.advance(START+.1)
    summary = await session.finish()
    assert not session.engine.executions
    assert summary['accounts']['PAPER']['pending_reservations'] == 1


@pytest.mark.asyncio
async def test_a27_ingestion_and_processing_delay_prevent_early_execution():
    session = await make_session(processing=.1)
    event = OrderBookSnapshotEvent('m', 'YES', START, 'A', {.49: 100}, {.5: 100})
    session.publish(envelope(session, event, START, available_at=START+.2))
    await session.advance(START+.1)
    assert not session.state.orderbooks
    await session.advance(START+.2)
    oid = await submit_size(session, 10, price=.5)
    await session.advance(START+.25)
    assert not portfolio(session).positions
    await session.advance(START+.31)
    assert record(session, oid).filled_quantity == 10


@pytest.mark.asyncio
async def test_a05_mirrored_no_sell_and_yes_buy_compete_for_same_ask():
    session = await make_session()
    await book(session, bids={.5:100}, asks={.51:100})
    owned = await submit_size(session, 60, price=.5, outcome='NO')
    await session.advance(session.clock())
    await book(session, bids={.49:100}, asks={.5:100})
    yes = await submit_size(session, 70, price=.5, strategy='two', tif=TimeInForce.IOC)
    await session.advance(session.clock())
    result = await session.submit(signal(session, outcome='NO', side=Side.SELL, price=.5,
        reduce_only=True, estimated_probability=None, resolution_date=None, tif=TimeInForce.IOC))
    await session.advance(session.clock())
    assert record(session, yes).filled_quantity == 70
    assert record(session, result.submission_ids[0]).filled_quantity == 30
    assert portfolio(session).positions['A:m:NO'].size == 30


@pytest.mark.asyncio
async def test_a07_duplicate_print_and_book_before_print_never_double_fills():
    session = await make_session()
    await book(session, bids={.49:10}, asks={.51:100})
    oid = await submit_size(session, 10, price=.49, tif=TimeInForce.GTC)
    await session.advance(session.clock())
    await book(session, bids={.49:0}, asks={.51:100})
    await trade(session, 15, trade_id='same-print')
    await trade(session, 15, trade_id='same-print')
    assert record(session, oid).filled_quantity == 5
    assert session.engine.books[('A','m')].quarantined
    await book(session, bids={.49:0}, asks={.51:100})
    assert not session.engine.books[('A','m')].quarantined


@pytest.mark.asyncio
async def test_a15_failed_report_delivery_retained_without_rematching(monkeypatch):
    session = await make_session()
    await book(session, asks={.5:100})
    oid = await submit_size(session, 70, price=.5)
    original = session.coordinator.apply_paper_update
    async def fail(*args):
        raise RuntimeError('temporary accounting delivery outage')
    monkeypatch.setattr(session.coordinator, 'apply_paper_update', fail)
    with pytest.raises(RuntimeError, match='delivery outage'):
        await session.advance(session.clock())
    assert len(session.engine.outbox) == 1
    assert sum(Decimal(fill.quantity) for fill in session.engine.executions) == 70
    monkeypatch.setattr(session.coordinator, 'apply_paper_update', original)
    await session.engine.retry_reports()
    await session.advance(session.clock())
    assert record(session, oid).filled_quantity == 70
    assert portfolio(session).balances['A']['USD'].cash == 9965
    assert len(session.engine.executions) == 1 and not session.engine.outbox


@pytest.mark.asyncio
async def test_lost_ack_keeps_reservation_until_registry_recovery():
    session = await make_session()
    session.engine.config = replace(session.engine.config, lose_acknowledgements=True)
    await book(session)
    oid = await submit_size(session, 10, price=.49, tif=TimeInForce.GTC)
    await session.advance(session.clock())
    assert portfolio(session).reservations and not portfolio(session).open_orders
    await session.coordinator.resolve_paper_submission(oid)
    assert not portfolio(session).reservations and f'A:{oid}' in portfolio(session).open_orders


@pytest.mark.asyncio
async def test_delayed_reports_keep_inventory_and_capital_reserved():
    session = await make_session(rules={'A': fixture_venue('A', acknowledgement=Delay(.3), execution_report=Delay(.2))})
    await book(session, asks={.5:100})
    oid = await submit_size(session, 70, price=.5)
    await session.advance(START+.1)
    assert portfolio(session).reservations and not portfolio(session).positions
    assert session.state.risk_managers['PAPER'].available_cash(portfolio(session), 'A') == 9965
    await session.advance(START+.25)
    assert record(session, oid).status == OrderStatus.FILLED
    await session.advance(START+.4)
    assert portfolio(session).positions['A:m:YES'].size == 70


@pytest.mark.asyncio
async def test_post_only_and_unsupported_atomic_groups_reject_cleanly():
    session = await make_session()
    await book(session)
    oid = await submit_size(session, 10, price=.5, tif=TimeInForce.GTC, post_only=True)
    await session.advance(session.clock())
    assert record(session, oid).reason == 'post_only_would_cross'
    result = await session.submit_batch(OrderIntentBatch(legs=[signal(session)],
        quantity_ratios=[1], capital_ceiling=10, execution_policy='paper_all_or_none'))
    assert result.decision.reason == 'unsupported_batch_execution_policy'
    assert not portfolio(session).positions


@pytest.mark.asyncio
async def test_missing_settlement_terms_leave_positions_unresolved():
    session = await make_session()
    await book(session)
    oid = await submit_size(session, 10)
    await session.advance(session.clock())
    event = SettlementEvent(exchange='A', market_id='m', timestamp=session.clock(),
        settlement_id='missing', payouts={})
    session.publish(envelope(session, event))
    await session.advance(session.clock())
    assert portfolio(session).positions['A:m:YES'].size == 10
    assert not portfolio(session).settlements


@pytest.mark.asyncio
async def test_a19_netted_profile_rejects_opposite_inventory_entry():
    session = await make_session(rules={'A': fixture_venue('A', netted_outcomes=True)})
    await book(session)
    oid = await submit_size(session, 10)
    await session.advance(session.clock())
    result = await session.submit(signal(session, outcome='NO', price=.51))
    assert result.decision.reason == 'opposite_inventory_conflict'
    assert not session.state.portfolios['LIVE'].positions


@pytest.mark.asyncio
async def test_strategy_replay_uses_fresh_session_time_ids_and_randomness(tmp_path):
    capture = tmp_path / 'capture'
    session = await make_session(capture=capture, position_cap=10)

    class Strategy:
        def __init__(self, context):
            self.context, self.sent = context, False

        async def on_snapshot(self, snapshot):
            if self.sent:
                return []
            self.sent = True
            return [self.context.new_intent(market_id='m', exchange='A', side=Side.BUY,
                outcome='YES', price=.5, strategy='one',
                estimated_probability=.8 + self.context.random.random()*.1,
                resolution_date=START+3600)]

    session.strategy = Strategy(session)
    await book(session)
    await session.advance(START+.1)
    original = await session.finish()
    first, first_summary = await replay(capture, strategy_factory=Strategy)
    second, second_summary = await replay(capture, strategy_factory=Strategy)
    assert first.engine.executions == second.engine.executions == session.engine.executions
    assert first_summary['accounts'] == second_summary['accounts'] == original['accounts']
    assert len(first.engine.orders) == 1  # recorded intents are not also submitted


@pytest.mark.asyncio
async def test_participation_is_reported_and_expired_exit_fees_are_unavailable():
    fees = FeeSchedule('temporary', 'flat', '.01', effective_until=START+.2)
    session = await make_session(rules={'A': fixture_venue('A', fees=fees)})
    await book(session, asks={.5:100})
    await submit_size(session, 20, price=.5)
    await session.advance(START+.1)
    alert = next(row for row in session.engine.audit if row['event'] == 'participation_alert')
    assert alert['basis'] == 'displayed_eligible_depth' and Decimal(alert['ratio']) == Decimal('.2')
    await session.advance(START+.3)
    exit_result = await session.submit(signal(session, side=Side.SELL, reduce_only=True,
        price=.49, close_fraction=1, estimated_probability=None))
    assert exit_result.decision.reason == 'paper_fee_profile_unavailable'
    summary = await session.finish()
    account = summary['accounts']['PAPER']
    assert Decimal(account['fees']) == Decimal('.2')
    assert account['gross_pnl'] == pytest.approx(account['net_pnl']+.2)
    assert account['liquidation'][0]['net_proceeds'] is None
    assert account['liquidation'][0]['reason'] == 'exit_fee_schedule_unavailable'


def test_us_maker_fragment_rounding_is_reserved_and_fee_breakdown_reconciles():
    model = FeeModel(FeeSchedule('fragmented-maker', 'polymarket_us', '.07', maker='.07'))
    accumulator = FeeAccumulator()
    total = sum((model.charge(1, '.5', 'BUY', 'maker', accumulator)[0] for _ in range(100)), Decimal(0))
    assert total == Decimal('2.00')
    assert model.reserve(100, 1, 'BUY', '.5') >= total
    details = accumulator.last_breakdown
    assert Decimal(details['charged_fee']) == Decimal('.02')
    kalshi = FeeModel(FeeSchedule('fractional', 'kalshi', '.07', balance_precision='.01'))
    accumulator = FeeAccumulator()
    for _ in range(50):
        fee, _ = kalshi.charge('.01', '.43', 'BUY', 'taker', accumulator)
        details = {key: Decimal(value) for key, value in accumulator.last_breakdown.items()}
        assert fee == details['model_fee']+details['balance_rounding_adjustment']-details['immediate_rounding_refund']


@pytest.mark.asyncio
async def test_settlement_does_not_reveal_delayed_private_fills_early():
    session = await make_session(rules={'A': fixture_venue('A', acknowledgement=Delay(.3),
                                                         execution_report=Delay(.2))})
    await book(session)
    await submit_size(session, 10, price=.5)
    await session.advance(START+.1)
    settlement = SettlementEvent(exchange='A', market_id='m', timestamp=session.clock(),
        settlement_id='delayed-account', payouts={'YES':'1', 'NO':'0'})
    session.publish(envelope(session, settlement))
    await session.advance(START+.15)
    assert portfolio(session).balances['A']['USD'].cash == 10000
    assert not portfolio(session).positions and not portfolio(session).settlements
    assert portfolio(session).reservations
    await session.advance(START+.25)
    assert portfolio(session).positions['A:m:YES'].size == 10
    await session.advance(START+.35)
    assert portfolio(session).balances['A']['USD'].cash == 10005
    assert not portfolio(session).positions and portfolio(session).settlements


@pytest.mark.asyncio
async def test_cancel_response_for_filled_order_obeys_report_delay():
    session = await make_session(rules={'A': fixture_venue('A', acknowledgement=Delay(1),
        execution_report=Delay(1), cancel=Delay(.1))})
    await book(session, asks={.5:100})
    order_id = await submit_size(session, 10, price=.5)
    await session.advance(START+.01)
    assert session.engine.orders[order_id].phase == 'terminal'
    assert await session.cancel(order_id, 'A')
    await session.advance(START+.12)
    assert not portfolio(session).positions
    assert portfolio(session).balances['A']['USD'].cash == 10000
    assert portfolio(session).reservations
    await session.advance(START+1.01)
    assert record(session, order_id).status == OrderStatus.FILLED
    await session.advance(START+1.2)
    assert portfolio(session).positions['A:m:YES'].size == 10
    assert portfolio(session).balances['A']['USD'].cash == 9995


@pytest.mark.asyncio
@pytest.mark.parametrize('legacy', [False, True])
async def test_same_time_driver_inputs_replay_in_causal_order(tmp_path, legacy):
    capture = tmp_path/'capture'
    session = await make_session(capture=capture, position_cap=5)
    await book(session, asks={.5:100})
    result = await session.submit(signal(session, price=.5))
    assert result.status == 'pending'
    await session.advance(START)
    await book(session, asks={.6:100})  # Same timestamp, after the first fill.
    original = await session.finish()
    assert len(session.engine.executions) == 1

    if legacy:
        # Version 1 stored the input order but had no driver-step boundaries.
        from hashlib import sha256
        records = [json.loads(line) for line in (capture/'inputs.jsonl').read_text().splitlines()]
        records = [row for row in records if row['kind'] != 'advance']
        for row in records:
            row['schema_version'] = 1
            row.pop('origin')
            row.pop('cause')
        data = ''.join(json.dumps(row)+'\n' for row in records).encode()
        (capture/'inputs.jsonl').write_bytes(data)
        manifest = json.loads((capture/'manifest.json').read_text())
        manifest.update(schema_version=1, record_count=len(records), dataset_sha256=sha256(data).hexdigest())
        (capture/'manifest.json').write_text(json.dumps(manifest))

    restored, summary = await replay(capture)
    assert restored.engine.executions == session.engine.executions
    assert summary['accounts'] == original['accounts']


@pytest.mark.asyncio
async def test_same_time_batched_books_keep_strategy_decisions_inside_public_callbacks(tmp_path):
    capture = tmp_path/'capture'
    session = await make_session(capture=capture, position_cap=6)

    class Strategy:
        def __init__(self, context):
            self.context = context
            self.sent = False

        async def on_snapshot(self, snapshot):
            if self.sent:
                return []
            self.sent = True
            return [self.context.new_intent(market_id='m', exchange='A', side=Side.BUY,
                outcome='YES', price=.6, strategy='one', estimated_probability=1,
                resolution_date=START+3600)]

    session.strategy = Strategy(session)
    for ask in (.5, .6):
        event = OrderBookSnapshotEvent('m', 'YES', START, 'A', {.49:100}, {ask:100})
        session.publish(envelope(session, event))
    await session.advance(START)
    original = await session.finish()
    assert session.engine.executions[0].price == '0.6'
    assert session.engine.executions[0].decision_quote == .5

    for factory in (None, Strategy):
        restored, summary = await replay(capture, strategy_factory=factory)
        assert restored.engine.executions == session.engine.executions
        assert restored.engine.audit == session.engine.audit
        assert summary['accounts'] == original['accounts']


@pytest.mark.asyncio
async def test_replay_does_not_pump_unprocessed_arrivals_at_capture_end(tmp_path):
    capture = tmp_path/'capture'
    session = await make_session(capture=capture, position_cap=5)
    await book(session, asks={.5:100})
    await session.submit(signal(session, price=.5))
    original = await session.finish()  # No advance after submission, even at this timestamp.
    restored, summary = await replay(capture)
    assert not restored.engine.executions
    assert summary['accounts'] == original['accounts']
    assert summary['accounts']['PAPER']['pending_reservations'] == 1
