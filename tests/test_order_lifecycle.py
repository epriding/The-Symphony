import asyncio
from copy import deepcopy
from dataclasses import replace
import pytest

from conftest import FakeClient, ImmediateFillPaperEngine
from src.market_decisions.order_coordinator import OrderCoordinator
from src.utilities.classes import (
    AccountBalance, BalanceSnapshotEvent, BalanceUpdateEvent, AccountConnectionEvent,
    OrderIntentBatch, OrderRecord, OrderStatus, OrderUpdateEvent, Position, Side,
)
from src.utilities.ingest import WebsocketEventIngestor


@pytest.mark.asyncio
async def test_paper_buy_duplicate_target_and_full_exit(state, intent):
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    result = await coordinator.submit(intent)
    assert result.status == "submitted"
    portfolio = state.portfolios["PAPER"]
    assert portfolio.balances["A"]["USD"].cash == pytest.approx(700)
    assert portfolio.positions["A:m:YES"].size == 600
    assert not portfolio.open_orders and not portfolio.reservations
    assert (await coordinator.submit(intent)).decision.reason == "duplicate_intent"
    assert not (await coordinator.submit(replace(intent, intent_id="new"))).decision.approved
    snapshot = await state.snapshot()
    assert state.risk_managers["PAPER"].equity(snapshot) == pytest.approx(1000)
    sell = replace(intent, intent_id="close", side=Side.SELL, reduce_only=True,
                   estimated_probability=None, resolution_date=None)
    state.orderbooks["A:m:YES"].bids = {.6: 1000}
    result = await coordinator.submit(sell)
    assert result.status == "submitted"
    assert not portfolio.positions
    assert portfolio.balances["A"]["USD"].cash == pytest.approx(1060)
    assert portfolio.realized_pnl == pytest.approx(60)


@pytest.mark.asyncio
async def test_fee_reserves_and_realized_pnl(state, intent):
    state.markets["A:m"].fee_per_contract = .01
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    result = await coordinator.submit(intent)
    order = result.records[0]
    portfolio = state.portfolios["PAPER"]
    assert portfolio.balances["A"]["USD"].cash == pytest.approx(1000 - order.size * .51)
    assert portfolio.realized_pnl == pytest.approx(-order.size * .01)
    assert state.risk_managers["PAPER"].equity(await state.snapshot()) == pytest.approx(1000 - order.size * .01)


@pytest.mark.asyncio
async def test_concurrent_strategies_cannot_overspend(state, intent):
    state.risk_managers["PAPER"].config.max_total_exposure = 100
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    results = await asyncio.gather(coordinator.submit(intent), coordinator.submit(replace(intent, strategy="second", intent_id="second")))
    assert sum(r.status == "submitted" for r in results) == 1
    portfolio = state.portfolios["PAPER"]
    assert portfolio.balances["A"]["USD"].cash == 900
    assert state.risk_managers["PAPER"].get_exposure(await state.snapshot()).total == 100


@pytest.mark.asyncio
async def test_unknown_strategy_never_defaults_to_live(state, intent):
    result = await OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine()).submit(replace(intent, strategy="typo"))
    assert result.decision.reason == "unknown_strategy_route"


@pytest.mark.asyncio
async def test_snapshot_is_an_isolated_copy(state):
    snap = await state.snapshot()
    state.portfolios["PAPER"].balances["A"]["USD"].cash = 3
    state.markets["A:m"].category = "crypto"
    assert snap.portfolios["PAPER"].balances["A"]["USD"].cash == 1000
    assert snap.markets["A:m"].category == "other"


@pytest.mark.asyncio
async def test_balance_events_isolate_portfolios_exchanges_and_age(state, clock):
    ingestor = WebsocketEventIngestor(object(), state, OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine()))
    for exchange, value in [("A", 500), ("B", 800)]:
        await ingestor.handle_event(BalanceSnapshotEvent(exchange=exchange, timestamp=clock(), balances={"USD": AccountBalance(value, value, clock())}))
    assert state.portfolios["PAPER"].balances["A"]["USD"].cash == 1000
    assert state.portfolios["LIVE"].balances["A"]["USD"].cash == 500
    assert state.portfolios["LIVE"].balances["B"]["USD"].cash == 800
    await ingestor.handle_event(BalanceUpdateEvent(exchange="A", timestamp=clock()-1, currency="USD", balance=AccountBalance(1, 1, clock()-1)))
    assert state.portfolios["LIVE"].balances["A"]["USD"].cash == 500


@pytest.mark.asyncio
async def test_partial_fill_response_and_stream_are_applied_once(state, intent, clock):
    client = FakeClient(clock, status=OrderStatus.PARTIALLY_FILLED, filled=40)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    result = await coordinator.submit(replace(intent, strategy="live"))
    record = result.records[0]
    portfolio = state.portfolios["LIVE"]
    assert portfolio.strategy_positions["live"]["A:m:YES"].size == 40
    assert portfolio.open_orders["A:1"].remaining_quantity == 560
    await coordinator.apply_order_update(replace(record, strategy="", filled_quantity=40))
    assert portfolio.strategy_positions["live"]["A:m:YES"].size == 40
    await coordinator.apply_order_update(replace(record, strategy="", filled_quantity=100), fill_price=.5)
    assert portfolio.strategy_positions["live"]["A:m:YES"].size == 100
    client.orders["1"] = replace(client.orders["1"], filled_quantity=100)
    client.positions = [Position("m", "A", "YES", 100, .5, Side.BUY, "")]
    client.balances = {"USD": AccountBalance(700, 950, clock())}
    await coordinator.reconcile()
    exposure = state.risk_managers["LIVE"].get_exposure(await state.snapshot())
    assert exposure.total == 300  # $50 held + $250 outstanding, not $350
    assert not portfolio.dirty_exchanges
    assert not state.portfolios["PAPER"].positions


@pytest.mark.asyncio
async def test_terminal_fill_cannot_be_resurrected_by_stale_ack(state, intent):
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    result = await coordinator.submit(intent)
    record = result.records[0]
    await coordinator.apply_order_update(replace(record, status=OrderStatus.NEW, filled_quantity=0))
    portfolio = state.portfolios["PAPER"]
    assert not portfolio.open_orders
    assert portfolio.positions["A:m:YES"].size == 600
    assert portfolio.balances["A"]["USD"].cash == 700


@pytest.mark.asyncio
async def test_timeout_retains_reservation_and_blocks_retry(state, intent, clock):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    client.error = TimeoutError("ambiguous")
    live = replace(intent, strategy="live")
    result = await coordinator.submit(live)
    assert result.status == "unknown"
    portfolio = state.portfolios["LIVE"]
    assert live.intent_id in portfolio.reservations
    assert live.intent_id in portfolio.unresolved_intents
    assert (await coordinator.submit(live)).decision.reason == "duplicate_intent"
    assert (await coordinator.submit(replace(live, intent_id="another"))).decision.reason == "account_requires_reconciliation"
    await coordinator.reconcile()
    assert live.intent_id in portfolio.reservations  # absence in open orders proves nothing
    await coordinator.resolve_unknown(live.intent_id, confirmed_absent=True)
    assert not portfolio.reservations and not portfolio.unresolved_intents


@pytest.mark.asyncio
async def test_unknown_recovery_after_unattributed_stream_fill(state, intent, clock):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    client.error = TimeoutError()
    live = replace(intent, strategy="live")
    await coordinator.submit(live)
    raw = OrderRecord("recovered", "m", "A", "YES", Side.BUY, 600, .5,
                      OrderStatus.FILLED, filled_quantity=600)
    await coordinator.apply_order_update(raw)
    await coordinator.resolve_unknown(live.intent_id, order=raw)
    portfolio = state.portfolios["LIVE"]
    assert portfolio.strategy_positions["live"]["A:m:YES"].size == 600
    assert not portfolio.reservations


@pytest.mark.asyncio
async def test_cancel_releases_only_after_terminal_confirmation(state, intent, clock):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    await coordinator.submit(replace(intent, strategy="live"))
    client.cancel_status = OrderStatus.NEW
    assert not await coordinator.cancel("1", "A")
    assert "A:1" in state.portfolios["LIVE"].open_orders
    client.cancel_status = OrderStatus.CANCELED
    assert await coordinator.cancel("1", "A")
    assert not state.portfolios["LIVE"].open_orders


@pytest.mark.asyncio
async def test_missing_order_during_reconcile_keeps_commitment(state, intent, clock):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    await coordinator.submit(replace(intent, strategy="live"))
    client.orders.clear()
    await coordinator.reconcile()
    assert "A:1" in state.portfolios["LIVE"].open_orders
    assert "A" in state.portfolios["LIVE"].dirty_exchanges


@pytest.mark.asyncio
async def test_paper_batch_and_unsupported_live_group(state, intent, clock):
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    batch = OrderIntentBatch(legs=[intent, replace(intent, intent_id="no", outcome="NO")], quantity_ratios=[1, 2], capital_ceiling=99, execution_policy="paper_all_or_none")
    result = await coordinator.submit_batch(batch)
    assert result.status == "submitted"
    portfolio = state.portfolios["PAPER"]
    assert portfolio.positions["A:m:NO"].size == 2 * portfolio.positions["A:m:YES"].size
    assert portfolio.balances["A"]["USD"].cash == 901
    assert not portfolio.reservations
    live_batch = replace(batch, legs=[replace(leg, strategy="live") for leg in batch.legs])
    assert (await coordinator.submit_batch(live_batch)).decision.reason == "unsupported_batch_execution_policy"


@pytest.mark.asyncio
async def test_cross_exchange_group_cannot_transfer_cash(state, intent, clock):
    portfolio = state.portfolios["PAPER"]
    portfolio.balances["B"] = {"USD": AccountBalance(20, 20, clock(), False)}
    state.markets["B:m"] = replace(state.markets["A:m"], exchange="B")
    state.orderbooks["B:m:YES"] = replace(state.orderbooks["A:m:YES"], exchange="B")
    batch = OrderIntentBatch(legs=[intent, replace(intent, exchange="B", intent_id="b")], quantity_ratios=[1, 1], capital_ceiling=100, execution_policy="paper_all_or_none")
    result = await OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine()).submit_batch(batch)
    assert result.status == "submitted"
    assert [r.size for r in result.records] == [40, 40]
    assert portfolio.balances["A"]["USD"].cash == 980
    assert portfolio.balances["B"]["USD"].cash == 0


@pytest.mark.asyncio
async def test_withdrawal_adjusts_baseline_and_cannot_spend_reservations(state, intent):
    risk = state.risk_managers["PAPER"]
    state._reserve(risk.evaluate(intent, state._snapshot()))
    with pytest.raises(ValueError):
        await state.cash_flow("PAPER", "A", -701)
    await state.cash_flow("PAPER", "A", -500)
    assert state.portfolios["PAPER"].equity_high_water == 500
    assert risk.equity(await state.snapshot()) == 500


@pytest.mark.asyncio
async def test_stream_without_cumulative_requires_execution_id(state):
    record = OrderRecord("x", "m", "A", "YES", Side.BUY, 10, .5,
                         OrderStatus.PARTIALLY_FILLED)
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    with pytest.raises(ValueError, match="execution ID"):
        await coordinator.apply_order_update(record, fill_quantity=2)
    await coordinator.apply_order_update(record, fill_quantity=2, execution_id="first")
    await coordinator.apply_order_update(record, fill_quantity=2, execution_id="first")
    assert state.portfolios["LIVE"].order_history["A:x"].filled_quantity == 2


@pytest.mark.asyncio
async def test_reconnect_reconciles_and_clears_disconnected_state(state, intent, clock):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    ingestor = WebsocketEventIngestor(object(), state, coordinator)
    await coordinator.reconcile()
    await ingestor.handle_event(AccountConnectionEvent(exchange="A", timestamp=clock(), connected=False))
    assert (await coordinator.submit(replace(intent, strategy="live"))).decision.reason == "account_requires_reconciliation"
    await ingestor.handle_event(AccountConnectionEvent(exchange="A", timestamp=clock(), connected=True))
    assert not state.portfolios["LIVE"].disconnected_exchanges
    assert (await coordinator.submit(replace(intent, strategy="live"))).status == "submitted"


@pytest.mark.asyncio
async def test_live_venues_route_to_separate_clients(state, intent, clock):
    a, b = FakeClient(clock), FakeClient(clock, "B")
    state.markets["B:m"] = replace(state.markets["A:m"], exchange="B")
    state.orderbooks["B:m:YES"] = replace(state.orderbooks["A:m:YES"], exchange="B")
    coordinator = OrderCoordinator(state, {"A": a, "B": b}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    assert (await coordinator.submit(replace(intent, strategy="live"))).status == "submitted"
    await coordinator.reconcile("A")
    result = await coordinator.submit(replace(intent, exchange="B", strategy="live", intent_id="b"))
    assert result.status == "submitted"
    assert len(a.placed) == len(b.placed) == 1
    assert a.placed[0].exchange == "A" and b.placed[0].exchange == "B"


@pytest.mark.asyncio
async def test_canceled_order_without_fill_counter_keeps_commitment(state, intent, clock):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    await coordinator.submit(replace(intent, strategy="live"))
    unknown = replace(client.orders["1"], status=OrderStatus.CANCELED, filled_quantity=None)
    await coordinator.apply_order_update(unknown)
    portfolio = state.portfolios["LIVE"]
    assert portfolio.open_orders["A:1"].status == OrderStatus.UNKNOWN
    client.orders["1"] = unknown
    await coordinator.reconcile()
    assert "A" in portfolio.dirty_exchanges
    confirmed = replace(unknown, filled_quantity=40)
    client.orders["1"] = confirmed
    client.positions = [Position("m", "A", "YES", 40, .5, Side.BUY, "")]
    await coordinator.reconcile()
    assert "A:1" not in portfolio.open_orders
    assert portfolio.strategy_positions["live"]["A:m:YES"].size == 40


@pytest.mark.asyncio
async def test_batch_id_is_idempotent_even_with_new_leg_ids(state, intent):
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    batch = OrderIntentBatch(legs=[intent], quantity_ratios=[1], capital_ceiling=10,
                            execution_policy="paper_all_or_none")
    assert (await coordinator.submit_batch(batch)).status == "submitted"
    duplicate = replace(batch, legs=[replace(intent, intent_id="new-id")])
    assert (await coordinator.submit_batch(duplicate)).decision.reason == "duplicate_batch"


@pytest.mark.asyncio
async def test_broken_paper_group_rolls_back_all_legs(state, intent):
    class BrokenEngine(ImmediateFillPaperEngine):
        async def simulate_fill(self, order, market, quote):
            record = await super().simulate_fill(order, market, quote)
            return replace(record, average_fill_price=2) if order.outcome == "NO" else record
    coordinator = OrderCoordinator(state, paper_engine=BrokenEngine())
    batch = OrderIntentBatch(legs=[intent, replace(intent, outcome="NO", intent_id="no")],
        quantity_ratios=[1, 1], capital_ceiling=100, execution_policy="paper_all_or_none")
    assert (await coordinator.submit_batch(batch)).status == "failed"
    portfolio = state.portfolios["PAPER"]
    assert portfolio.balances["A"]["USD"].cash == 1000
    assert not portfolio.positions and not portfolio.reservations


@pytest.mark.asyncio
async def test_paper_and_live_use_independent_risk_settings(state, intent, clock):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    state.risk_managers["PAPER"].config.max_position_exposure = 20
    state.risk_managers["LIVE"].config.max_position_exposure = 80
    paper = await coordinator.submit(intent)
    live = await coordinator.submit(replace(intent, strategy="live"))
    assert paper.decision.orders[0].commitment == 20
    assert live.decision.orders[0].commitment == 80


@pytest.mark.asyncio
async def test_approval_delay_never_submits_stale_signal(state, intent, clock):
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    clock.now += 31
    result = await coordinator.submit(intent)
    assert result.decision.reason == "stale_signal"
    assert not state.portfolios["PAPER"].reservations


@pytest.mark.asyncio
async def test_submission_market_bound_remains_in_open_commitment(state, intent, clock):
    from src.utilities.classes import OrderType
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    result = await coordinator.submit(replace(intent, strategy="live", order_type=OrderType.MARKET, slippage=2))
    order = result.decision.orders[0]
    raw = replace(client.orders["1"], price=.5)
    await coordinator.apply_order_update(raw)
    portfolio = state.portfolios["LIVE"]
    assert portfolio.open_orders["A:1"].commitment_price == .52
    assert state.risk_managers["LIVE"].get_exposure(await state.snapshot()).total == pytest.approx(order.size * .52)


@pytest.mark.asyncio
async def test_partial_private_account_updates_cannot_inflate_high_water(state, clock):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    portfolio = state.portfolios["LIVE"]
    assert portfolio.equity_high_water == 1000
    held = Position("m", "A", "YES", 100, .5, Side.BUY, "")
    await state.update_live_position(held)
    await state.snapshot()
    assert portfolio.equity_high_water == 1000  # old cash + new inventory is incoherent
    client.positions = [held]
    client.balances = {"USD": AccountBalance(950, 950, clock())}
    await coordinator.reconcile()
    assert portfolio.equity_high_water == 1000
    assert state.risk_managers["LIVE"].equity(await state.snapshot()) == 1000


@pytest.mark.asyncio
async def test_reconciliation_rejects_concurrent_private_account_change(state, clock):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    original = client.get_positions
    async def racing_positions():
        await state.update_balance("USD", AccountBalance(800, 800, clock()), "A")
        return await original()
    client.get_positions = racing_positions
    with pytest.raises(RuntimeError, match="changed during reconciliation"):
        await coordinator.reconcile()
    assert "A" in state.portfolios["LIVE"].dirty_exchanges
    assert state.portfolios["LIVE"].balances["A"]["USD"].cash == 800


@pytest.mark.asyncio
async def test_old_balance_revision_cannot_complete_reconciliation(state, clock):
    client = FakeClient(clock)
    client.balances["USD"].source_timestamp = 10
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    client.balances["USD"].source_timestamp = 9
    with pytest.raises(RuntimeError, match="older balances"):
        await coordinator.reconcile()
    assert "A" in state.portfolios["LIVE"].dirty_exchanges
    assert state.portfolios["LIVE"].balances["A"]["USD"].source_timestamp == 10


@pytest.mark.asyncio
async def test_failed_reconnect_refresh_can_be_retried(state, intent, clock, monkeypatch):
    client = FakeClient(clock)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    ingestor = WebsocketEventIngestor(object(), state, coordinator)
    await coordinator.reconcile()
    await ingestor.handle_event(AccountConnectionEvent(exchange="A", timestamp=clock(), connected=False))
    original = client.get_balance

    async def unavailable_balance():
        raise TimeoutError("temporary balance failure")

    monkeypatch.setattr(client, "get_balance", unavailable_balance)
    with pytest.raises(TimeoutError):
        await ingestor.handle_event(AccountConnectionEvent(exchange="A", timestamp=clock(), connected=True))
    portfolio = state.portfolios["LIVE"]
    assert "A" not in portfolio.disconnected_exchanges
    assert "A" in portfolio.dirty_exchanges
    live = replace(intent, strategy="live")
    assert (await coordinator.submit(live)).decision.reason == "account_requires_reconciliation"

    monkeypatch.setattr(client, "get_balance", original)
    await coordinator.reconcile("A")
    assert not portfolio.requires_reconciliation
    assert (await coordinator.submit(live)).status == "submitted"


@pytest.mark.asyncio
async def test_reconciliation_does_not_clear_transport_disconnect(state, intent, clock):
    coordinator = OrderCoordinator(state, {"A": FakeClient(clock)}, paper_engine=ImmediateFillPaperEngine())
    ingestor = WebsocketEventIngestor(object(), state, coordinator)
    await ingestor.handle_event(AccountConnectionEvent(exchange="A", timestamp=clock(), connected=False))
    await coordinator.reconcile("A")
    assert "A" in state.portfolios["LIVE"].disconnected_exchanges
    assert (await coordinator.submit(replace(intent, strategy="live"))).decision.reason == "account_requires_reconciliation"


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot", [False, True])
async def test_external_unfilled_order_requires_balance_refresh(state, intent, clock, snapshot):
    a, b = FakeClient(clock), FakeClient(clock, "B")
    for client in (a, b):
        client.balances = {"USD": AccountBalance(100, 100, clock())}
    coordinator = OrderCoordinator(state, {"A": a, "B": b}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    external = OrderRecord("manual", "m", "A", "YES", Side.BUY, 180, .5,
                           OrderStatus.NEW, filled_quantity=0)
    if snapshot:
        await coordinator.apply_order_snapshot([external], "A")
    else:
        await coordinator.apply_order_update(external)
    live = replace(intent, strategy="live")
    assert "A" in state.portfolios["LIVE"].dirty_exchanges
    assert (await coordinator.submit(live)).decision.reason == "account_requires_reconciliation"
    assert not a.placed

    a.orders[external.order_id] = external
    a.balances = {"USD": AccountBalance(10, 100, clock())}
    await coordinator.reconcile("A")
    result = await coordinator.submit(live)
    assert result.status == "submitted"
    assert result.decision.orders[0].commitment == 10


@pytest.mark.asyncio
async def test_late_live_average_corrects_basis_without_adding_shares(state, intent, clock, monkeypatch):
    client = FakeClient(clock, status=OrderStatus.PARTIALLY_FILLED, filled=40)
    original = client.place_order

    async def response_without_average(order):
        return replace(await original(order), average_fill_price=None)

    monkeypatch.setattr(client, "place_order", response_without_average)
    coordinator = OrderCoordinator(state, {"A": client}, paper_engine=ImmediateFillPaperEngine())
    await coordinator.reconcile()
    result = await coordinator.submit(replace(intent, strategy="live"))
    portfolio = state.portfolios["LIVE"]
    corrected = replace(result.records[0], strategy="", average_fill_price=.4, fees_paid=.2)
    for _ in range(2):
        await coordinator.apply_order_update(corrected)
    owned = portfolio.strategy_positions["live"]["A:m:YES"]
    assert owned.size == 40
    assert owned.entry_price == pytest.approx(.4)
    assert portfolio.fill_notional_by_order["A:1"] == 16
    assert portfolio.realized_pnl == pytest.approx(-.2)
    assert portfolio.balances["A"]["USD"].cash == 1000  # REST owns live cash/inventory.
    assert not portfolio.positions

    state.orderbooks["A:m:YES"].bids = {.37: 1000}
    assert not state.risk_managers["LIVE"].stop_loss_intents(await state.snapshot())
    await coordinator.apply_order_update(replace(corrected, filled_quantity=100, average_fill_price=.46))
    assert owned.size == 100
    assert owned.entry_price == pytest.approx(.46)
    assert portfolio.fill_notional_by_order["A:1"] == pytest.approx(46)


@pytest.mark.asyncio
@pytest.mark.parametrize("close_fraction", [.5, 1])
async def test_late_buy_average_corrects_costs_after_exits(state, intent, close_fraction):
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    buy = (await coordinator.submit(intent)).records[0]
    state.orderbooks["A:m:YES"].bids = {.6: 1000}
    sell = replace(intent, intent_id="exit", side=Side.SELL, reduce_only=True,
                   close_fraction=close_fraction)
    sold = (await coordinator.submit(sell)).records[0].size
    correction = replace(buy, average_fill_price=.4)
    for _ in range(2):
        await coordinator.apply_order_update(correction)
    portfolio = state.portfolios["PAPER"]
    assert portfolio.balances["A"]["USD"].cash == pytest.approx(1000 - buy.size * .4 + sold * .6)
    assert portfolio.realized_pnl == pytest.approx(sold * .2)
    assert portfolio.filled_by_order[f"A:{buy.order_id}"] == buy.size
    if close_fraction == 1:
        assert not portfolio.positions
        assert not portfolio.strategy_positions["paper"]
    else:
        held = portfolio.positions["A:m:YES"]
        owned = portfolio.strategy_positions["paper"]["A:m:YES"]
        assert held.size == owned.size == buy.size - sold
        assert held.entry_price == owned.entry_price == pytest.approx(.4)


@pytest.mark.asyncio
async def test_late_sell_average_corrects_cash_and_realized_profit(state, intent):
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    await coordinator.submit(intent)
    state.orderbooks["A:m:YES"].bids = {.6: 1000}
    sell = replace(intent, intent_id="exit", side=Side.SELL, reduce_only=True)
    record = (await coordinator.submit(sell)).records[0]
    correction = replace(record, average_fill_price=.65, fees_paid=2)
    for _ in range(2):
        await coordinator.apply_order_update(correction)
    portfolio = state.portfolios["PAPER"]
    assert not portfolio.positions
    assert portfolio.realized_pnl == pytest.approx(record.size * .15 - 2)
    assert portfolio.balances["A"]["USD"].cash == pytest.approx(1000 + record.size * .15 - 2)


@pytest.mark.asyncio
async def test_cost_correction_preserves_other_strategy_inventory(state, intent):
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    first = (await coordinator.submit(intent)).records[0]
    second = (await coordinator.submit(replace(intent, strategy="second", intent_id="second"))).records[0]
    state.orderbooks["A:m:YES"].bids = {.6: 2000}
    exit_signal = replace(intent, intent_id="exit", side=Side.SELL, reduce_only=True,
                          close_fraction=.5)
    sold = (await coordinator.submit(exit_signal)).records[0].size
    await coordinator.apply_order_update(replace(first, average_fill_price=.4))
    portfolio = state.portfolios["PAPER"]
    assert portfolio.strategy_positions["second"]["A:m:YES"].size == second.size
    assert portfolio.strategy_positions["second"]["A:m:YES"].entry_price == .5
    held = portfolio.positions["A:m:YES"]
    assert held.size == first.size + second.size - sold
    # Account cost averages both strategies' purchases before the partial sale.
    pooled_average = (first.size * .4 + second.size * .5) / (first.size + second.size)
    assert held.entry_price == pytest.approx(pooled_average)
    assert portfolio.strategy_positions["paper"]["A:m:YES"].entry_price == pytest.approx(.4)
    assert portfolio.realized_pnl == pytest.approx(sold * .2)


@pytest.mark.asyncio
@pytest.mark.parametrize("average", [float("nan"), float("inf"), -.1, 1.1])
async def test_invalid_late_average_does_not_change_accounting(state, intent, average):
    coordinator = OrderCoordinator(state, paper_engine=ImmediateFillPaperEngine())
    record = (await coordinator.submit(intent)).records[0]
    before = deepcopy(state.portfolios["PAPER"])
    with pytest.raises(ValueError, match="average fill price"):
        await coordinator.apply_order_update(replace(record, average_fill_price=average))
    assert state.portfolios["PAPER"] == before
