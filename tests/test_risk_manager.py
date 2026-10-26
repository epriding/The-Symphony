from copy import deepcopy
from dataclasses import replace
import math
import pytest

from src.market_decisions.risk_manager import RiskManager
from src.utilities.classes import (
    AccountBalance, OrderIntentBatch, OrderRecord, OrderStatus, OrderType,
    Position, RiskConfig, Side, TimeInForce,
)


def snapshot(state):
    return state._snapshot()


def position(state, size=400, strategy="paper", category="other", outcome="YES"):
    held = Position("m", "A", outcome, size, .5, Side.BUY, strategy, "PAPER", category)
    state.portfolios["PAPER"].positions[f"A:m:{outcome}"] = deepcopy(held)
    state.portfolios["PAPER"].strategy_positions.setdefault(strategy, {})[f"A:m:{outcome}"] = held
    state.portfolios["PAPER"].balances["A"]["USD"].cash -= size * .5
    state.portfolios["PAPER"].balances["A"]["USD"].buying_power -= size * .5
    return held


def test_kelly_and_exact_horizon(state, clock):
    risk = state.risk_managers["PAPER"]
    assert risk.kelly_criterion(.8, .5, clock() + 604800) == pytest.approx(.15)
    assert risk.kelly_criterion(.8, .5, clock() + 604799) == pytest.approx(.3)
    assert risk.kelly_criterion(.4, .5, clock() + 1) == 0
    assert risk.kelly_criterion(0, .5, clock() + 1) == 0
    assert risk.kelly_criterion(1, .5, clock() + 1) == .5


@pytest.mark.parametrize("probability,price,remaining", [(math.nan,.5,1), (1.1,.5,1), (-.1,.5,1), (True,.5,1), (.8,0,1), (.8,1,1), (.8,math.inf,1), (.8,.5,0)])
def test_invalid_kelly(state, clock, probability, price, remaining):
    with pytest.raises(ValueError):
        state.risk_managers["PAPER"].kelly_criterion(probability, price, clock() + remaining)


@pytest.mark.parametrize("changes", [
    {"diversification": {"crypto": 1}},
    {"diversification": {"other": .5, "crypto": .6}},
    {"max_total_exposure": -1}, {"max_position_exposure": math.nan},
    {"max_open_positions": 1.5}, {"max_drawdown_pct": 101},
    {"kelly_criterion": {"long_term": 1.1, "short_term": .5}},
    {"currency": "USDC"}, {"approval_ttl_seconds": 0},
])
def test_invalid_config(changes):
    with pytest.raises(ValueError):
        RiskManager(RiskConfig(**changes))


def test_target_subtracts_existing_inventory(state, intent):
    position(state)
    decision = state.risk_managers["PAPER"].evaluate(intent, snapshot(state))
    assert decision.approved
    assert decision.orders[0].size == 200  # target $300 minus $200 held
    assert decision.sizing["equity"] == 1000


def test_category_headroom_and_pending_partial_fill(state, intent):
    risk = state.risk_managers["PAPER"]
    risk.config.diversification = {"crypto": .25, "other": .75}
    state.markets["A:m"].category = "crypto"
    position(state, category="crypto")
    decision = risk.evaluate(intent, snapshot(state))
    assert decision.orders[0].commitment == 50
    assert decision.limiting_budget == "category_exposure"
    portfolio = state.portfolios["PAPER"]
    portfolio.open_orders["A:x"] = OrderRecord("x", "m", "A", "YES", Side.BUY,
        100, .5, OrderStatus.PARTIALLY_FILLED, "second", filled_quantity=40,
        portfolio_id="PAPER", category="crypto")
    exposure = risk.get_exposure(snapshot(state))
    assert exposure.total == 230  # $200 inventory + $30 unfilled commitment
    assert risk.evaluate(intent, snapshot(state)).orders[0].commitment == 20
    portfolio.open_orders["A:x"].status = OrderStatus.CANCELED
    assert risk.get_exposure(snapshot(state)).total == 200


@pytest.mark.parametrize("changes,reason", [
    ({"created_at": 0}, "stale_signal"), ({"expires_at": 0}, "expired_signal"),
    ({"estimated_probability": math.nan}, "invalid_probability"),
    ({"price": .501}, "invalid_price_tick"),
    ({"price": .49}, "price_bound_violated"),
    ({"tif": TimeInForce.GTD}, "invalid_order_expiry"),
    ({"order_type": OrderType.MARKET}, "unbounded_market_order"),
    ({"reduce_only": True}, "reduce_only_buy_unsupported"),
])
def test_entry_rejections(state, intent, changes, reason):
    assert state.risk_managers["PAPER"].evaluate(replace(intent, **changes), snapshot(state)).reason == reason


def test_stale_market_rules_and_quotes(state, intent, clock):
    state.markets["A:m"].fee_per_contract = None
    assert state.risk_managers["PAPER"].evaluate(intent, snapshot(state)).reason == "unknown_market_rules"
    state.markets["A:m"].fee_per_contract = 0
    clock.now += 6
    assert state.risk_managers["PAPER"].evaluate(intent, snapshot(state)).reason == "stale_quote"


def test_bounded_market_order_uses_worst_price_and_fee(state, intent):
    state.markets["A:m"].fee_per_contract = .01
    risk = state.risk_managers["PAPER"]
    risk.config.max_position_exposure = 50
    decision = risk.evaluate(replace(intent, order_type=OrderType.MARKET, slippage=2), snapshot(state))
    assert decision.orders[0].price == .52
    assert decision.orders[0].commitment <= 50
    assert decision.orders[0].size == 94


def test_exit_ignores_entry_limits_but_reserves_owned_inventory(state, intent):
    position(state)
    risk = state.risk_managers["PAPER"]
    risk.config.max_total_exposure = 0
    state.portfolios["PAPER"].entries_halted = True
    sell = replace(intent, side=Side.SELL, reduce_only=True, estimated_probability=None, resolution_date=None)
    decision = risk.evaluate(sell, snapshot(state))
    assert decision.approved and decision.orders[0].size == 400
    state._reserve(decision)
    retry = replace(sell, intent_id="different")
    assert not risk.evaluate(retry, snapshot(state)).approved
    assert risk.evaluate(replace(retry, strategy="second"), snapshot(state)).reason == "no_owned_inventory"
    assert risk.evaluate(replace(retry, reduce_only=False), snapshot(state)).reason == "exit_must_reduce_inventory"


def test_market_slot_counts_orders_and_reservations(state, intent):
    risk = state.risk_managers["PAPER"]
    risk.config.max_open_positions = 1
    state._reserve(risk.evaluate(intent, snapshot(state)))
    state.markets["A:n"] = replace(state.markets["A:m"], market_id="n")
    state.orderbooks["A:n:YES"] = replace(state.orderbooks["A:m:YES"], market_id="n")
    assert risk.evaluate(replace(intent, market_id="n", intent_id="new"), snapshot(state)).reason == "position_count_limit"
    assert risk.evaluate(replace(intent, strategy="second", intent_id="second"), snapshot(state)).approved


def test_no_category_transfer_from_existing_holdings(state, intent):
    position(state, category="crypto")
    state.markets["A:m"].category = "politics"
    risk = state.risk_managers["PAPER"]
    assert risk.get_exposure(snapshot(state)).by_category == {"crypto": 200}


def test_live_requires_cash_and_fresh_complete_account(state, intent, clock):
    risk = state.risk_managers["LIVE"]
    portfolio = state.portfolios["LIVE"]
    live_intent = replace(intent, strategy="live")
    portfolio.balances["A"] = {"USD": AccountBalance(1000, timestamp=clock())}
    portfolio.positions_ready.add("A")
    portfolio.orders_ready.add("A")
    assert risk.evaluate(live_intent, snapshot(state)).reason == "account_or_valuation_unavailable"
    portfolio.balances["A"]["USD"].cash = 1000
    assert risk.evaluate(live_intent, snapshot(state)).approved
    portfolio.balances["A"]["USD"].timestamp -= 31
    assert not risk.evaluate(live_intent, snapshot(state)).approved


def test_batch_shared_budget_and_ratios(state, intent):
    risk = state.risk_managers["PAPER"]
    risk.config.max_total_exposure = 100
    batch = OrderIntentBatch(legs=[replace(intent, estimated_probability=None), replace(intent, intent_id="second", estimated_probability=None, outcome="NO")],
                            quantity_ratios=[1, 2], capital_ceiling=150)
    decision = risk.evaluate_batch(batch, snapshot(state))
    assert decision.approved
    assert [order.size for order in decision.orders] == [66, 132]
    assert sum(order.commitment for order in decision.orders) == 99
    assert all(order.batch_id == batch.batch_id for order in decision.orders)


def test_batch_increments_preserve_ratios(state, intent):
    state.markets["A:m"].quantity_step = .03
    state.markets["A:m"].min_quantity = .03
    decision = state.risk_managers["PAPER"].evaluate_batch(OrderIntentBatch(
        legs=[intent, replace(intent, intent_id="no", outcome="NO")],
        quantity_ratios=[2, 3], capital_ceiling=1), snapshot(state))
    assert decision.approved
    a, b = decision.orders
    assert a.size * 3 == pytest.approx(b.size * 2)
    assert sum(o.commitment for o in decision.orders) <= 1


def test_batch_rejects_unfunded_venue(state, intent):
    state.markets["B:m"] = replace(state.markets["A:m"], exchange="B")
    state.orderbooks["B:m:YES"] = replace(state.orderbooks["A:m:YES"], exchange="B")
    decision = state.risk_managers["PAPER"].evaluate_batch(OrderIntentBatch(
        legs=[intent, replace(intent, exchange="B", intent_id="b")],
        quantity_ratios=[1, 1], capital_ceiling=100), snapshot(state))
    assert not decision.approved


def test_drawdown_and_stop_loss_use_marked_equity(state, intent):
    held = position(state)
    risk = state.risk_managers["PAPER"]
    risk.config.max_drawdown_pct = 5
    state.orderbooks["A:m:YES"].bids = {.3: 1000}
    assert risk.equity(snapshot(state)) == 920
    assert risk.evaluate(intent, snapshot(state)).reason == "drawdown_limit"
    stops = risk.stop_loss_intents(snapshot(state))
    assert len(stops) == 1 and stops[0].reduce_only
    assert risk.evaluate(stops[0], snapshot(state)).approved


def test_metadata_change_does_not_reset_market_category_budget(state, intent):
    risk = state.risk_managers["PAPER"]
    risk.config.diversification = {"crypto": .25, "politics": .25, "other": .5}
    position(state, category="crypto")
    state.markets["A:m"].category = "politics"
    decision = risk.evaluate(intent, snapshot(state))
    assert decision.approved
    assert decision.orders[0].category == "crypto"
    assert decision.orders[0].commitment == 50


def test_entry_with_edge_erased_by_fees_is_rejected(state, intent):
    state.markets["A:m"].fee_per_contract = .01
    decision = state.risk_managers["PAPER"].evaluate(replace(intent, estimated_probability=.505), snapshot(state))
    assert not decision.approved


def test_fractional_group_scale_is_exact(state, intent):
    state.markets["A:m"].quantity_step = .1
    state.markets["A:m"].min_quantity = .1
    decision = state.risk_managers["PAPER"].evaluate_batch(OrderIntentBatch(
        legs=[intent, replace(intent, outcome="NO", intent_id="no")],
        quantity_ratios=[3, 6], capital_ceiling=1.1), snapshot(state))
    assert decision.approved
    a, b = decision.orders
    assert a.size == .7
    assert b.size == 2 * a.size
    assert sum(o.commitment for o in decision.orders) <= 1.1


def test_group_quantity_uses_fee_provider_instead_of_flat_fee_bound(state, intent):
    risk = state.risk_managers["PAPER"]
    state.markets["A:m"].fee_per_contract = .1

    without_provider = risk.evaluate(intent, snapshot(state), group_budget=1.5, group_quantity=3)
    assert not without_provider.approved
    assert without_provider.reason == "batch_group_ceiling"

    risk.fee_reserve_provider = lambda intent, market, quantity, price: 0
    with_provider = risk.evaluate(intent, snapshot(state), group_budget=1.5, group_quantity=3)
    assert with_provider.approved
    assert with_provider.orders[0].size == 3
