# Risk manager usage

The strategy proposes an unsized `OrderIntent`. `OrderCoordinator` selects its configured portfolio, evaluates risk, reserves resources, and dispatches an `ApprovedOrder`. The live adapter or paper simulator returns an `OrderRecord`. All order responses, stream events, and reconciliation use one cumulative-fill ledger.

Run the offline demonstration and tests from the repository root:

```bash
venv/bin/python -m examples.risk_manager_demo
venv/bin/python -m pytest -q
```

## Reading the implementation

- `RiskManager.evaluate()` runs shared validation, then chooses `_evaluate_entry()` or `_evaluate_exit()`. `_approve()` handles final quantity rounding and approval expiry.
- `RiskManager.evaluate_batch()` checks leg limits, calculates a common group size, and rechecks each leg against projected commitments.
- `OrderCoordinator` owns submission and network calls. `_apply_paper_batch()` owns group rollback; `_reconcile_exchange()` refreshes one live account.
- `TradingState._apply_order_update()` preserves attribution, determines the new fill, applies cash/inventory changes, and records the result. `_FillAmounts` names the quantities and costs passed between those steps.
- `PortfolioState.pending_orders` iterates open orders and local reservations. Each exposes `remaining_quantity` and `commitment`, so callers use the same definitions for reserved shares and USD.

## Configuration and migration

```python
from src.utilities.classes import RiskConfig, TradingConfig
from src.utilities.trading_state import TradingState
from src.market_decisions.order_coordinator import OrderCoordinator

config = TradingConfig()
config.strategy_portfolios = {"model_a": "PAPER", "model_b": "LIVE"}
config.paper_starting_balances = {"PAPER": {"POLYMARKET_US": 1000.0}}
config.risk_configs["PAPER"] = RiskConfig(
    max_total_exposure=700.0,
    max_position_exposure=100.0,
    diversification={"crypto": .2, "politics": .3, "sports": .2, "other": .3},
    max_drawdown_pct=10.0,
)
state = TradingState(config)
# Supply live clients by exchange; paper uses its own simulator.
coordinator = OrderCoordinator(state, clients={})
```

- Risk settings formerly on `TradingConfig` now belong to `config.risk_configs[portfolio_id]`. `max_position_size` becomes `max_position_exposure`, measured in committed USD per exchange/market.
- `confidence` becomes `estimated_probability`: the probability of the specified YES or NO outcome paying $1. Exit intents omit probability and resolution.
- Construct `TradingState(config)` and `OrderCoordinator(state, clients=...)`. `RiskManager` accepts a `RiskConfig`, portfolio identifier, and optional clock.
- Strategies receive a typed `Snapshot`. Read account data from `snapshot.portfolios[portfolio_id]`. Snapshots are independent copies.
- Legacy `config.paper_trading = {strategy: bool}` remains a routing input; explicit `strategy_portfolios` entries take precedence. Routes and risk settings are copied at state construction. Unknown strategies never default to live.
- Extra risk configuration entries create independent paper portfolios. Set a strategy route and starting balances for each. Only `LIVE` routes to real clients.

Market metadata must include `tick_size`, `quantity_step`, `min_quantity`, and a conservative USD `fee_per_contract`. An absent value blocks trading rather than guessing venue rules. `min_notional` defaults to zero. Add overrides by `exchange:market_id` before inserting markets:

```python
config.market_overrides["POLYMARKET_US:market-id"] = {
    "category": "sports",
    "resolution_date": 1900000000,
    "tick_size": .01,
    "quantity_step": 1,
    "min_quantity": 1,
    "fee_per_contract": .01,
}
```

These are illustrative values, not venue fee/precision recommendations. Set actual rules for the market. `state.update_trading_state()` applies overrides; public book/quote updates are shared across portfolios. Unknown metadata categories use `other`. Existing market commitments keep their original category; later entries in that market use the same category until it is fully closed.

## Entries and reductions

```python
from src.utilities.classes import OrderIntent, Side

signal = OrderIntent(
    market_id="market-id", exchange="POLYMARKET_US", outcome="NO",
    side=Side.BUY, strategy="model_a", price=.40,
    estimated_probability=.55, resolution_date=1900000000,
)
result = await coordinator.submit(signal)
print(result.status, result.decision.reason)
```

The short horizon uses half Kelly; at or above one week it uses quarter Kelly. These configurable defaults preserve the existing model; they are not calibrated strategy parameters. The dollar target is the fraction times marked portfolio equity, optionally capped by `capital_allocation`. Filled and pending commitment for the same strategy/market/outcome reduces the remaining target. Entry Kelly uses the bounded purchase price plus the configured fee bound, so an edge erased by fees cannot produce an entry. Every strategy still shares its portfolio's cash, category, market, total exposure, and position-count limits.

Percentages in `diversification` are fractions between 0 and 1; `stop_loss_pct` and `max_drawdown_pct` use 0–100. Category caps may sum to less than 1, leaving unallocated capital. Fees are included in pending budgets. Positions contribute held cost, not marked value, to exposure; marked bids determine equity. Account positions count once; strategy ownership is used only for attribution and Kelly targets.

`created_at` defaults to the current Unix time. Default limits are a 30-second signal lifetime, 5-second quote age, 30-second live account age, and 2-second approval validity. Set `expires_at` for a shorter signal lifetime. `resolution_date` is distinct from `good_till_date`, which is required for GTD. Market order `slippage` is a nonnegative number of ticks around `price`; sizing reserves the worst permitted price. Tick/quantity rounding never increases approved size.

```python
exit_signal = OrderIntent(
    market_id="market-id", exchange="POLYMARKET_US", outcome="NO",
    side=Side.SELL, strategy="model_a", price=.35,
    reduce_only=True, close_fraction=.5,
)
result = await coordinator.submit(exit_signal)
```

Exits bypass entry allocation, Kelly, and drawdown limits, while retaining freshness, market, price, precision, and inventory checks. They require confirmed account inventory AND strategy-owned inventory. Pending exits reserve both. SELL without `reduce_only` is rejected. This version does not open naked shorts. Paper holds YES and NO separately; live opposing entries in an already-held or committed market are rejected to avoid inventing cash/netting rules across strategies.

`submit()` returns a `SubmissionResult`, including its `RiskDecision`, records, and status (`rejected`, `submitted`, `exchange_rejected`, `failed`, or `unknown`). Risk approval is not a promise of exchange acceptance. Reuse the same `intent_id` when retrying the same opportunity. Approved identifiers remain consumed even after cancellation or local failure; issue a new intent only for a deliberate new evaluation.

## Batches

`submit_many(list_of_intents)` processes independent opportunities in list order and returns one result per intent. Each approval sees earlier commitments.

For dependent entries, use `OrderIntentBatch(legs=[...], quantity_ratios=[1, 2], capital_ceiling=100.0, ...)`. `evaluate_batch(batch, snapshot)` sizes the group with common ratios under all venue/category/market/total budgets, without standalone Kelly or assumed hedge offsets. All strategies must route to one portfolio when submitted. Common quantity increments preserve ratios; an infeasible minimum rejects the group. Batch IDs and leg IDs are idempotent.

Live dependent submission currently returns `unsupported_batch_execution_policy`. Paper supports `execution_policy="paper_all_or_none"`: prepare and validate all fills, then apply the complete group as a ledger transaction. No part remains applied if accounting fails. This policy is a simulator property; it does not imply atomic exchange execution. Supporting live dependent execution requires a separate policy for rejected/timed-out legs and already-filled exposure.

## Live account normalization and reconciliation

The checked local SDK is `polymarket-us==0.1.2`, now declared in requirements. Its account response is a list of currency balances, and order creation returns an execution list or order ID. The adapter preserves `currentBalance`, `buyingPower`, and `balanceReservation` separately, translates TIF values, serializes `marketSlug`/Amount prices/GTD, and normalizes cumulative fills, average prices, and reported fees. Position pagination loads all pages. Account freshness uses the time of observation; a separate source timestamp rejects older balance updates without treating an unchanged cash balance as stale merely because its last transaction was old.

References: [official SDK](https://github.com/Polymarket/polymarket-us-python), [account response](https://docs.polymarket.us/api-reference/account/get-account-balances), [order request](https://docs.polymarket.us/api-reference/orders/create-order). Current online documentation can describe newer SDK versions; the installed SDK contract is the implementation reference.

The published fields do not establish a universal equity formula for every account's reservation/collateral treatment. `create_poly_us_clients(..., cash_resolver=callable)` accepts an account-specific mapping shared by REST and websocket. The callback receives the raw balance entry and must return owned USD cash **including order-held cash and excluding position value**. Confirm that mapping for the account before supplying it. With no resolver, `cash=None` and live entry sizing returns `account_or_valuation_unavailable`; buying power is never substituted for equity. Another adapter may return normalized `AccountBalance` directly under the same contract.

Live buying power already excludes exchange holds, so those holds are not subtracted again. Local submissions still awaiting acknowledgment reserve additional cash. After live order changes (including unfilled external orders), fills, or private cash/position updates, call `await coordinator.reconcile(exchange)` before further live submissions. It refreshes fill counters, complete positions, and balances together, preserving local attribution and other venues/paper accounts. Reconciliation failures leave that venue dirty and new submissions blocked. If private account updates arrive during the REST reads, reconciliation rejects that inconsistent view and must be retried. Equity high-water marks only advance from a coherent view, so mismatched cash/position updates cannot create a false drawdown. A private websocket disconnect blocks new entries; reconnect automatically attempts reconciliation. If that refresh fails, a later successful reconciliation restores trading once the transport is connected. Reconciliation alone does not clear an actual transport disconnect. Periodic reconciliation belongs in the production runner, which remains outside this change.

External account positions/orders count toward limits, even without strategy ownership. They do not acquire a strategy merely by sharing its market. Missing local orders are individually queried; absence from the open-orders endpoint does not release their commitment. A canceled order without a final fill count remains unknown until reconciled. Drift in strategy ownership versus actual inventory blocks entries.

If a create request times out or returns an ambiguous result, `status="unknown"` retains its reservation and prevents duplicate submission. Reconciliation alone cannot prove that an unidentified order never existed. After auditing venue history, recover with:

```python
await coordinator.resolve_unknown(intent_id, order=confirmed_order)
# Or, only after confirming no order/fill exists in venue history:
await coordinator.resolve_unknown(intent_id, confirmed_absent=True)
```

Cancellation releases commitments only after a terminal exchange order state is observed. HTTP cancellation acknowledgment alone does not suffice. Terminal order records are kept in session history, outside `open_orders`. Deduplication uses cumulative fills or an execution ID when cumulative counts are absent. Reported `avgPx`/`lastPx` determines fill cost when present; otherwise the reported order price is the fallback. Realized P&L includes reported fees; unknown live fees are not invented from fee reserves.

A late cumulative average at an unchanged filled quantity corrects the recorded order cost without adding shares. Execution deltas are retained so weighted cost and realized P&L can be recomputed after partial or complete exits. Paper cash is adjusted once; live cash and account inventory still come from reconciliation.

## Performance and remaining scope

Risk evaluation performs no network I/O. Mutable state belongs to `TradingState`; the coordinator serializes submissions/account events and reserves under the state lock, releasing that lock for network calls. Always submit through the coordinator, not directly through an exchange adapter.

`risk.stop_loss_intents(await state.snapshot())` produces verified reduction requests using fresh outcome bids. Submit these through the coordinator. With `max_drawdown_pct` configured, new entries halt while current marked equity breaches the high-water limit. Reductions remain available. `state.cash_flow(portfolio_id, exchange, amount)` declares deposits/withdrawals and adjusts the baseline; paper also changes actual cash. For live flows, reconcile the actual balance before evaluating risk again.

The offline harness and tests cover sizing, account separation, independent/group submission, cash/P&L, stop losses, drawdown, partial fills, replay, cancellation, and recovery. The paper simulator assumes complete immediate FOK/IOC fills at the supplied executable quote and uses the configured fee bound as its fee. It does not model depth or resting orders. A production runner must schedule strategy evaluation, reconciliation, and stop-loss checks. There is no durable order journal or strategy attribution recovery after process restart; audit live history before resuming a session that ended with unresolved submissions. Paper sessions restart from configured cash. New exchange adapters, live dependent execution, correlated portfolio Kelly, and a multi-instance launcher remain outside this scope.
