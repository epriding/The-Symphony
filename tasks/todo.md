# Tasks: Immediate complement live paper strategy

Status: approved for unattended `/build auto`; implementation in progress. See [plan.md](plan.md).
Task completion requires its acceptance checks and verification, not just source edits. Proposed new test files below do not exist yet. Keep each task to at most five files; split before exceeding that limit.

## T01: Price a candidate pair after fees

- [x] Implement T01.
- Description: Add the pure, bounded candidate evaluator for equal-quantity complementary purchases, using actual ask depth and the existing fee model.
- Acceptance:
  - [x] Evaluate both cross-exchange orientations using decimal depth costs, valid quantity grids, price bounds, and separate available funds; reject missing/stale/skewed books and incompatible normal terms.
  - [x] Reuse `FeeModel`/reserves for both venues. Admit only surplus above the configured net threshold; correctly reject gross-positive/net-negative and fee-rounding boundary cases without relying on rebates.
  - [x] Bound quantity candidates, choose deterministically, and expose stable skip reasons; matching terms are the paper premise, not an unconditional payout certification.
- Verify: `python3 -m pytest -q tests/test_complement_quotes.py`; use production-shaped fixtures plus independently calculated decimal fee examples and resized quantities.
- Dependencies: none.
- Likely files: `src/strategies/immediate_complement.py`, `src/paper_trading/fees.py` only if a reusable public helper is needed, `tests/test_complement_quotes.py`.
- Estimated scope: medium, up to three files.

## T02: Admit linked strategy decisions

- [x] Implement T02.
- Description: Extend the supervised callback boundary to carry a linked pair to the existing batch gateway without exposing mutable account/coordinator access.
- Acceptance:
  - [x] Context-generated pair and leg identities are deterministic. Validate the entire callback result, strategy ownership, market membership, duplicate IDs, and flattened leg/byte limits before submitting any item.
  - [x] Route the pair through `submit_batch`; preserve existing individual-order callbacks, timeout/disable behavior, and paper-only ownership.
  - [x] Capture serialization round-trips linked decisions using existing allowed types; no arbitrary imports or global live-client construction.
- Verify: `python3 -m pytest -q tests/test_complement_gateway.py tests/test_bot_loop_v1.py tests/test_bot_replay_v1.py`.
- Dependencies: T01.
- Likely files: `src/strategies/context.py`, `src/paper_trading/runtime.py`, `src/paper_trading/recording.py`, `src/utilities/classes.py`, `tests/test_complement_gateway.py`.
- Estimated scope: medium, five files.

## T03: Preserve net edge during risk approval

- [x] Implement T03.
- Description: Ensure linked sizing and reservations preserve pair economics at the final approved quantity.
- Acceptance:
  - [x] Risk checks both legs against shared exposure limits and each exchange's cash; resizes on a common grid or rejects the entire pair before dispatch.
  - [x] Recalculate net surplus and fee bounds at final size and approved price caps; reject edge erased by rounding/resizing. Do not invent probability 1 for either directional leg to bypass sizing.
  - [x] Fresh dispatch checks reject expired decisions, stale quotes, or insufficient funds; rejected pairs leave no orphan reservations or partial submissions.
- Verify: `python3 -m pytest -q tests/test_complement_risk.py tests/test_risk_manager.py tests/test_order_lifecycle.py`.
- Dependencies: T01, T02.
- Likely files: `src/market_decisions/risk_manager.py`, `src/market_decisions/order_coordinator.py`, `src/utilities/classes.py`, `tests/test_complement_risk.py`.
- Estimated scope: medium, four files.

## Checkpoint A: Candidate to approved pair

- [x] Gross/net fee examples and stale-data rejection pass through the actual gateway.
- [x] Full tests and source compilation pass; existing single-order strategies remain compatible.
- [x] Review the candidate/approval trace before enabling any public strategy.

## T04: Recover independently failed legs

- [ ] Implement T04.
- Description: Connect linked entry execution to observed-fill reconciliation and bounded excess recovery.
- Acceptance:
  - [ ] Reuse realistic independent-leg execution with nonzero fixture delays. Demonstrate both fill, neither fills, and one fills/one fails against arrival depth; batch approval never forces atomic fills.
  - [ ] On failure/deadline, cancel pending entries, reconcile cumulative fills, and issue bounded reduce-only IOC sales of confirmed excess. Late/duplicate/corrected reports and pending exits cannot oversell.
  - [ ] Progress on account updates/timers during quiet markets. Missing/stale bids or uncertain order status retain commitments/residual inventory and block new entries; shutdown attempts bounded cleanup without fabricated fills.
- Verify: `python3 -m pytest -q tests/test_complement_recovery.py tests/test_bot_recovery_v1.py tests/test_bot_shutdown_v1.py`.
- Dependencies: T02, T03.
- Likely files: `src/strategies/immediate_complement.py`, `src/paper_trading/runtime.py`, `src/paper_trading/session.py`, `src/market_decisions/order_coordinator.py`, `tests/test_complement_recovery.py`.
- Estimated scope: medium, five files. If engine behavior needs correction, split that fix into a focused follow-up before exceeding the budget.

## T05: Enforce daily loss centrally

- [ ] Implement T05.
- Description: Add the daily portfolio loss stop to the risk manager and supervised risk ticks.
- Acceptance:
  - [ ] Compute daily loss from UTC start equity using conservative executable liquidation values and fees across both venues, without double-counting reserves or debited fees. Default to 10%; validate configured 10–15%.
  - [ ] At the exact threshold, latch new-entry prohibition for the UTC day and request entry cancellations. Permit valid reduce-only exits; report unavoidable overshoot and reject invalid/stale valuation instead of granting more risk.
  - [ ] Market rollover never resets the daily baseline/latch. UTC midnight carries real holdings and unresolved obligations; high-water drawdown remains a distinct existing control.
- Verify: `python3 -m pytest -q tests/test_complement_daily_risk.py tests/test_risk_manager.py` with threshold equality, fees, unrealized loss, blocked-entry reductions, and midnight cases.
- Dependencies: T03, T04.
- Likely files: `src/market_decisions/risk_manager.py`, `src/utilities/classes.py`, `src/paper_trading/runtime.py`, `tests/test_complement_daily_risk.py`.
- Estimated scope: medium, four files.

## T06: Preserve matched holdings to actual settlement

- [ ] Implement T06.
- Description: Make matched quantities visible to centralized exit checks and maintain authoritative per-venue payout accounting.
- Acceptance:
  - [ ] Exempt only confirmed matched quantities from single-leg stop-loss sales; reject fake group exemptions and keep unmatched excess eligible for recovery.
  - [ ] Credit each venue's authoritative settlement exactly once; expiry, shared sources, or an expected $1 combined payout never credit cash on their own.
  - [ ] Handle delayed or differing venue outcomes, fee corrections, and partial settlements without discarding inventory or treating held capital as spendable.
- Verify: `python3 -m pytest -q tests/test_complement_settlement.py tests/test_paper_trading.py tests/test_order_lifecycle.py`.
- Dependencies: T04, T05.
- Likely files: `src/market_decisions/risk_manager.py`, `src/market_decisions/order_coordinator.py`, `src/utilities/trading_state.py`, `src/paper_trading/session.py`, `tests/test_complement_settlement.py`.
- Estimated scope: medium, five files.

## Checkpoint B: Complete one window in paper

- [ ] An offline forced opportunity travels through strategy, risk, independent execution, recovery, and actual settlement events.
- [ ] One-leg failure loses money visibly when appropriate; daily stop blocks later entries; matched inventory is preserved.
- [ ] Full tests and source compilation pass. No live order placement is reachable.

## T07: Discover matching Bitcoin windows

- [ ] Implement T07.
- Description: Add bounded official-series discovery for current and next windows through read-only public transports.
- Acceptance:
  - [ ] Discover Kalshi `KXBTC15M` and configured US Bitcoin series through paginated metadata; use canonical identities and typed UTC measurement boundaries, not guessed slugs or administrative expiry.
  - [ ] Verify asset, environment, duration, reference/calculation, outcome mapping, and opening threshold. Missing/unknown/ambiguous counterparts skip entry and continue discovery; prepared future windows are not treated as settled merely because closed.
  - [ ] Enforce response/page/result bounds, existing rate budgets, and retry backoff. Keep allowed public GET routes explicit; never expand transport access to account/order endpoints.
- Verify: `python3 -m pytest -q tests/test_complement_discovery.py tests/test_paper_market_data.py tests/test_bot_transport_limits_v1.py` with saved public payload shapes and missing-window fixtures.
- Dependencies: T01, T06.
- Likely files: `src/paper_trading/window_discovery.py`, `src/api/interfaces/bounded_public.py`, `src/api/interfaces/polymarket_us_public.py`, `src/api/interfaces/kalshi.py`, `tests/test_complement_discovery.py`.
- Estimated scope: medium, five files.

## T08: Activate rolling public subscriptions

- [ ] Implement T08.
- Description: Prepare the next window and change the active strategy market set under runtime supervision.
- Acceptance:
  - [ ] Prepare current/next subscriptions within configured limits and require verified metadata, thresholds, and full fresh depth on both venues before activation.
  - [ ] Atomically change callback market membership and readiness at rollover without admitting old-window decisions, permitting concurrent callbacks, or resetting account state. Reconnect restores actual current readiness rather than trading missed windows.
  - [ ] Block old entry at cutoff and run at least three successive synthetic windows without manual edits. Preserve legacy fixed-market configuration and observation-only mode.
- Verify: `python3 -m pytest -q tests/test_complement_rolling.py tests/test_bot_startup_lifecycle_v1.py tests/test_bot_loop_v1.py`.
- Dependencies: T02, T07.
- Likely files: `src/paper_trading/runtime.py`, `src/paper_trading/runtime_config.py`, `src/api/interfaces/polymarket_us_public.py`, `src/api/interfaces/kalshi.py`, `tests/test_complement_rolling.py`.
- Estimated scope: medium, five files.

## T09: Retain old obligations within bounds

- [ ] Implement T09.
- Description: Keep older-window execution and settlement alive while safely retiring completed subscriptions/state.
- Acceptance:
  - [ ] Late old-window fills, cancellation races, settlements, cash, reserves, and daily state remain correctly attributed when the next window is active. Unresolved excess blocks entries in every window.
  - [ ] Retain lifecycle subscriptions/polling for outstanding obligations. Archive fully settled history and retire unneeded depth without losing required evidence or duplicate protection.
  - [ ] Exercise more windows than the retention limit; working market/task/buffer state stays bounded. Capacity exhaustion pauses new entries visibly rather than deleting holdings or silently raising limits.
- Verify: `python3 -m pytest -q tests/test_complement_retention.py tests/test_bot_retention_v1.py tests/test_bot_archived_accounting_v1.py`.
- Dependencies: T06, T08.
- Likely files: `src/paper_trading/runtime.py`, `src/paper_trading/paper_trading_engine.py`, `src/paper_trading/history.py`, `src/utilities/trading_state.py`, `tests/test_complement_retention.py`.
- Estimated scope: medium, five files.

## Checkpoint C: Continuous rolling experiment

- [ ] Three-window run includes one skipped window, one delayed settlement, and a daily-stop boundary without resets or lost obligations.
- [ ] Retention/capacity tests and full regression suite pass.
- [ ] Review rolling traces and retained inventory before launch wiring.

## T10: Preserve capture and recorded replay

- [ ] Implement T10.
- Description: Extend the existing capture/replay lifecycle for pairs and rolling discovery; do not build historical backtesting.
- Acceptance:
  - [ ] Capture candidate decisions/reasons, pair identities, discovery/readiness/activation/retirement events, timers, daily stops, and observed execution/settlement inputs in causal order.
  - [ ] Existing recorded-decision replay reproduces balances, fees, fills, recovery, and rollover without network reads or inventing post-capture liquidity. Older supported captures retain their documented behavior.
  - [ ] Keep explicit schema/version handling, bounds, provenance, and incomplete-capture rejection. Do not capture credential values or private headers.
- Verify: `python3 -m pytest -q tests/test_complement_replay.py tests/test_bot_replay_v1.py tests/test_bot_replay_admission_v1.py`.
- Dependencies: T04, T05, T09.
- Likely files: `src/paper_trading/recording.py`, `src/paper_trading/replay.py`, `src/paper_trading/runtime.py`, `tests/test_complement_replay.py`.
- Estimated scope: medium, four files.

## T11: Report complete experiment results

- [ ] Implement T11.
- Description: Add strategy-specific results to existing bounded summaries/detail streams.
- Acceptance:
  - [ ] Show projected/realized net returns, entry/recovery fees, paired/failed/unfilled attempts, unhedged duration, per-venue cash/reserves, unresolved inventory, and daily-stop/overshoot events.
  - [ ] Attribute results to window/group IDs and record discovered/activated/skipped windows. Reconcile detail streams to full-session portfolio totals, including delayed settlements and corrections.
  - [ ] Keep aggregate summaries bounded, retain existing artifact conventions, and state fee/latency/settlement assumptions. No-trade runs explain their skip reasons and remain valid results.
- Verify: `python3 -m pytest -q tests/test_complement_reporting.py tests/test_bot_summary_v1.py tests/test_bot_detail_streams_v1.py`.
- Dependencies: T06, T09, T10.
- Likely files: `src/paper_trading/reporting.py`, `src/paper_trading/history.py`, `tests/test_complement_reporting.py`.
- Estimated scope: medium, three files.

## T12: Expose a configured paper launch

- [ ] Implement T12.
- Description: Register the strategy and provide a usable rolling paper configuration through the existing CLI.
- Acceptance:
  - [ ] `main.py run` can launch the registered strategy with rolling series, $25 starting cash per venue, configurable sizing/profit/freshness/delay/recovery settings, and the daily limit; no manual per-window market IDs are needed.
  - [ ] Validate finite settings and resource budgets before transport startup. Example fee/delay values are explicit assumptions; reject missing credentials or bad configuration with clear errors through existing setup rather than adding private-endpoint test gates.
  - [ ] Launch wiring is paper-only, starts no private order client, and leaves smoke/replay/fixed-market observation behavior intact. Defaults are reviewable and do not claim calibrated performance.
- Verify: `python3 -m pytest -q tests/test_complement_cli.py tests/test_bot_cli_v1.py` plus a synthetic configured launch using fake transports.
- Dependencies: T03, T05, T08, T11.
- Likely files: `main.py`, `src/paper_trading/runtime_config.py`, `examples/complement_config.example.json`, `src/strategies/immediate_complement.py`, `tests/test_complement_cli.py`.
- Estimated scope: medium, five files.

## Checkpoint D: Launchable paper strategy

- [ ] Configured synthetic launch produces the normal capture, execution, and summary artifacts with correct risk behavior.
- [ ] Full tests, source compilation, and existing smoke/replay commands pass.
- [ ] Review example defaults and confirm that no new private-API recertification or backtesting dependency was introduced.

## T13: Exercise the full failure matrix

- [ ] Implement T13.
- Description: Run the real strategy/runtime against deterministic synthetic markets to verify required behavior before public paper use.
- Acceptance:
  - [ ] Cover profitable pair, gross-positive/net-negative pair, stale/skewed data, insufficient depth/funds, both/zero/one-leg fill, cancel race, and missing recovery liquidity through the real gateway and ledger.
  - [ ] Cover quiet-market recovery, duplicate/corrected reports, actual delayed/differing payouts, daily limit crossing, UTC midnight, three rolling windows, and retained-inventory capacity exhaustion.
  - [ ] Assert accounting invariants and replay equality using independent expected totals; never require live networking, profitable public markets, or a forced minimum number of trades in CI.
- Verify: `python3 -m pytest -q tests/test_complement_integration.py` followed by full tests, compilation, and offline smoke/replay in fresh directories.
- Dependencies: T12.
- Likely files: `tests/test_complement_integration.py`, `tests/complement_support.py`, `examples/complement_smoke.py` if needed for a reviewable command.
- Estimated scope: medium, up to three files.

## T14: Document and exercise live paper operation

- [ ] Complete T14.
- Description: Provide the exact launch workflow and a bounded run against public markets using existing credentials and no real orders.
- Acceptance:
  - [ ] Document configuration copying, existing credential variable names, run/stop commands, automatic rolling, fee assumptions, daily stop, and interpretation of unfinished holdings in `docs/COMPLEMENT_PAPER.md`.
  - [ ] With the existing environment available, run the paper strategy across at least two window transitions and inspect artifacts. Zero entries is acceptable; missing credentials/readiness is reported as an operational limitation rather than success or another research gate.
  - [ ] Record command, duration, observed windows, runtime outcome, and any limitations without secrets. Confirm independently that the launch has no real-order route; do not test private order placement.
- Verify: `python3 main.py run --config local-complement.json --output /tmp/complement-live-paper --duration 3600`; replay the completed capture into a different fresh directory. Adjust duration/start time to include two actual transitions; no profitable trade requirement.
- Dependencies: T13.
- Likely files: `docs/COMPLEMENT_PAPER.md`, `docs/BOT_RUNTIME.md`, one local/ignored run report if appropriate. Keep captures outside source control.
- Estimated scope: small, two documentation files plus local run evidence.

## Checkpoint E: Ready for the user's paper experiment

- [ ] Required software checks pass and the documented command is usable.
- [ ] Live-run evidence or the exact missing operational input is stated honestly; no real orders or claimed guaranteed profits.
- [ ] Review the completed implementation and results with the user before expanding to delayed-entry strategies.
