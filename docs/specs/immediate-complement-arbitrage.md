# Spec: Immediate Complement Arbitrage for Paper Trading

Status: approved and implemented for paper execution; live two-transition smoke pending existing public-feed credentials. Live paper execution uses matching normal-settlement terms as an experimental premise and accounts for actual venue outcomes independently.
Date: 2026-11-13.
Branch: `strategy/cross-exchange-complement-arbitrage`.
Source: [approved strategy idea](../ideas/cross-exchange-complement-arbitrage.md).

## Objective

Give the account owner a paper strategy that attempts to buy equal quantities of complementary Bitcoin 15-minute outcomes below their assumed combined $1 payout under matching normal settlement terms, after fees and conservative execution allowances. Target Kalshi and Polymarket US, with independently funded balances of $25–$50 per exchange.

Software success means correct decisions, realistic independent fills, bounded recovery, centralized risk enforcement, and reproducible accounting. The first economic evaluation uses live public markets, including failed attempts and remaining inventory. Backtesting and historical depth reconstruction are outside v1. Working software does not establish an edge; zero trades is an acceptable result when no eligible opportunity exists.

### Scope clarification after validation

The user directed the work back to a practical live paper experiment, then explicitly requested a task plan before implementation. Matching CF Benchmarks index, measurement interval, opening threshold, outcome mapping, and normal calculation rules are the compatibility checks. An unconditional guarantee covering every exceptional exchange decision is not a prerequisite for paper execution. This is an experimental premise, not a claim that disagreement is impossible. Actual outcomes and delayed settlement remain independent for each venue.

Do not add historical backtesting, a new research platform, or separate private-API recertification to the critical path. Use existing credential/feed setup when starting a run; missing credentials or failed data readiness still produce clear operational errors. The user subsequently approved the implementation plan and unattended `/build auto` execution. See [operating guide](../COMPLEMENT_PAPER.md) and [verification evidence](../COMPLEMENT_VERIFICATION.md).

## Scope and Assumptions

- This spec covers one immediate-pairing capability end to end, using existing execution, risk, capture, and reporting infrastructure. Shared infrastructure changes are limited to what that capability needs.
- Automatic discovery and rolling through successive Bitcoin 15-minute windows are included in v1. Configure the permitted venue series and environment rather than requiring manual market IDs for every window. Each discovered window must have an unambiguous UTC start and end and pass exact-market verification before entry.
- Entry considers YES on Kalshi plus NO on Polymarket US and the reverse. Same-exchange candidates may be evaluated only where the account/outcome model can represent their acquisition without self-trading or fictional liquidity. If netting makes independent purchases ineligible, record the reason and do not execute them.
- Only immediate taker entries are included. Resting-entry/immediate-hedge and buy-first/wait-for-dip remain separate later experiments; no first-entry signal for either is implied here.
- A dedicated PAPER portfolio owns the strategy, both exchange balances, and all of its positions. No inter-exchange transfer or shared cash balance is assumed.
- Initial daily loss setting is 10%, measured against start-of-day equity, with a UTC day boundary. Configuration may select 10–15%. These are proposed operational choices from the approved idea, not calibrated trading parameters.
- Real-money placement, automatic account reconciliation with live private accounts, automatic process recovery, new assets, and commodities are excluded.

## Existing System and Required Gaps

Repository evidence, inspected at local base `3a2c2ce`:

| Area | Existing behavior | Requirement for this feature |
| --- | --- | --- |
| `src/paper_trading/runtime.py` | Validates bounded lists of individual intents; dispatches public-update callbacks | Admit linked two-leg decisions without decomposing their risk approval; progress recovery on account updates and timers |
| `src/strategies/context.py` | Supplies controlled time, seeded randomness, deterministic intent IDs | Preserve isolation and deterministic identities for pair decisions |
| `src/market_decisions/order_coordinator.py` | `submit_batch` supports linked paper submissions and independent-leg execution | Use that gateway; maintain cancellation, late-fill, and recovery correlation |
| `src/market_decisions/risk_manager.py` | Checks funds, fees, exposure, batch sizing, high-water drawdown, and individual stop losses | Add daily-loss enforcement and treat verified matched quantities as a hedge |
| `src/paper_trading/paper_trading_engine.py` | Models arrival delays, depth, independent group execution and cancellation deadlines | Reuse realistic execution; a group deadline does not by itself liquidate orphan inventory |
| `src/paper_trading/fees.py` | Decimal fee calculation, per-order accumulators, conservative reserves | Reuse effective account/market profiles; validate their applicability |
| `src/paper_trading/replay.py` | Public replay reproduces recorded decisions; fresh strategies use an internal test seam | Distinguish reproduction from rerunning a strategy on captured public inputs |
| Public-feed adapters, runtime subscription/readiness configuration | Subscriptions and required market identities are fixed for a run | Discover successive windows, admit bounded subscription changes, and refresh exact-market readiness without resetting the account |
| `main.py` | Paper-only commands; bundled strategy registry has synthetic smoke only | Register this strategy deliberately for configured paper runs |

These are observations of code, not evidence that authenticated feeds or execution assumptions have been calibrated.

## Market Eligibility and Settlement Verification

### Validation result: 2026-11-13

**Discovery follow-up, 2026-11-14:** a [matching active pair with nonempty public books](../validation/bitcoin-15m-2026-11-14/README.md) was found for 02:00–02:15 UTC. Public market availability is established for that sample. Feed freshness, account profiles, actual fill behavior, and the unconditional contractual payout bound remain separate checks; this discovery does not authorize ignoring them.

The [actual-pair validation report](../validation/bitcoin-15m-2026-11-13/README.md) inspected Kalshi `KXBTC15M-26NOV130100-00` and Polymarket US `2464553`, covering 05:45–06:00 UTC. Normal reference, thresholds, and observed settlement values matched. Exceptional data-error/correction, missing-data, and review provisions do not establish an unconditional $1 combined-payout lower bound. The initial report did not certify either orientation against the earlier unconditional guarantee requirement. That historical finding remains evidence, not a continuing block on paper execution under the scope clarification above.

Both public REST APIs were reachable, but the inspected pair had already settled and its books were empty. The then-active Kalshi window had no matching US Bitcoin window in the captured series response. Simultaneous executable depth, account-specific fees, authenticated feed readiness, and measured execution assumptions remain unverified. Proceed with the scoped live paper experiment using the operational checks below; record the settlement premise and independently account for actual outcomes. Do not describe this as a certified live-money guarantee.

Use the actual Kalshi series-linked `CRYPTO.pdf` and the official US CPCBTC product filing plus its October 20 amendment. The preliminary CRYPTO15M reference and generic crypto FAQs are insufficient controlling evidence. Persist resolved document URLs and versions per admitted market.

Before entry, require a captured, versioned verification record tied to the exact venue market IDs. An arbitrary `market_equivalence` string alone is not evidence. Record:

- Venue environment (production versus preproduction), asset, contract IDs, currencies, payout units, and outcome mapping.
- Window start/end, last trading time, opening threshold, reference index, observation intervals, averaging, precision, rounding, and tie comparison.
- Payout behavior for complete data, missing data, corrections, cancellation, review, and other exceptional settlement paths.
- Official rule URLs, retrieved date, relevant rule text or digest, and the reviewed conclusion about compatibility.

For paper entry, require matching normal-settlement terms and use an assumed combined payout of $1 per matched pair. Record the compatibility checks and this premise with the run. Exceptional settlement uncertainty is a disclosed research assumption, not an unconditional payout guarantee or a prerequisite for the first paper run. An actual mismatch must remain visible in accounting and results.

Verify opening thresholds once available; identical window labels or shared index providers are insufficient. Changed terms, mismatched thresholds, unknown normal settlement mechanics, or missing required metadata block new entries. Existing inventory remains tracked through authoritative outcomes and risk recovery.

The current US crypto FAQ describes CF Benchmarks minute averages but also mentions preproduction availability. Kalshi's help page similarly describes minute averages; a generic contract PDF includes exceptional-data provisions. These sources guide verification, but do not establish that any particular production pair is eligible. Actual market terms control. See Sources.

## Automatic Rolling Windows

The running paper session must discover, prepare, and switch to consecutive eligible Bitcoin 15-minute windows without a manual restart or configuration edit. This is part of v1, including observation mode. Discovery uses official market metadata and paginated APIs, restricted to configured series, asset, duration, and environment. Do not guess identifiers from titles or reuse the previous window's opening threshold.

Discover the current and next window on a bounded polling schedule with configured lookahead, pagination/request limits, retry backoff, and rate budgets. Match by verified UTC boundaries and outcome semantics. Deduplicate canonical market IDs and reject ambiguous matches. A missing, delayed, incompatible, or unavailable counterpart produces a recorded skipped window; it never authorizes pairing adjacent windows or changing environment.

Prepare the next window's subscriptions and metadata before its entry period where available. Readiness is specific to that window and both venues: obtain complete fresh depth, effective fee/precision profiles, and a verification record for its exact IDs and opening thresholds. A reviewed series-rule template may be reused only when its checked rule version still applies; every window needs its own metadata/threshold checks. Unknown opening values block entry until verified. Changed terms invalidate eligibility.

At the prior window's entry cutoff, block further entries for that window. Cancel pending entry orders by their deadlines and continue reconciling their fills. Activate the next pair only when its entry period has begun and it is fully verified and ready; activation does not reopen the old window. Keep at most one entry attempt in flight across all windows. Unresolved excess, unknown order status, or any portfolio risk block prevents new entries in subsequent windows.

Carry cash, positions, commitments, daily-loss baseline/latch, and pending settlement across the rollover. Do not reset capital or assume an expired contract has paid. Matched holdings from older windows may coexist with the active window within aggregate limits, but their cash remains unavailable until authoritative credit. Continue receiving the metadata, settlement, and execution updates needed for all older obligations.

Bound active/prepared/retained market counts, discovery results, subscription tasks, buffers, and archive working state using explicit configuration and existing resource limits. Retire old depth subscriptions when no recovery, valuation, or execution work needs them, while retaining the lifecycle monitoring needed for unsettled positions. Archive settled history through the existing store. If retained obligations or next-window preparation exhaust capacity, skip/pause new entries visibly; never discard unsettled inventory or enlarge limits silently.

Market admission, subscription changes, readiness, activation, skipped windows, and retirement must be captured as ordered events. Preserve late old-window reports under their original market IDs and pair IDs. Reconnection must rebuild current readiness and discover the actual current/next windows without replaying missed entry opportunities. Account state survives transport recovery; full process restart remains outside scope.

## Quote, Quantity, and Profitability Rules

Use fresh, sequenced, executable ask depth for both legs. Reject missing, crossed/invalid, disconnected, incomplete, or stale books. Enforce configured maximum book age and maximum cross-venue observation skew using recorded availability times; never read future updates from a capture.

For a feasible equal quantity `q`, define:

```text
assumed_pair_value = q * assumed_combined_payout
quoted_cost = depth_cost(first_leg, q) + depth_cost(second_leg, q)
conservative_cost = first_leg_price_cap * q + second_leg_price_cap * q
                    + conservative_entry_fee_reserves + explicit_other_entry_costs
entry_surplus = assumed_pair_value - conservative_cost
admit only when entry_surplus >= q * min_net_profit_per_pair_usd
```

Price caps include the configured allowed adverse movement and are rounded to valid venue ticks. Fees must bound fills permitted by those caps, including price-dependent rates, fragmentation, order-wide adjustments, and account rounding. Quoted cost and conservative cost are both reported; do not subtract the same slippage allowance twice. Do not include speculative rebates, recycled unsettled payouts, or transfers as available funds.

Use decimal arithmetic for prices, quantities, fee calculations, and profit comparisons. If existing float-based intent boundaries are retained, validate conversions and tick/quantity alignment at the boundary so rounding cannot reverse an admission decision.

Select quantities on the common venue quantity grid and at or above both minimums, within depth, separate available balances, fee reserves, risk budgets, and configured pair spend. Search only a bounded configured number of quantity candidates. Choose the eligible candidate with greatest conservative total surplus, breaking ties by smaller spend and then stable orientation ID. These rules are deterministic; an empty feasible set means no trade.

The risk manager must preserve equal quantities when resizing a batch. The final approved quantity and price bounds must still satisfy profitability after nonlinear fee rounding. A smaller size is not assumed to retain the same per-pair economics. Do not supply invented directional probabilities, such as probability 1 for each leg, to bypass existing sizing rules.

## Decision and Execution Lifecycle

1. **Observe:** verify terms, account readiness, quote freshness, cutoff, and portfolio risk status. Reevaluate after relevant updates without repeatedly submitting against unchanged depth.
2. **Propose:** create a linked 1:1 batch with stable correlation IDs, price bounds, capital ceiling, expiry, and execution deadline. Whole-result validation occurs before any leg is submitted.
3. **Approve and reserve:** the risk manager approves the pair together against shared budgets and independent exchange funds. Rejection of either leg before dispatch means neither leg is sent.
4. **Execute independently:** use concurrent bounded limit orders with FOK entry instructions where supported and verified. Reject an unsupported instruction rather than silently weakening it. Each venue can fill or reject independently; this is not atomic arbitrage.
5. **Reconcile:** use observed cumulative order/execution reports, including corrections, to establish filled, pending, matched, and excess quantities. An acknowledgement or cancellation request does not establish a fill or final cancellation.
6. **Hold matched quantity:** retain the verified matched portion to authoritative settlement. Do not treat expiry time alone as a payout or book the projected surplus as realized profit.
7. **Recover excess:** cancel outstanding entry quantities on rejection, execution deadline, daily stop, or loss of readiness. After pending quantities are resolved, submit bounded reduce-only IOC sells of excess inventory against fresh executable bids. Do not sell the matched portion. Never open a speculative replacement leg in this MVP.
8. **Close or retain visibly:** allow only configured recovery attempts until the recovery deadline. If liquidity, connectivity, or final order status prevents safe completion, stop new entries, retain uncertain commitments and residual inventory, and report unresolved recovery. Do not fabricate a fill or silently mark the attempt successful.

Allow at most one entry attempt in flight across the rolling session. Hold only a configured bounded number of completed groups; unresolved excess prevents another entry in any window. An unchanged book revision cannot justify repeated attempts, but an explicitly bounded retry may follow materially new executable liquidity after the prior attempt is fully resolved.

Progress deadlines and cancellation/recovery even when there are no market updates. Account-only and timer-driven processing must not manufacture fresh quotes or bypass readiness. The strategy remains unable to mutate the coordinator/account or invoke private reports; the exact callback/result extension is a planning decision subject to these requirements.

On shutdown or strategy disable, cancel entries and perform bounded recovery when data and shutdown grace allow. Otherwise retain and report obligations. Late fills during cancellation must update recovery quantities exactly once. No recovery sell may exceed confirmed owned excess after accounting for pending exits.

After each window, the rolling controller prepares and activates the next eligible pair under the rules above. This transition changes the markets being considered; it does not close old positions or reset daily risk.

## Centralized Risk and Pair-Aware Exits

The strategy owns opportunity selection and its required profit margin. The risk manager owns independently enforced funds, exposure, maximum pair spend, maximum unhedged exposure, daily loss, and admission of proposed actions. The coordinator owns order submission/cancellation and execution correlation. Pair verification, fee calculations, and accounting have one authoritative representation rather than strategy-specific competing ledgers.

Calculate daily equity as cash plus a conservative liquidation value of held inventory, net of estimated liquidation fees. Reserved cash is already part of cash and must not be subtracted twice; fees already debited must not be counted twice. Value unsettled matched and unmatched holdings consistently using executable sell depth; give unsellable residual quantity zero liquidation value and flag incomplete valuation. Stale/missing inputs pause entries instead of allowing a favorable stale valuation. Projected settlement proceeds are reported separately.

Daily loss fraction is `max(0, (day_start_equity - current_equity) / day_start_equity)`. A zero/invalid baseline blocks entries. Set the baseline at session start on its first UTC day; at subsequent midnight boundaries, use conservative equity from information available at that boundary. If it cannot be established reliably, pause entries until an explicitly auditable conservative baseline is established; do not shift the baseline to erase a loss.

At or above the configured daily limit, latch entry prohibition for the rest of the UTC day and cancel pending entries. Permit verified risk-reducing exits while retaining ordinary position, ownership, and order-safety checks. Midnight does not clear unresolved orders, excess inventory, connectivity failures, or other risk blocks. Deposits, withdrawals, and ad hoc top-ups during an evaluation run are disallowed so they cannot mask performance.

The existing high-water drawdown rule remains distinct and must not be relabeled a daily stop. Individual stop losses must not sell the verified matched portion of a pair. Orphan quantities remain subject to risk reduction; hedge exemption is limited to actual confirmed matched quantities with valid verification evidence, not arbitrary batch labels.

Loss limits are triggers, not guarantees on realized loss: gaps, fees, and missing liquidity can cause overshoot. Record the trigger equity, outstanding orders, subsequent fills, and final loss. Do not omit loss-making recovery from results.

Crash/restart continuation is not supported by the existing runtime. A new independent paper evaluation may start a new ledger, but cannot be represented as continuation of the same daily-risk history. Any future resume feature must preserve baseline, latch, positions, and commitments before admitting entries.

## Configuration Contract

Register an explicit factory name, proposed `immediate-complement-v1`, routed only to a dedicated PAPER portfolio. Add one documented example configuration with placeholders; it must not be mistaken for a verified deployable profile.

Required settings, validated before activation:

- Allowed Bitcoin series identifiers and environment per venue, verification template/version, enabled eligible orientations, and quantity-search bound. Capture exact discovered IDs and per-window verification records before activation.
- Discovery poll interval, lookahead, maximum pages/results, request/retry budgets, and bounds on prepared windows, retained markets, and subscription tasks. Settings must fit existing runtime limits and account for older unsettled positions.
- Positive minimum net profit per pair in dollars and nonnegative allowed adverse entry movement; no profit threshold is claimed to be optimal.
- Maximum pair spend, maximum aggregate held spend, maximum unhedged exposure, and bounded completed-group count, consistent with venue balances and centralized limits.
- Maximum book age, maximum observation skew, minimum time before close for entry, entry execution timeout, recovery timeout, maximum recovery attempts, and recovery sell price bounds.
- Effective-dated venue/account fee models and precision; known non-trading costs or an explicit zero-cost assumption; measured or explicitly hypothetical latency/request-budget assumptions.
- Per-venue initial cash, daily limit, UTC baseline policy, and all existing runtime capacity limits.

All monetary/timing values must be finite and correctly signed. Reject missing bounds, unknown orientations, unsupported execution modes, unrepresentable quantities, invalid expiry relationships, and settings that cannot fit shutdown or close-time constraints. The entry cutoff must leave room for configured execution and recovery; operations reaching close still follow actual venue lifecycle events.

Profit/timing/exposure parameters have no hidden public-run defaults. Supply labeled synthetic fixture values for deterministic tests. Capture the selected values and provenance with every run; credentials never enter configurations committed to source or captured artifacts.

## Capture, Replay, and Reporting

Reuse the bounded archival streams and capture integrity checks. Record opportunity evaluations with stable reason codes, book revisions and ages, orientation, quantity, costs, fee profile, verification version, projected surplus, rejection reason, and resulting batch/order IDs. Bound candidate details per evaluation and archive them rather than growing in-memory arrays.

Capture discovery responses and rolling-control events needed to reconstruct the admitted market universe, including each window's metadata, rule checks, readiness, subscription lifecycle, skipped-window reasons, and activation times. Preserve older-window fills and settlement alongside newer-window market data. Replay consumes captured discovery/control inputs instead of querying today's listings. Do not infer fills or evaluate uncaptured depth for an unrecorded market.

Preserve deterministic time, IDs, event ordering, callback coalescing, risk ticks, and timer/report recovery decisions in replay. Recorded-decision replay must reproduce accounting but is not a test of new strategy parameters. Preserve compatibility with the existing recorded-decision replay. A new fresh-strategy replay/backtesting workflow is deferred; it is not required to begin live paper trading.

Summaries must include attempted/rejected/unfilled/partially or fully paired groups; projected versus realized net profit; entry and recovery fees; actual credited rebates separately; recovery proceeds/losses; unhedged quantity and duration; available/reserved cash per venue; held capital; settled versus unsettled results; executable ending inventory value and unvalued quantity; daily-stop events and overshoot; discovered/activated/skipped window counts; and completeness/assumption warnings. Detail streams contain per-window and group attribution and lifecycle evidence, while aggregate summaries remain bounded.

Distinguish a properly stopped entry process from complete inventory recovery. A session may shut down cleanly with explicitly retained matched inventory; uncertain cleanup must retain the existing runtime failure semantics. Finite datasets never create future liquidity or settlement. No-op and blocked runs report why they did not trade.

## Tech Stack and Project Structure

Use the existing Python asyncio runtime, dataclasses, standard-library `decimal`, JSON/SQLite capture, pytest, and pytest-asyncio. CI uses Python 3.12. Dependency constraints remain those in `requirements.txt`, including `polymarket-us==0.1.2`; this spec introduces no new package requirement or SDK upgrade.

Proposed source location is `src/strategies/immediate_complement.py`. Extend existing runtime/context, public-feed discovery/subscription adapters, coordinator, risk, fee, replay, and reporting owners only as needed. Rolling discovery and subscription management belong to supervised runtime/feed infrastructure; the strategy evaluates the active verified pair. Shared types belong alongside existing types in `src/utilities/classes.py`; exact interface signatures and file splits belong in the implementation plan. Tests belong under `tests/`, examples under `examples/`, operating documentation alongside `docs/BOT_RUNTIME.md`, and this spec under `docs/specs/`.

## Commands

Run from the repository root with dependencies installed in the selected Python environment. These existing commands are executable now; output directories must be new:

```bash
python -m pip check
python -m compileall -q main.py src examples tests
python -m pytest -q
python main.py smoke --output /tmp/complement-spec-smoke
python main.py replay --capture /tmp/complement-spec-smoke --output /tmp/complement-spec-replay
python main.py run --config local-complement.json --output /tmp/complement-observe --observe-only --duration 60
```

The observation command requires a locally supplied valid public-feed configuration and credentials. After implementation, the existing run command must also support the registered factory:

```bash
python main.py run --config local-complement.json --output /tmp/complement-paper --duration 3600
python main.py replay --capture /tmp/complement-paper --output /tmp/complement-paper-replay
```

The local configuration must select the strategy and rolling series explicitly; it is not created by the command. The duration is an example for observing multiple rollover boundaries, not a guarantee of settlement or eligible trades. Without `--duration`, the run continues until stopped and must keep rolling within configured bounds. Fixed-market observation configurations must continue to work. No standalone linter/type-check command is currently configured in CI. Document the fresh-strategy replay command/API when its interface is specified; do not invent an already available CLI flag.

## Code Style

Follow the small factory/context style already demonstrated in `src/strategies/smoke.py`:

```python
async def on_snapshot(self, snapshot):
    if self.sent:
        return None
    self.sent = True
```

This is an existing style excerpt, not the entry algorithm. Use snake_case functions, explicit state transitions, short helpers, and stable rejection reasons. Use the context clock/IDs rather than wall-clock reads or random UUIDs inside strategy decisions. Avoid extra generic trading frameworks, hidden network requests, and unbounded per-snapshot work. Existing single-intent strategies must retain their contract and behavior when paired decisions are added.

## Testing Strategy and Acceptance Criteria

Use deterministic unit and integration tests with pytest/pytest-asyncio and existing fake clocks/feeds. Do not require credentials or live network access in ordinary CI. Acceptance is behavior coverage below, not a new arbitrary line-coverage percentage. Preserve existing tests and safety checks.

1. **Eligibility:** reject mismatched windows, opening thresholds, indices, ties/rounding, unknown normal settlement mechanics, environment mismatches, incomplete verification, and changed terms. Eligible synthetic terms admit the intended orientation.
2. **Economics:** the 40-cent plus 50-cent example yields 10 cents gross per pair before costs; admission uses net surplus after exact/reserved fees and configured bounds. Test a gross-positive/net-negative pair, threshold equality, fee rounding/fragmentation, decimal boundary values, and a resized batch that becomes uneconomic.
3. **Depth and capital:** reject insufficient depth, stale or skewed books, unavailable per-venue funds, incompatible quantity grids, and unsupported same-venue netting. Both configured $25 and $50 venue balances are valid scenarios, without pooling balances.
4. **Linked admission:** validate the entire decision before dispatch; approve/reserve linked quantities together; reject foreign/malformed/duplicate IDs and capacity overruns without partially submitting a callback result. Repeated unchanged snapshots do not duplicate attempts.
5. **Independent execution:** demonstrate both fill, neither fills, and one fills/one rejects under nonzero delays. No synthetic atomic fill guarantee, untouched resting fill, or invented probability may create a trade.
6. **Recovery:** test cancellation races, delayed/duplicate/corrected reports, mismatched partial quantities, timer-only progression, fresh-data requirements, bounded recovery orders, missing bids, residual inventory, expiry/close, disconnect, and shutdown. Pending exits cannot cause overselling.
7. **Central risk:** test the daily threshold including fees/unrealized liquidation loss, exact trigger boundary, latch after recovery, UTC rollover with carried holdings, invalid baseline, independent high-water drawdown, risk-reducing sells while entry-blocked, and no single-leg stop on confirmed matched quantity. Controls apply at dispatch and periodic checks, not only inside strategy code.
8. **Accounting:** reconcile group and portfolio totals for settled wins, failed hedges, recovery sales, authoritative exceptional payouts, and unfinished runs. Never count projected payout as realized cash or omit remaining positions.
9. **Replay:** identical captured decisions reproduce fills/accounting. Include report/timer-only recovery, daily boundary, and disabled-strategy cases. A fresh-strategy backtesting interface is deferred.
10. **Compatibility and bounds:** existing smoke, replay, individual strategies, archive limits, and public-feed readiness behavior continue to pass. Register the new strategy without enabling live execution or arbitrary code imports.
11. **Rolling:** run a synthetic sequence of at least three consecutive windows without restarting. Verify discovery pagination/deduplication, matching by exact UTC windows, activation only after both venues are ready, late/missing counterparts, unknown opening thresholds, rule changes, rejected adjacent-window matches, and close-time entry cutoffs. A gap is skipped without stopping future discovery.
12. **Carryover and recovery:** retain old-window fills, pending cancellation, settlement, balances, and daily risk when activating a later window. Show that unresolved excess blocks later entry, settlement arriving late credits once, and midnight risk handling remains independent of window rollover. No subscription removal may lose required lifecycle evidence.
13. **Rolling replay and resource bounds:** replay discovery, admission, skips, readiness, rollovers, and late old-window reports deterministically without network requests. Exercise repeated rollovers beyond the configured retention count and verify bounded working state, settled-history archival, rate-budget handling, and visible pause when retained obligations consume capacity. Full captures remain on disk; no unlimited in-memory market registry is permitted.

Evaluate the first experiment on live public-market paper runs, including all attempts, costs, residual inventory, and assumption provenance. Synthetic profit or uncalibrated paper results do not establish profitable real-money performance. Historical backtesting, parameter optimization, and statistical certification are later research, not v1 completion gates.

## Boundaries

- **Always:** use coordinator/risk gateways; validate exact market terms; retain uncertain commitments; keep accounting and decision evidence; use controlled time and bounded work; run relevant tests before commits; label hypothetical execution parameters.
- **Ask first:** expanding into delayed-entry trading, additional assets/series beyond the configured Bitcoin scope, live-money execution, incompatible-settlement basis trading, new dependencies, unrelated CI/schema changes, or revising approved risk semantics. Automatic rolling/discovery for the scoped Bitcoin windows is explicitly authorized for v1. Routine implementation choices within this spec do not require repeated approval.
- **Never:** commit credentials; bypass risk with inflated probabilities; imply cross-venue atomicity; credit guessed settlement; fabricate liquidity; drop losing attempts; silently lower checks, skip failing tests, or overwrite capture output.

## Open Questions and Gates

### Approved operating choices

- The user approved the spec after explicitly adding automatic rolling through consecutive Bitcoin 15-minute windows to v1.
- Initial daily loss setting is 10% with a UTC reset and conservative executable-liquidation equity, as specified above.
- Failed-leg recovery cancels entries and sells excess instead of opening an additional hedge after the original attempt fails.

### Required evidence before public paper execution

- Verify permitted production series and their discovery metadata. For every discovered window, resolve exact market IDs and verify matching normal-settlement terms. Record exceptional settlement uncertainty without claiming an unconditional guarantee.
- Use explicit effective fee profiles and quantity/price rules, verify order-instruction support, and require fresh public-feed readiness at runtime. Published conservative fee assumptions may be used and labeled; account-tier/private-API checks are not a separate prerequisite.
- Select and label net-profit, spend, freshness/skew, cutoff, execution/recovery, and residual-risk settings. Measure latency where possible and record uncertainty.

### Later research

- Establish observation duration, independent sample requirements, and criteria for accepting or rejecting an economic edge.
- Define entry signals and independent evaluation for the two delayed-entry experiments.

These checks support the live paper experiment. They must not expand into mandatory backtesting, private-endpoint recertification, or an unconditional settlement-guarantee research project. Missing market terms, unusable fees, stale feeds, or risk-limit breaches still block affected entries at runtime.

## Sources

Official references checked 2026-11-13; capture current market-specific evidence again before execution:

- [Polymarket US crypto FAQ](https://docs.polymarket.us/faqs/crypto-faqs): market discovery and settlement overview; includes environment-availability caveats.
- [Polymarket US fee schedule](https://docs.polymarket.us/fees): account tiers, effective dates, and per-order rounding; do not substitute international Polymarket fees.
- [Kalshi crypto overview](https://help.kalshi.com/en/articles/13823838-crypto-markets): reference-index and window-average overview; specific market rules prevail.
- [Kalshi published CRYPTO15M contract document](https://assets.kalshi.com/contract_terms/CRYPTO15M.pdf): reference and exceptional payout provisions; verify the actual terms linked by the selected market.
- [Actual-pair validation and evidence](../validation/bitcoin-15m-2026-11-13/README.md): selected series links to [CRYPTO.pdf](https://assets.kalshi.com/contract_terms/CRYPTO.pdf); official US product/notice listings supply CPCBTC-specific filings and amendments.
- [Kalshi fee rounding](https://docs.kalshi.com/getting_started/fee_rounding): account precision and per-order accumulators.
- [Runtime guide](../BOT_RUNTIME.md), [simulator guide](../PAPER_TRADING.md), and [risk manager guide](../RISK_MANAGER.md): existing repository behavior and unverified assumptions.
