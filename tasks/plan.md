# Implementation plan: Live paper complement strategy

Status: software implementation and deterministic acceptance complete under the approved unattended `/build auto` run. The live two-transition smoke remains pending existing feed credentials. See [verification](../docs/COMPLEMENT_VERIFICATION.md) and todo.md.
Scope: immediate pairing on rolling Bitcoin 15-minute markets, Kalshi and Polymarket US.
Specification: [immediate complement strategy](../docs/specs/immediate-complement-arbitrage.md).
Task list target: [todo.md](todo.md). This index references that checklist rather than duplicating task status.

## Goal and scope decisions

Deliver a runnable paper strategy through the existing bot, not a second trading engine or a backtesting platform. Start with $25 per venue, configurable up to the user's proposed $50 per venue. Use a 10% daily loss stop by default, configurable within 10–15%, enforced by the risk manager against the dedicated PAPER portfolio.

Automatic rolling through consecutive Bitcoin 15-minute windows is part of v1. The first strategy attempts both legs immediately. Resting-entry and wait-for-dip experiments remain later work.

The user corrected the direction after market validation became too broad. Matching normal CF Benchmarks contract terms are the paper experiment's operational premise. Do not claim an unconditional settlement guarantee; do not require proving one before paper trading. Record source/correction uncertainty, wait for authoritative venue payouts, and account for each independently. The prior report is research evidence, not a veto on this scoped experiment.

No historical order-book reconstruction, new fresh-strategy backtesting interface, statistical edge certification, or private-endpoint recertification is required. Normal credential loading and public-data readiness remain operational requirements of the existing runner. Existing smoke/replay and regression tests remain required.

## Architecture decisions

- Reuse `BotRunner`, `PaperSession`, `OrderCoordinator.submit_batch`, the realistic independent-leg paper engine, `FeeModel`, the portfolio ledger, capture/archive, and existing recorded-decision replay.
- Put the strategy in `src/strategies/immediate_complement.py`. Keep opportunity selection separate from centralized risk enforcement and supervised discovery/subscription management.
- Evaluate both cross-exchange orientations. Treat same-exchange combinations as eligible only when the current netting model can represent them correctly; do not add a separate venue-netting implementation to v1.
- Use a bounded callback result that can carry existing single orders or one linked pair decision. Extend the supervised runtime/context, never hand the strategy a mutable coordinator or account. Preserve existing single-order callbacks.
- Admit equal quantities together, then execute each leg independently with realistic arrival/report/cancel delays. Batch risk approval does not guarantee two fills. Recheck economics at the final approved size using existing fee reserves and actual venue precision.
- Drive pair reconciliation and recovery on observed account changes and timers, including quiet markets. Maintain one unresolved entry attempt across all windows. Cancel failed entries and sell confirmed excess; keep matched quantities to authoritative settlement.
- Keep daily loss enforcement in `RiskManager`, using conservative executable-liquidation equity. A daily stop blocks new entries and requests cancellation while allowing valid reductions. Exclude confirmed matched quantities from single-leg stop-loss sales; do not exempt unrelated inventory.
- Add bounded discovery plus current/next-window subscription changes to the existing public feed path. Carry the ledger, daily risk state, old fills, and unsettled positions across rollover. Skip missing windows instead of guessing identifiers or matching adjacent windows.
- Archive details and preserve existing output conventions. Add strategy/window/recovery results to existing reports rather than introducing a dashboard or new persistence system.
- Register the strategy only for paper execution. Keep all real-order placement unreachable from the new launch path.

Exact additional type fields, capture-version migration mechanics, and callback event names are implementation details to finalize within the tasks below. They must satisfy the listed contracts and preserve existing capture compatibility; do not create a broad generic plugin/event framework.

## Dependency graph and build order

```text
T01 quote economics -> T02 linked decision gateway -> T03 final pair approval
                                                       |
                                                       v
T04 independent execution/recovery -> T05 daily risk -> T06 matched inventory
                                                       |
                                                       v
T07 discovery -> T08 rolling subscriptions -> T09 carryover/retirement
                                                       |
                                                       v
T10 capture/replay -> T11 reporting -> T12 configured paper launch
                                                       |
                                                       v
T13 integrated scenarios -> T14 operating guide and live paper smoke
```

This is a conservative sequential build order that avoids simultaneous edits to shared runtime/accounting code. The task list records the actual dependencies. No agents or parallel implementation are started by this plan.

## Ordered task index

1. T01: Price a candidate pair after fees.
2. T02: Submit a linked strategy decision through the runtime.
3. T03: Preserve pair economics through centralized risk approval.
4. T04: Recover independently failed entry legs.
5. T05: Enforce the daily loss stop in the risk manager.
6. T06: Preserve matched holdings through exits and settlement.
7. T07: Discover matching live Bitcoin windows.
8. T08: Activate successive public-feed windows.
9. T09: Retain old obligations across rollover within limits.
10. T10: Record and replay the new lifecycle.
11. T11: Report actual strategy results.
12. T12: Expose the configured paper strategy through the CLI.
13. T13: Verify the complete experiment with deterministic scenarios.
14. T14: Document and exercise the live paper workflow.

Checkpoints follow T03, T06, T09, T12, and T14. Each checkpoint includes a reviewable result and regression verification. Failed checks are fixed rather than suppressed. User approval of this plan authorizes its scoped implementation; checkpoint results do not require repeating earlier permissions unless scope changes or a material unresolved decision arises.

## Acceptance contract

- **Fees:** debit actual simulated fills using the existing decimal fee models; reserve conservatively before dispatch, including fragmentation and rounding. Gross-positive/net-negative pairs must not trade. Any configured fee/latency assumptions appear in results.
- **Stale quotes:** missing, invalid, stale, disconnected, incomplete, or excessively skewed quotes block entry and arrival matching as appropriate. Reconnection alone does not restore readiness. Source timestamps and local receipt times retain their distinct meanings.
- **Independent fills:** a profitable decision can produce two fills, zero fills, or one fill. Depth can change after decision and before either arrival. Duplicate or corrected reports must not duplicate money or inventory.
- **Recovery:** one filled leg triggers cancellation/reconciliation and bounded reduce-only sales of excess. Cancel races and pending sells cannot oversell. Missing liquidity leaves visible residual inventory and blocks further entries.
- **Daily risk:** 10% default daily loss includes fees and conservative unrealized liquidation loss across both venues. The risk manager latches the stop until UTC day rollover, cancels pending entries, permits valid reductions, and never resets on market rollover. Overshoot is reported rather than hidden.
- **Rolling:** at least three successive synthetic windows run without manual market changes or account resets. Missing/delayed counterparts are skipped; old authoritative settlements and late fills remain attributable and credit exactly once.
- **No real orders:** new launch wiring creates paper accounts and public-data transports only. No private order endpoint is called.
- **Honest results:** include rejected/unfilled/failed attempts, recovery costs, held capital, and unresolved inventory. No profitable run or minimum trade count is required to prove the software works.

## Verification approach

Use pytest/pytest-asyncio, existing deterministic clocks/feeds, and production-shaped public payload fixtures. Focused test paths listed in `todo.md` are planned additions, not commands already available today. Run them after implementing their task. Use the project's installed Python environment; examples below use `python3` because this workspace currently has no `python` executable or installed pytest environment.

```bash
python3 -m compileall -q main.py src examples tests
python3 -m pytest -q
python3 main.py smoke --output /tmp/complement-plan-smoke
python3 main.py replay --capture /tmp/complement-plan-smoke --output /tmp/complement-plan-replay
```

Dependencies must already be installed from `requirements.txt` in the chosen environment before running Python tests/runtime checks. Do not add dependencies or alter CI for this feature. Use new output directories for every command invocation. There is no new coverage percentage, generic lint gate, or unrelated benchmark project.

The final live paper smoke uses the existing public-feed credentials and explicit paper configuration. Do not require separate private account endpoint tests, permission changes, or real order probes. If credentials are unavailable, report that operational limitation without changing the strategy or marking the live smoke passed. Synthetic integration success still establishes the code path; economic conclusions require actual live paper results.

## Risks and mitigations

| Risk | Handling within scope |
| --- | --- |
| Risk resizing erases edge after fee rounding | Reprice approved quantities before dispatch; reject if below threshold |
| Second leg fails or cancel loses a race | Confirm cumulative state; recover only owned excess; bound attempts |
| Strategy is idle when a deadline expires | Timer/account-triggered progression independent of market callbacks |
| Stop loss sells half a completed hedge | Matched-quantity-aware risk treatment; excess remains eligible for reduction |
| Discovery picks the wrong boundary or administrative expiry | Typed market terms and canonical IDs; check source, open value, UTC measurement times |
| Public book appears current only because REST returned now | Preserve source/receipt timestamps and feed continuity; readiness checks remain active |
| Repeated windows grow retained state | Finite prepared/active/retained budgets, archive settled history, pause visibly at capacity |
| Settlement is delayed or differs | Keep actual per-venue inventory and payouts; no assumed $1 cash credit |
| Published fee assumptions differ from account entitlement | Explicit conservative profile and provenance; do not depend on rebates |

## Remaining implementation choices

- Choose small documented paper defaults for profit margin, pair spend, delays, freshness/skew, and recovery bounds in T12. Label unmeasured values; require finite validated configuration and do not claim optimal settings.
- Select the smallest backward-compatible callback/batch and rolling-admission extension. Each task has a five-file budget; split any larger implementation before editing rather than quietly expanding it.
- Existing recorded replay must continue to work. Historical model tuning and a fresh-strategy replay product are explicitly deferred.

These are implementation choices, not another market-validation interview. Return to the user only for a material change to the approved experiment or a concrete blocker that requires their input.
