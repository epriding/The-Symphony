# Cross-Exchange Complement Arbitrage

Status: approved idea for paper-trading research; not an implementation specification.

## Problem Statement

Can your bot acquire complementary outcomes on Kalshi and Polymarket US for less than their combined payout, after fees and realistic execution costs, using $25–$50 per exchange?

## Recommended Direction

Start with one verified Bitcoin 15-minute market pair and record executable order-book depth. Run these experiments against the same captured data, each with its own identical starting balances:

1. **Immediate pairing:** purchase both sides when the available combined cost clears a minimum net-profit threshold. Evaluate both cross-exchange directions and eligible same-exchange combinations.
2. **Resting entry, immediate hedge:** post a limit order on one side. After a fill, immediately attempt the opposite side within a defined price ceiling. Test whether better entry prices compensate for missed fills and adverse movement.
3. **Buy first, wait for the opposite side:** purchase one side using an explicit entry rule, then wait for a cheaper complementary position. This tests a directional trading hypothesis until the hedge completes.

The third experiment needs the most skepticism. Its first-entry signal, maximum waiting time, and fallback exit must be defined before testing. Otherwise, “buy cheap and wait” can produce attractive completed pairs while accumulating losses in unfinished positions.

## Key Assumptions to Validate

- [ ] **Contract compatibility:** the selected outcomes provide the intended combined payout across relevant settlement scenarios. Verify the actual market rules, including rounding, ties, and missing-data treatment.
- [ ] **Executable opportunities:** profitable prices exist simultaneously at sufficient size and remain available through realistic order delays.
- [ ] **Fees:** account-specific rates, rounding, and any applicable market overrides are correct. Do not rely on hypothetical rebates.
- [ ] **Resting-entry advantage:** savings survive the price movement that follows a fill; touching a limit price must not automatically count as a fill.
- [ ] **Delayed-entry advantage:** the first-entry signal and subsequent price movement compensate for failed pair completion and forced exits.
- [ ] **Capital feasibility:** minimum quantities, fee rounding, and separate exchange balances allow trades at the intended account size.

## MVP Scope

Observe first, then paper-test all three approaches with separate accounting. Measure net profit, completed-pair frequency, unhedged duration, failed-hedge losses, capital tied up, and remaining inventory valued at executable liquidation prices.

Use **10% of start-of-day equity as the initial paper daily-stop setting**, configurable within the requested 10–15% range. Proposed reset: midnight UTC. Include realized losses, executable unrealized losses, and fees across both exchanges.

The risk manager should enforce that limit and exposure caps. The coordinator should cancel pending entries and handle outstanding fills. Reaching the limit should stop new risk while permitting risk-reducing actions; it cannot guarantee losses never exceed the threshold. Completed hedges need treatment that avoids unnecessarily selling one side.

## Not Doing (and Why)

- **Real-money execution:** first establish an edge in paper trading.
- **Commodities or additional crypto assets:** validate the first pair before expanding the market universe.
- **Treating similar market titles as proof of complementary settlement:** actual contract terms must establish compatibility.
- **Increasing size to recover losses:** losses must remain subject to centrally enforced limits.
- **Selecting parameters and claiming success on the same recording:** evaluate selected parameters on separate data.

## Open Questions

- Is the selected market pair contractually compatible across settlement scenarios?
- What are the first-entry rules for the resting-entry and delayed-pair experiments?
- Which measured execution assumptions should drive the simulations?
- What net-profit buffers, hedge price ceilings, waiting times, and recovery deadlines should apply?

Failure to find compatible contracts or positive returns under realistic fills is a valid reason to stop.
