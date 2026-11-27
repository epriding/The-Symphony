# v2 trading bot specification

## What this version is for

v2 will turn the current paper-trading framework into a bot architecture that can run strategies against Polymarket US and Kalshi through shared data, risk, execution, capture, and control components. The first useful outcome is reliable market-data capture that supports review after a trading session. The next outcome is paper execution through the new gateway path. Real order routing comes later, after the required account, risk, cancellation, and rate-limit controls have been verified.

Strategies will ask shared services for market data and submit orders through a gateway client. They will not call a venue API or keep their own order ledger. This makes one strategy usable in live observation, replay, and paper trading.

## Scope and delivery order

The work has five dependent areas.

1. Capture records raw venue messages before normalization, assigns a receipt sequence and timestamps, writes accepted records away from the feed path, and marks gaps when its bounded queue cannot keep up. Trading continues during a capture gap.
2. Data delivery defines normalized events, a replay clock, a market-data bus, and universe providers for series and filtered discovery. Replay delivers records in the order the strategy originally received them.
3. Paper execution adds venue gateways, account checks, order management, and simulated fills that consume recorded order-book depth.
4. Portfolio risk checks every order across accounts. It reserves capacity before sending an order, retains it while a venue outcome is unknown, and owns a persistent manual-reset kill switch.
5. Live readiness adds account reconciliation and shared per-account rate limits. Only then can a later release consider real order routing.

SQLite or Postgres metadata, QuestDB recent-history queries, and Parquet archives remain follow-up storage work. The first release uses append-only capture files and a validator because review happens after sessions.

## Boundaries and risks

v2 begins with Polymarket US and Kalshi. It does not add Polymarket International, a network transport, or a live trading strategy. It does not close positions when the kill switch is used.

Capture may have gaps during a long disk stall or storage failure. The capture validator will identify the affected sequence ranges. Replay rejects incomplete captures by default because missing order-book updates can make later state unreliable.

The remaining design decisions are the measured rate limits for each venue account, the network transport for multi-process mode, and recovery behavior when one side of a cross-venue trade fills without the other.

## Done and verification

The first implementation slice is done when a simulated venue feed writes versioned raw records through a bounded queue, reports and records gaps, and a command validates the resulting capture. Automated tests must cover ordering, a full queue, writer failure, and detection of incomplete files. Existing compile, test, smoke, and replay checks must continue to pass.
