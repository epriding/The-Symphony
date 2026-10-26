# Reading the codebase

Start with [the offline demo](../examples/paper_trading_demo.py). It constructs a complete paper session and follows orders through approval, execution, cancellation and reporting without network access. [Paper trading usage](PAPER_TRADING.md) covers running sessions; [risk manager usage](RISK_MANAGER.md) covers sizing and account rules.

## Follow one paper order

1. `PaperSession.submit()` records the intent and passes it to `OrderCoordinator.submit()`.
2. The coordinator selects the portfolio, asks `RiskManager.evaluate()` for approval, and reserves cash or inventory in `TradingState`.
3. `PaperTradingEngine.enqueue()` creates a pending order. The scheduler runs dispatch and arrival after the configured delays.
4. `_arrive()` checks current market rules, self-trade restrictions and available depth. It fills eligible quantity or puts the remainder in a passive queue.
5. `_preflight()` prepares fees without changing the working order. `_fill()` commits an execution and schedules a cumulative report.
6. The coordinator applies that report to the shared portfolio ledger. The engine's effective order can be ahead of the account's observed order because private reports take time to arrive.
7. `Reporter` reads execution and account state to produce diagnostics. Reporting never supplies data to matching or risk decisions.

Cancellation follows the same timing model: queuing a request does not confirm cancellation. A terminal report preserves any fills that occurred first.

## Where responsibilities live

| Area | Start here | Responsibility |
| --- | --- | --- |
| Shared data | [classes.py](../src/utilities/classes.py), [market_events.py](../src/utilities/market_events.py) | Intents, approvals, orders, portfolios and public-event provenance |
| Risk | [risk_manager.py](../src/market_decisions/risk_manager.py) | Validate intents, size entries/reductions and enforce budgets |
| Submission | [order_coordinator.py](../src/market_decisions/order_coordinator.py) | Route requests, reserve resources, apply reports and reconcile live accounts |
| Account state | [trading_state.py](../src/utilities/trading_state.py) | Own cash, positions, attribution and cumulative-fill accounting |
| Paper execution | [paper_trading_engine.py](../src/paper_trading/paper_trading_engine.py) | Own working orders, arrival/fill/cancel lifecycle and batch queue rollback |
| Matching | [orderbook.py](../src/paper_trading/orderbook.py), [matching.py](../src/paper_trading/matching.py) | Shared depth consumption, price-level allocations and passive queues |
| Fees and time | [fees.py](../src/paper_trading/fees.py), [scheduler.py](../src/paper_trading/scheduler.py) | Fee calculation/reserves and ordered callbacks |
| Sessions | [session.py](../src/paper_trading/session.py), [live.py](../src/paper_trading/live.py), [replay.py](../src/paper_trading/replay.py) | Compose the same engine with live data or recorded inputs |
| Public adapters | [public_market_data.py](../src/api/interfaces/public_market_data.py), [kalshi.py](../src/api/interfaces/kalshi.py), [polymarket_us_public.py](../src/api/interfaces/polymarket_us_public.py) | Normalize payloads and collect public market feeds |
| Live US account adapter | [polymarket_US.py](../src/api/interfaces/polymarket_US.py), [polyus_rest_api.py](../src/api/polymarket/polyus_rest_api.py) | Translate real account/order operations and SDK responses |
| Output | [recording.py](../src/paper_trading/recording.py), [reporting.py](../src/paper_trading/reporting.py) | Preserve replay inputs and export execution/account diagnostics |

`main.py` and `src/bot.py` remain the production-loop scaffold. `src/strategies/base_strategy.py` defines the strategy interface; it does not schedule strategies itself.

## Details that matter when changing code

- An `OrderIntent` expresses a strategy's request. An `ApprovedOrder` freezes risk-approved size, price bound and commitment. An `OrderRecord` describes the observed cumulative outcome.
- `WorkingOrder.phase` tracks the simulator's lifecycle. `OrderRecord.status` tracks the order state delivered to the account. Keep their timing distinct.
- The matching pool uses YES-denominated prices. NO prices use the complement and the corresponding opposite book side. Both views consume the same liquidity.
- Matching and fees use `Decimal`. Legacy account/public interfaces retain floats; exact cumulative notional accompanies paper reports so cash uses the executed decimal amount.
- `BookPool.executable()` returns a blocking reason, or an empty string for usable depth. A truthy result means execution is blocked.
- Scheduler order is timestamp, priority, then insertion sequence. Among work queued for one advance, equal-time public events precede cancels/expiry, arrivals, and private reports. Recorded driver advances preserve those boundaries; later inputs cannot overtake completed callbacks.
- The coordinator and state locks protect reservation/account updates. Timer delays and network calls run outside the state lock. `TradingState` synchronous helpers prefixed with `_` expect their caller to hold that lock.
- Revisions, cumulative counters and stable execution IDs prevent duplicate or late reports from applying fills twice. Preserve those checks during refactors.

## Verify behavior

```bash
venv/bin/python -m pytest -q
venv/bin/python -m examples.paper_trading_demo --output /tmp/paper-demo
venv/bin/python -m src.paper_trading.session replay --capture /tmp/paper-demo --output /tmp/paper-replay
```

Use fresh output directories. Execution logs and account results should match for the same captured inputs and configuration. Wall-clock processing measurements naturally vary between runs.
