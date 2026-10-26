# Realistic paper trading

Paper execution uses one shared engine for Polymarket US and Kalshi. Orders pass through the existing risk manager, reserve funds or inventory, arrive after configured delays, consume available depth or join a conservative queue, and update the existing portfolio ledger through delayed reports. Adding another venue requires a public-data adapter and verified venue rules; it does not require a second matching engine.

The implementation has offline regression coverage and a deterministic two-venue example. Authenticated live-feed capture and empirical execution calibration have **not** been verified. Example latencies, request budgets, precision overrides, and starting balances are assumptions. The production bot loop remains separate.

## Run the offline example

From the repository root, using the project's virtual environment:

```bash
venv/bin/python -m examples.paper_trading_demo --output /tmp/paper-demo
venv/bin/python -m src.paper_trading.session replay --capture /tmp/paper-demo --output /tmp/paper-replay
venv/bin/python -m pytest -q
```

Use new output directories for each run. Capture and audit writers refuse to overwrite existing files. The example makes no network requests. It demonstrates depth walking, a partial resting fill, a delayed cancellation, and linked orders where one venue fills and the other fails. `examples.risk_manager_demo` runs the same example.

Artifacts include:

| File | Contents |
| --- | --- |
| `manifest.json` | Versioned configuration, seed, code revision, input digest/count, start/end boundaries and finalization status |
| `inputs.jsonl` | Normalized public inputs, metadata, intents/batches, cancels and driver advances, including which public callback produced each strategy decision |
| `audit.jsonl` | Decisions, dispatch, source revisions, queue changes, executions, timing, data-quality and participation diagnostics |
| `executions.jsonl` | Exact decimal execution prices/quantities/notional, formula/rounding/refund fee breakdowns, credits, liquidity role, fee version and benchmarks |
| `summary.json` | Fill outcomes, cash/equity, gross/net P&L, fees/credits, reserves, incomplete inventory liquidation, markouts and diagnostics |

Replay verifies input integrity and streams the capture through the same engine. It uses a virtual clock and does not sleep through historical time. Schema 2 records driver advances so equal-time inputs retain their original causal order. Public inputs queued together precede cancellations/expiry, arrivals, and private reports within that advance; inputs published after an advance cannot overtake work it already completed. Recorded strategy decisions run inside their original public callbacks. Pending arrivals and resting orders receive no fabricated fills after the dataset ends, including arrivals queued but never processed at the final timestamp. Already determined private reports can finish delivering; remaining commitments and inventory appear in the summary.

Schema 1 captures remain readable using recorded file order. They lack driver boundaries and decision origins, so the exact batching of equal-time inputs cannot always be reconstructed from those older files.

## Configure read-only live data and capture

Copy [the example configuration](../examples/paper_config.example.json) to a local file. Replace the market slug/ticker placeholders and verify each subscribed market's price grid, quantity quantum/minimum, category, resolution terms and account fee profile. If required metadata is absent, provide explicit `feeds.<exchange>.market_rules` values. Do not infer fractional contract precision from a displayed quote. `market_overrides` uses canonical `EXCHANGE:market_id` keys; US slugs are resolved to canonical IDs by the adapter.

Each venue profile explicitly sets maximum book age, outbound/acknowledgement/report/cancel delays, operation costs and budgets, supported instructions, outcome-netting restrictions and effective-dated fees. `processing_delay` adds simulated decision/risk delay before dispatch. `Delay` supports a seeded uniform jitter around its base. These parameters must come from measurements before making claims about fast-trading performance.

Set credentials through the environment:

```bash
export POLYMARKET_US_KEY_ID='your-key-id'
export POLYMARKET_US_SECRET_KEY='your-secret-key'
export KALSHI_API_KEY_ID='your-key-id'
export KALSHI_PRIVATE_KEY_PATH='/absolute/path/to/kalshi-private-key.pem'
venv/bin/python -m src.paper_trading.session capture --config local-paper.json --output /tmp/paper-capture --duration 60
```

The venues require authentication for these public websocket connections. This startup opens market-data subscriptions and public REST reads; it does not start live-account reconciliation, private order subscriptions, or real order submission. Credentials and headers are excluded from capture settings. The US adapter uses the installed SDK; Kalshi supports RSA-PSS and Ed25519 signing.

`live` accepts the same flags as `capture`. Both CLI commands collect public data and drive the paper engine. They do not select or discover a trading strategy automatically. Attach a strategy through the Python session API for live paper decisions. `--metadata-interval` controls periodic metadata/settlement checks (default 30 seconds); per-feed `read_interval` spaces REST requests. Missing verified depth from either selected venue causes the CLI to fail readiness validation.

Sequence gaps, malformed books, disconnects and queue overflow stop unsafe execution. Reconnection requires fresh depth and resets passive priority conservatively. A Kalshi fee-change notification freezes affected market execution; refresh the verified fee profile and restart the session rather than continuing with an obsolete rate. A close/resolution timestamp alone never supplies a settlement payout. Only authoritative final payout terms close positions and credit cash. Public settlement stops further market execution immediately; account settlement waits for preceding private reports so it cannot reveal fills before their delivery time.

## Strategy and order API

For live composition, load the explicit configuration, construct `PaperSession(config, RealClock(), capture_directory)`, attach `session.strategy`, and pass `create_feeds(feed_settings)` to `run_live()`. Strategies implement async `on_snapshot(snapshot)` and return a list of `OrderIntent` objects. They are called after public market updates have been applied atomically. Transport connection events do not trigger fresh decisions.

Use `session.new_intent(...)` to obtain the session clock's `created_at` and a deterministic intent ID. Use `session.random` for strategy randomness. Strategies must also avoid wall-clock reads, unseeded randomness, network lookups, and mutable state reused between runs if reproducibility is required. Supply the actual market's outcome probability and resolution date to the existing entry-risk model; reduce-only exits retain their normal inventory checks.

```python
result = await session.submit(intent)
if result.status == "pending":
    order_id = result.submission_ids[0]
    # Result indicates a queued submission; inspect later snapshots for fills.

queued = await session.cancel(order_id, intent.exchange, portfolio_id="PAPER")
# True means the cancel request was queued. Fills can occur before confirmation.
```

Paper submissions now return `status="pending"`, identifiable `submission_ids` and a risk decision. An accepted submission can subsequently be rejected at arrival. Reservations remain until observed cumulative updates transfer or release them. `open_orders` contains observed active orders; `order_history` retains terminal records. Stable revisions prevent late acknowledgements from undoing newer fills. Lost acknowledgements can be resolved with `coordinator.resolve_paper_submission(order_id)`. Report-delivery retries reuse the existing execution rather than rematching liquidity.

For virtual sessions, call `await session.advance(timestamp)` to process scheduled work. For live sessions, `run_live()` pumps timers independently of incoming messages. Submit through the session/coordinator so risk, capture and attribution are retained. `session.replace_order(old_order_id, new_intent)` requires observed cancellation or expiry; the replacement receives a fresh approval and loses queue priority.

Linked orders use `execution_policy="paper_independent_legs"`, `dispatch_policy="concurrent"` or `"sequential"`, and an explicit `execution_deadline`. Each leg experiences its venue's delays and liquidity. Failed/deadline-expired groups request cancellation of remaining legs; completed fills remain in the account and need separate risk-approved reductions. Provide `market_equivalence` metadata when claiming contracts hedge each other. The realistic engine rejects `paper_all_or_none`.

Decision replay uses the captured intents/cancels by default. To evaluate a fresh strategy against the same market inputs:

```python
session, summary = await replay(
    capture_directory,
    output_directory,
    strategy_factory=lambda context: MyStrategy(context),
)
```

The factory receives a new session with controlled clock, IDs and independent seeded strategy RNG. Strategy replay skips recorded intents, batches and cancellations. An optional `execution_config` override supports assumption sweeps; results from different assumptions should be compared explicitly.

## Execution assumptions and limits

- Takers walk captured eligible depth at arrival. IOC keeps partial fills; FOK commits the full quantity or zero. Bounded market orders enforce slippage limits. Variable price ladders accept verified limit prices; bounded MARKET orders on those ladders are currently rejected.
- YES/NO aliases share one canonical liquidity pool. Local fills consume a shared private overlay; repeated identical snapshots do not restore consumed volume. Only net observed replenishment becomes available. Public prints quarantine affected taker levels until refreshed depth resolves uncertainty.
- Resting orders join behind visible same-price external volume and earlier local orders. Only verified, deduplicated aggressive prints at that price advance the queue. Touches, cancellations, trade-through, hidden liquidity and missing trade IDs cannot manufacture maker fills. Missing reliable identities pause affected matching until a valid recovery boundary. This can underfill; it does not guarantee conservative strategy P&L.
- Actual fees are calculated separately from quantity-aware risk reserves. Profiles support US quadratic fees/rounding and Kalshi model rounding, balance alignment and fractional refunds, plus maker fees/credits and per-market overrides. Verify effective dates, account precision and fee tiers. References: [Polymarket US fees](https://docs.polymarket.us/fees), [Kalshi fee rounding](https://docs.kalshi.com/getting_started/fee_rounding).
- `participation_alert` flags a high ratio of taker quantity to displayed eligible depth, or maker quantity to the eligible public print. This is a diagnostic, not an empirical market-impact model. Counterparty fragmentation and hidden liquidity remain unknown.
- Timing is based on data received and available to the collector. Source timestamps and receipt-only timestamps remain distinguishable. This model cannot reconstruct unreceived exchange events or promise real queue position. Measure live transport and processing behavior before using it for HFT conclusions.
- Liquidation reports show only depth-covered quantity, after estimated exit fees; insufficient/stale depth or unavailable fees produce an incomplete/unavailable estimate. Markouts require actual later book evidence and never influence earlier fills.
- Collector/scheduler/report queues and the in-memory audit window are bounded. Full inputs/audits stream to disk; order, execution, trade-deduplication and markout histories grow with session activity. The synthetic book-only benchmark does not establish a constant-memory bound for arbitrarily long trading sessions.
- Shutdown finalizes the capture and retains unresolved capital/inventory in reports. There is no durable process-resume mechanism, native amendment model, live Kalshi order submission, automatic hedge/unwind, or production strategy orchestration. Advanced calibrated queues, impact and latency reconstruction remain in [the research notes](../notes.md#future-paper-trading-research).

## Validate adapters and measure performance

Offline tests cover both public payload formats, decimal/complement rules, fee examples, queue/depth competition, cancel races, stale reports, lost acknowledgements, funding, settlements and replay. Adapter fixtures are synthetic payloads shaped to official schemas. They are not authenticated exchange captures.

Run a read-only capture with real credentials and selected markets to verify transport/schema compatibility. Replay that dataset, examine diagnostics and obtain representative execution evidence before calibrating timing or assessing accuracy. No real-money validation trades are part of this implementation.

```bash
venv/bin/python -m examples.paper_trading_benchmark --events 2000 --output /tmp/paper-benchmark
```

This command records/replays a synthetic two-venue full-book workload, measures throughput and traced memory, and writes `benchmark.json`. It intentionally has no strategy orders or network traffic. Throughput varies by machine and instrumentation; a real workload with trades, strategies and long-lived inventory is still needed to choose production targets.

`processing_wall_time` reports scheduler callback wall-time percentiles over a bounded recent sample and the maximum across the run. These diagnostics include Python callback/await work; they do not alter virtual time, represent measured exchange latency, or promise original live CPU jitter will be reproduced. Configured processing delay remains an explicit simulation assumption.
