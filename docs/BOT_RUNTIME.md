---
type: feature
title: Paper bot runtime
feature: bot-loop-v1
description: Run supervised paper strategies, observe public feeds and replay complete local captures
status: current
code:
  - main.py
  - src/bot.py
  - src/paper_trading/runtime.py
  - src/paper_trading/runtime_config.py
  - src/paper_trading/history.py
  - src/paper_trading/replay.py
  - src/strategies/context.py
---
# Paper bot runtime

## Purpose and goal contribution

The Python runner combines Polymarket US and Kalshi public data, snapshot-based
strategies, periodic risk checks and one paper account ledger. It supports explicit
observation without a strategy and an offline smoke strategy. Actual trading
algorithms and real-money execution are outside this runtime's scope.

## Relationships

The [simulator guide](PAPER_TRADING.md) explains execution, fees and existing
session APIs. [Code ownership](CODE_GUIDE.md) explains the coordinator and ledger.

## How it works

### Run offline

From the repository root, use new output directories:

```bash
venv/bin/python main.py smoke --output /tmp/bot-smoke
venv/bin/python main.py replay --capture /tmp/bot-smoke --output /tmp/bot-replay
venv/bin/python -m pytest -q
```

Smoke uses finite synthetic data for both venues through the actual runner,
risk manager, coordinator, matching engine and accounting. Its factory,
`smoke-v1`, is restricted to synthetic operation. It makes no network requests
and needs no credentials. Its prices, fees, limits and timings are uncalibrated
fixtures. Replay consumes recorded decisions without importing their factories.

Exit codes are 0 for verified normal completion, 2 for configuration errors, and
1 for runtime or incomplete-cleanup failure. Existing output is never overwritten.
Keep partial artifacts after failures; use a new output directory for another run.

## Observe configured public markets

Copy [the observation example](../examples/bot_config.example.json) to a local
configuration. Replace both market placeholders and verify the venue fee
profiles, price grid, quantity step/minimum, resolution terms and timing settings.
The example deliberately has no enabled strategies and uses synthetic, unmeasured
execution parameters; it is not a calibrated deployment configuration.

```bash
venv/bin/python main.py run --config local-bot.json --output /tmp/bot-observation --observe-only --duration 60
```

The public websocket connections require venue credentials. `run` reads the
local `.env` file without overriding existing environment variables. You can also
set the configured credential environment variables directly:

| Venue | Default environment variables |
| --- | --- |
| Polymarket US | `POLYMARKET_US_KEY_ID`, `POLYMARKET_US_SECRET_KEY` |
| Kalshi | `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH` |

Feed configuration can select alternative environment variable names. Captures
store those names, not resolved credentials, authentication headers or key files.
The observation example selects `POLY_KEY_ID` and `POLY_SECRET_KEY` for
Polymarket US, and `KALSHI_KEY_ID` and `KALSHI_SECRET_KEY` for Kalshi. Kalshi's
`private_key_env` selects a variable containing a key-file path or private-key
material: PEM (including escaped newlines), or base64-encoded PEM/DER. Key parsing
and signing happen in memory; no temporary key file is created.
The runner creates market-data clients only; it does not bootstrap a live account,
subscribe to private orders or place real orders. Offline smoke/replay do not
load `.env`.

Public subscriptions are fixed for a run. The US adapter resolves slugs to
canonical market IDs; Kalshi uses tickers. All initial subscriptions need verified
metadata and full executable depth before strategies activate. Later stale,
disconnected or incomplete data pauses affected strategies until verified recovery.
Unrelated ready strategies may continue. Transport reconnection alone does not
establish readiness. See the official [US market websocket contract](https://docs.polymarket.us/api-reference/websocket/markets)
and [Kalshi orderbook contract](https://docs.kalshi.com/websockets/orderbook-updates).

## Configure strategies and limits

The existing `strategy_portfolios`, `starting_balances`, `risk`, `execution`,
`feeds` and optional `market_overrides` sections remain. Enabled strategy routes
must explicitly select a funded `PAPER` portfolio. Observation requires public
source and no enabled strategies; ordinary execution requires an enabled,
code-registered factory. Config is never evaluated as Python or dynamically
imported as a plugin.

Each strategy entry contains `name`, `factory`, `enabled`, `required_markets`
and JSON-object `settings`. Required identities use `EXCHANGE:market_id` strings
matching the configured subscriptions. Names are unique. Different names can
use one factory with independent settings.

`runtime` contains `source` (`public` or `synthetic`), `observe_only`,
`startup_timeout_seconds`, `strategy_timeout_seconds`, `risk_interval_seconds`,
`shutdown_grace_seconds` and `limits`. Timings must be finite and positive.

| Limit | Controlled state |
| --- | --- |
| `max_strategies`, `max_markets` | Strategy and subscription cardinality |
| `max_active_orders`, `max_positions` | Active account obligations and holdings |
| `max_intents_per_callback` | Whole callback-result admission |
| `max_hot_history_records` | Working terminal-history window, with finite active/cleanup headroom |
| `max_archive_cache_records` | Historical lookup cache |
| `max_event_bytes` | Transport/capture/normalized input and settings size |
| `memory_budget_bytes` | Working-state admission, copies and cleanup headroom |

Execution queues/outboxes also retain their existing limits. Oversized inputs,
capacity exhaustion and failed archival stop new work visibly. They do not discard
inventory, release uncertain commitments or liquidate positions to free space.
Public transports bound reads before JSON decoding, refuse compressed HTTP bodies
on the bounded path and cap accumulating depth/normalizer state. The bounded US
transport is tied to the installed `polymarket-us==0.1.2` contract.

These controls do not sandbox arbitrary Python. Strategy code must remain
cooperative and bounded. Deployment memory/throughput claims require measurements
on the selected workload and environment, including native allocations and report/
replay peaks. A passing finite test or a small returned summary alone is insufficient.

## Extend a strategy

A factory receives an isolated context and returns an object implementing
`async on_snapshot(snapshot)`. The context exposes `strategy_name`,
`portfolio_id`, `settings`, `now()`, seeded `random` and `new_intent(**values)`.
It exposes no mutable coordinator or account. Use the context's time, randomness
and IDs for deterministic replay.

```python
from src.bot import BotRunner, load_bot_config

# make_strategy is application code implementing the agreed callback contract.
runner = BotRunner(
    load_bot_config("local-bot.json"),
    output_directory="/tmp/my-paper-run",
    strategy_factories={"my-factory": make_strategy},
)
summary = await runner.run(duration=60)
```

Register factories in application code; configure their names and paper routes
in the JSON file. The bundled CLI registry contains only the synthetic smoke
factory. Add a real strategy deliberately before attempting ordinary public-mode
execution; observation remains available without one.

Callbacks return `None` or a bounded list of `OrderIntent`. Use
`context.new_intent(...)` for the configured identity and deterministic metadata.
The entire result is validated before submitting any intent. The coordinator then
performs fresh risk/reservation checks in returned order.

Each strategy has one active callback and one latest pending notification.
Intermediate redundant notifications coalesce; market inputs and execution
reports are retained. Snapshots and settings are independent copies. Mutating one
does not mutate another strategy, live accounting or original replay settings.
Cooperative callback waits do not suspend execution timers or periodic risk.
Exceptions, timeouts and invalid results disable the strategy and cancel its
outstanding paper orders. Healthy strategies continue; losing every enabled
strategy fails the session. Results from a disabled callback cannot act.

## Stop, archival and artifacts

The first SIGINT or `runner.request_stop()` begins ordinary stop: cease new
dispatch, abort provably unsent work, request paper cancellations and process
necessary inputs/timers/reports during finite grace. A fill can win a cancel race.
Held positions remain. Unknown or dispatched obligations retain their commitments.
Unresolved cleanup is a failure. A second interrupt can force cancellation and
must leave capture finalization false.

A finite synthetic/replay boundary delivers already determined reports without
inventing future market liquidity, payouts or cancellations. Its remaining
orders/positions are reported honestly.

The new runner retains bounded active/hot state and archives completed history
locally using standard-library SQLite. Commit precedes eviction; indexed evidence
supports old duplicates and late cumulative/cost corrections. The archive is
history, not a second account ledger or an automatic restart journal. Disk use
grows, and no automatic deletion or process resume is provided.

| Artifact | Purpose |
| --- | --- |
| `manifest.json`, `inputs.jsonl` | Schema-3 configuration, integrity metadata, causal public/control/decision records |
| `history.sqlite3` | Indexed complete history, correction/dedup evidence and pending historical work |
| `audit.jsonl`, `runtime.log` | Execution and operational diagnostics |
| `executions.jsonl` | Complete executions and explicit correction rows |
| `markouts.jsonl` | Completed or explicitly missing markouts |
| `order_timings.jsonl`, `groups.jsonl` | Complete timing and group records |
| `summary.json`, `outcome.json` | Bounded aggregate account/execution totals and runtime/cleanup status |

`summary["history"]` declares `scope: "full-session"` and references the four
detail streams by relative path, kind and written-row count. Correction rows count
as detail rows without counting as additional executions. Working snapshots
contain active state plus the configured hot history; use paged archive/history
artifacts for complete detail. The runner summary never returns growing per-order
or per-fill arrays. Legacy paper commands retain their established summary shape.

History export schema 2 defines `accounting_order` as a one-based ordinal within
each detail stream, including correction rows. It is not a cross-stream clock.
SQLite's global record sequence remains the internal archive paging key. This
keeps detail hashes independent of unrelated stream flush timing; rollback and
retry retain transactional sequence behavior. Older history schema 1 exports
used global SQLite row IDs and could have different detail hashes after replay
despite identical account and execution data. Existing capture input schemas
remain readable, but replay emits the current history format: schema-1 history
hashes are not byte-compatibility promises for a schema-2 replay. Original
captures and their history artifacts remain read-only.

Schema-3 replay preserves original named settings, controlled time/IDs/RNG and
recorded dispatch, coalescing, completion, risk and shutdown order. The public
`replay_bot(capture_directory, output_directory)` uses recorded decisions. The
private test seam also exercises deterministic fresh strategies with different
playback pacing. Older schema-1/2 captures keep their original causal limitations;
the new facade applies a finite compatibility profile and fails on excess capacity.
Original captures are read-only.

## Decisions and limitations

Measure increasing workloads in fresh processes, using new output directories:

```bash
venv/bin/python -m examples.bot_trading_benchmark --cycles 1000 --output /tmp/bot-memory-1000
venv/bin/python -m examples.bot_trading_benchmark --workload stress --cycles 12 --output /tmp/bot-stress-12
```

The benchmark executes two-venue round trips, reliable duplicate prints, archival,
summary construction and recorded replay. It records Python allocations, Linux
RSS/peak RSS, retained-state high water, archive growth and elapsed time.

The stress workload uses a serialized diagnostic driver through the actual runner,
coordinator, engine and ledger. It combines two-venue grouped entries, shallow
IOC partial fills, resting-order cancellations, delayed reports, retained
inventory, old duplicate IDs, archived cumulative corrections and markouts.
The diagnostic correction input is captured before accounting and reproduced
during replay; registered strategies receive no report-injection capability.
Measurements include correction/query/report/replay phases, native residency,
bounded container high water and timer backlog. The workload is a test harness;
its synthetic decisions and execution parameters are not a trading strategy.
Use the corresponding fault integration tests as well. Neither benchmark alone
establishes arbitrary-duration memory bounds or empirical execution calibration.

Authenticated public-feed capture for both selected markets, observed lifecycle/
settlement coverage, effective fee/precision verification and representative
long-run deployment measurements remain separate release evidence. Synthetic
fixtures are not authenticated capture or empirical calibration. Real trading
algorithms, real-money execution, Rust UI and automatic crash/restart recovery
remain future work.
