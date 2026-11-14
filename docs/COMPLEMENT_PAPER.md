# Immediate complement paper strategy

This Bitcoin 15-minute experiment runs through the existing paper bot. It discovers Kalshi `KXBTC15M` and Polymarket US Bitcoin series `375`, checks their normal terms, and rolls through eligible windows automatically. No real orders are placed. Resting orders, dip entries, commodities, and same-venue netting experiments are outside this version.

## Run an offline proof first

From the repository root, using the installed project environment:

```bash
venv/bin/python -m examples.complement_smoke --output /tmp/complement-proof --scenario rolling_daily --windows 3
```

Use a new output path for every run. This deterministic fixture uses the registered strategy, standard example fee models, nonzero independent delays, real risk manager, matching engine, settlement ledger, reports, and recorded replay. It creates `capture/` and `replay/` below the output path.

The three-window scenario fills a pair, delays its US settlement into a skipped window, then fails a hedge and crosses the daily stop. Its $50 starting cash ends at $44.75 in the synthetic scenario. That deliberate loss verifies recovery costs and the daily limit; it is not a prediction of market returns.

Other fixture scenarios are `paired`, `recovery`, `no_trade`, `neither`, `stale`, `rolling`, and `daily_stop`. Use `--windows 1` for individual cases and `--windows 2` for the daily stop plus a later rejected entry. A one-pair `paired` case ends at $50.06; its 10-cent gross edge pays 4 cents of entry fees. The `recovery` case ends at $49.95 after paying entry and exit fees. Every scenario verifies its account/report results against recorded replay.

## Start public-market paper trading

[examples/complement_config.example.json](../examples/complement_config.example.json) is a runnable template. It needs no per-window market IDs. Copy it outside the repository to customize settings; keep secret values out of that JSON file.

The existing CLI loads `.env` when starting public feeds. The example names these variables:

| Variable | Existing feed input |
| --- | --- |
| `POLY_KEY_ID` | Polymarket US public-stream key ID |
| `POLY_SECRET_KEY` | Polymarket US stream signing secret |
| `KALSHI_KEY_ID` | Kalshi public-stream key ID |
| `KALSHI_SECRET_KEY` | Existing Kalshi signing-key material or key-file input |

Supply them through the environment or an ignored local `.env`, using your existing credential setup. Do not put credentials into strategy settings, captures, Git, or chat. This task adds no private-endpoint certification or private-order test requirement. Authentication is used for the existing public market streams.

```bash
venv/bin/python main.py run \
  --config examples/complement_config.example.json \
  --output /tmp/complement-live-paper \
  --duration 3600
```

Choose a fresh output directory. An hour allows at least two quarter-hour transitions. Press Ctrl-C once for bounded cancellation/report delivery and an attempt to reduce known excess; a second interrupt forces cancellation. Missing liquidity or uncertain reports can leave inventory or an incomplete run, which must remain visible.

For observation, use a copy with `strategies: []` and `runtime.observe_only: true`; keep `rolling` and both feeds. Observation records discovered markets and public inputs without submitting paper entries.

## Default settings and assumptions

- $25 paper cash per venue, funded independently. Unsettled payouts and reserves cannot finance another entry.
- Immediate FOK purchases of equal quantities, with separate arrivals and reports. Each matched window gets at most one entry attempt in this version. Both cross-venue orientations are considered.
- At most $2 per pair attempt and two whole-contract candidates (risk sizing and exits retain native fractional precision); at least 2 cents of conservative profit per pair after entry fee reserves. Actual risk sizing rechecks economics after rounding/resizing.
- Three-second maximum book age, one-second cross-venue observation skew, and no new entry in the last 30 seconds of a window. Freshness uses the existing runtime's local receipt/availability semantics; the source timestamp and its quality remain in captures.
- A one-second entry deadline, five seconds for excess recovery, and at most three strategy recovery attempts. Recovery sells confirmed excess with reduce-only IOC orders. Pending exits prevent duplicate sales. Unresolved excess blocks later-window entries until resolved, including by authoritative settlement.
- A central 10% daily loss stop, configurable within 10–15%. The risk manager uses owned cash plus fresh executable liquidation value after fee allowances. Fresh unsellable remainder is valued at zero and flagged incomplete; stale or incomplete valuation forbids new entries. The stop latches for the UTC day, requests entry cancellations, and permits reductions. Gaps and delayed fills can exceed the threshold; overshoot is reported. When midnight valuation is incomplete, entry stays paused and a later baseline uses the larger of the last complete value and first complete value, so a quote gap cannot erase a loss. High-water drawdown is a separate existing setting.
- Confirmed matched quantities are held for settlement and excluded from single-leg stop-loss sales. Unmatched excess receives no exemption.
- Explicit example fee profiles: Polymarket US retail quadratic coefficient `.095`, Kalshi quadratic `.07`, cent balance precision, and no assumed rebates. US metadata that exceeds the configured coefficient blocks entry. The existing Kalshi fee-change feed freezes affected markets. These are reviewable example account/market assumptions; edit profiles if your applicable schedule differs.
- Nonzero example latency and processing delays are unmeasured assumptions. Change them to explore sensitivity; synthetic success does not calibrate execution or establish a public-market edge.

Discovery compares the asset, BRTI reference, 60-price minute-average calculation, two-decimal rounding, inclusive Up/YES mapping, exact UTC interval, and opening threshold. Unknown or conflicting terms block entry. A shared reference source is the normal-settlement premise. Each exchange's actual payout is credited separately and only once; expiry alone never credits an assumed combined dollar.

Prepared, active, and older obligated markets share the configured market limit. Old holdings, fills, reports, and settlement monitoring survive rollover. Completed windows are archived before retirement. Capacity exhaustion pauses new entries rather than discarding old positions. Process restart/resume is outside this version.

## Read the results

`summary.json` contains:

- `accounts.PAPER`: actual cash, realized P&L, available liquidation estimates, fees, reserves, open inventory, and the daily baseline/halt/loss/overshoot/completeness flag.
- `complement`: attempted, paired, rejected, failed and unfilled groups; projected surplus at approved price/fee bounds; actual realized portfolio P&L; entry/recovery fees and recovery proceeds; unhedged duration; held capital; unresolved positions; per-venue available/reserved cash; daily-stop counts; activation/retirement counts; and decision/skip reasons.
- `history.details`: paths, record counts, and hashes for complete detail streams. `groups.jsonl` includes group/window attribution and complement control/rejection rows. `executions.jsonl` includes executions and corrections; use its `kind` field when reading it.
- `runtime`: readiness, shutdown result, and whether the capture completed cleanly.

Projected surplus includes approved attempts even when the pair later fails or never fills. It assumes normal complementary settlement. Actual P&L includes the loss from failed hedges. Discovery match observations count each poll; activated-window counts are unique. A no-trade run is valid: inspect decision reasons, discovery skips, and readiness in `runtime.log`.

Replay a completed capture into a different new directory:

```bash
venv/bin/python main.py replay \
  --capture /tmp/complement-live-paper \
  --output /tmp/complement-live-paper-replay
```

Replay uses captured inputs and decisions with no network reads. It cannot invent liquidity after the recording ends or reconstruct unrecorded historical books. Incomplete captures are rejected.

## Verification and operational evidence

See [tasks/todo.md](../tasks/todo.md) for the acceptance checks and [COMPLEMENT_VERIFICATION.md](COMPLEMENT_VERIFICATION.md) for the exact build/run evidence and any remaining operational limitation. Synthetic acceptance tests show accounting and lifecycle behavior, not the existence of a tradable public edge.
