# Complement paper implementation verification

Checked 2026-11-14 on `strategy/cross-exchange-complement-arbitrage`.

The software implementation is complete. The remaining operational acceptance item is a public-feed run across two actual window transitions, which is blocked by missing existing feed credentials. This report does not claim a live edge or a successful authenticated feed run.

## Software checks

- Full default-order regression: **493 passed in 86.27 seconds**. Log: `/tmp/complement-final-tests.log`.
- Source compilation: `venv/bin/python -m compileall -q src main.py examples/complement_smoke.py`, exit 0.
- Existing `main.py smoke` and `main.py replay`: exit 0, two orders, equal accounts, submitted count, and fees. Artifacts: `/tmp/complement-build-existing-smoke` and `/tmp/complement-build-existing-replay`.
- No tests, assertions, timing limits, memory limits, or existing quality thresholds were disabled or lowered.

The test process used the normal collection order in a child shell process. Running Python as the tool launcher's final command inherited a roughly 403 MB lifetime RSS high-water counter; a forked Python child began around 8 MB. This was a launcher accounting issue, not a reason to change memory thresholds. The initial test-order hypothesis was corrected in the audit trail.

One unchanged legacy retention test hit its callback deadline under `tracemalloc` during a full run. It then passed in isolation (19.65 seconds) and in the full rerun above. An earlier legacy stress detail-order mismatch also passed on isolated/full rechecks. The same retention deadline failure appeared in baseline investigation before runtime changes. These remain timing-sensitive verification observations; they were not hidden with retries in test code, skips, or relaxed limits. A separately reproduced rolling retirement ordering bug was fixed and has its own regression.

## Reproducible configured proof

```bash
venv/bin/python -m examples.complement_smoke \
  --output /tmp/complement-build-proof-final \
  --scenario rolling_daily --windows 3
```

Result: exit 0; capture finalized cleanly; recorded replay matched accounts, fees, submitted count, complement report, and history hashes.

| Observed item | Result |
| --- | --- |
| Windows traversed | 3 |
| Activated matched windows | 2; the middle window lacked its counterpart |
| Entry attempts | 2 |
| Completed pair / failed pair | 1 / 1 |
| Orders submitted | 5, including recovery |
| Daily stops | 1 |
| Ending Kalshi / US cash | $27.61 / $17.14 |
| Total ending cash | $44.75 from $50 |
| Entry fees / recovery fees | $0.88 / $0.02 |
| Recovery proceeds | $0.15 |
| Held capital / unresolved inventory | $0 / 0 positions |
| Largest observed daily-stop overshoot | $0.38 |

The first pair holds its US leg until authoritative settlement arrives in the skipped middle window. The final window deliberately fails its hedge and crosses the central daily stop. Loss is intentional acceptance evidence. Capture and replay are in `/tmp/complement-build-proof-final/capture` and `/tmp/complement-build-proof-final/replay`; the command output is `/tmp/complement-build-proof-final.log`.

Other deterministic integration scenarios cover a profitable fee-aware pair ($50.06 ending cash), recovery loss ($49.95), a gross-positive but fee-negative no-trade, both legs unfilled, stale receipt-time books, later-window rejection after the daily stop, and prevention of early entry in a prepared future window.

## Acceptance coverage

| Capability | Main regression files |
| --- | --- |
| Depth, grids, staleness/skew, fees and rounding | `tests/test_complement_quotes.py`, `tests/test_complement_precision.py` |
| Whole callback validation and linked gateway | `tests/test_complement_gateway.py`, existing bot loop/integration tests |
| Final risk sizing and fee-erased edge | `tests/test_complement_risk.py`, existing risk/order lifecycle tests |
| Independent fills, quiet recovery, pending exits, disable and callback limits | `tests/test_complement_recovery.py`, existing shutdown/recovery tests |
| Daily equality, fees, incomplete liquidation, UTC rollover and invalid/gap baselines | `tests/test_complement_daily_risk.py` |
| Matched exemptions, actual/differing/duplicate payouts | `tests/test_complement_settlement.py`, existing ledger tests |
| Official series, typed windows, threshold changes, deduplication and stale-context revocation | `tests/test_complement_discovery.py`, `tests/test_complement_rolling.py` |
| Retained obligations, capacity pause, durable retirement | `tests/test_complement_retention.py`, existing archive/retention tests |
| Recorded/fresh callback replay and full report equality | `tests/test_complement_replay.py`, `tests/test_complement_reporting.py` |
| Registered configured launch and complete scenarios | `tests/test_complement_cli.py`, `tests/test_complement_integration.py` |

The precision regression admits both saved real production book shapes through the real book validator. Native fractional book/fill increments are separate from conservative whole-contract candidate selection. Fee reserves bound native fragmentation. Relevant official references are [Kalshi orderbook responses](https://docs.kalshi.com/getting_started/orderbook_responses), [Kalshi fractional-share rule filing](https://www.cftc.gov/sites/default/files/filings/orgrules/26/01/rules01122636538.pdf), and [Polymarket US fractional contracts](https://docs.polymarket.us/learn/trading/basics/fractional-shares).

## Actual public startup attempt

```bash
venv/bin/python main.py run \
  --config examples/complement_config.example.json \
  --output /tmp/complement-live-paper-final-startup \
  --duration 3600
```

The final-code startup attempt exited 1 in about 0.32 seconds:

```text
Runtime failed: RuntimeError: Set the configured Polymarket US public-feed credential environment variables
```

All four configured variables were absent: `POLY_KEY_ID`, `POLY_SECRET_KEY`, `KALSHI_KEY_ID`, `KALSHI_SECRET_KEY`. Neither the strategy worktree nor the original checkout had a local `.env` file. Only variable/file presence was inspected; no secret values were printed or saved.

Observed live windows/transitions: **0 / 0**. Submitted orders: **0**. Cash stayed $25 per venue. Runtime status is failed and the capture is deliberately incomplete. This is a clear startup failure, not a completed live-paper smoke; the incomplete capture must not be used as a successful replay dataset. Log: `/tmp/complement-live-paper-final-startup.log`.

A separate read-only public REST discovery check earlier in this run found one current normal-terms match (`KXBTC15M-26NOV140145-45` / US `2502570`), with matching UTC boundaries and threshold. That proves the discovery request path returned an actual pair at that observation time. It does not replace the missing authenticated-stream/two-transition run or establish an executable edge. Its evidence is active-run transcript position 369.

Once the existing credential inputs are available, use the operating guide's public command with a new output directory for an hour, inspect readiness and both transitions, then replay the completed capture. No private-order endpoint test or historical backtest is a prerequisite.

## Review and audit

The implementation was reviewed against the approved spec and its failure cases. Review regressions corrected current-match revocation, refused-retirement archival timing, native fractional depth/fee precision, disabled-strategy recovery, successful-group deadline classification, recovery exposure duration, central-exit fee attribution, and daily baseline gaps.

The new launch registers only PAPER routes, uses the existing paper coordinator/engine, and creates public market-data transports. Existing tests guard the private-account boundary. No real-order client or private order-placement endpoint was exercised.

The append-only decision trail is `.audit/complement-build.tsv` in this worktree. Its test-count claims were checked against the retained logs, and early decisions were checked against the host's active-run transcript. The superseded RSS diagnosis remains visible with its correction. Some host transcript items are truncated; complete retained test logs and current committed regressions supply the verification evidence.

Self-review was performed with GPT-6.1 Sol. A review on a different model family could not run: the only configured alternative family provider reported that its Claude executable was not installed. This is not an independent review. The live-run limitation and timing-sensitive legacy tests above remain explicit review flags.
