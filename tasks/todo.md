# Runtime-access tasks

Plan: [plan.md](plan.md). Status: Approved; RA-00 in progress.
Every behavior task uses RED -> GREEN -> full regression -> compile -> review
-> commit. Also run fresh offline smoke/replay after runner, capture or CLI
changes. Preserve concrete test results in `.audit/` and log decisions in
`.audit/trading-bot-tui.tsv`. Acceptance IDs refer to SPEC-runtime-access.md.

## RA-00: Establish an executable baseline

- [ ] Restore Python environment and verify the unmodified application.
- Acceptance: Isolated Python 3.12 environment has the existing requirements;
  pip check, full pytest, compileall and fresh smoke/replay have real recorded
  results. No project dependencies or tests were removed to make setup pass.
- Verify: Commands in plan.md plus `python3 main.py smoke --output <fresh-dir>`
  and `python3 main.py replay --capture <smoke-dir> --output <new-dir>`.
- Dependencies: Approved plan.
- Files: Local environment and `.audit/` evidence; this todo status only.
- Scope: Setup; no application code. Coverage: baseline for RA14.

## RA-01: Pause strategy decisions without suspending account processing

- [ ] Add the runner control boundary and compatible replay behavior.
- Acceptance: Pause/resume is independent of readiness; late callback results
  cannot act across the pause barrier, including after resume. Existing fills
  and risk processing continue; stop wins, and uncooperative tasks fail within
  grace. Recorded and fresh-strategy replay remain consistent.
- Verify: `python3 -m pytest -q tests/test_bot_control.py tests/test_bot_replay_v1.py`
  plus the common task checks. Synchronize race tests with explicit barriers.
- Dependencies: RA-00.
- Files: src/paper_trading/runtime.py, src/paper_trading/replay.py,
  tests/test_bot_control.py, tests/test_bot_replay_v1.py.
- Scope: Medium. Coverage: RA06, RA07, RA08.

## RA-02: Attach to an idle private service

- [ ] Serve a real local overview without creating a bot session.
- Acceptance: Strict versioned/framed request validation works over a private
  Unix socket; idle overview is bounded and does not load credentials or start
  feeds. Reject foreign peers, insecure paths, existing sockets and malformed
  requests; cleanup cannot unlink a replacement entry.
- Verify: `python3 -m pytest -q tests/test_bot_service.py` plus common checks;
  use actual Unix sockets and filesystem permission/ownership assertions.
- Dependencies: RA-01.
- Files: src/operator/__init__.py, src/operator/protocol.py,
  src/operator/service.py, tests/test_bot_service.py.
- Scope: Medium. Coverage: RA01, RA10, RA13.

### Checkpoint: runner and idle attach boundary

- [ ] Full regression/compile and pause/replay checks pass; inspect the socket
  and runner ownership boundary before adding session controls.

## RA-03: Start and stop an independent paper session

- [ ] Route explicit lifecycle controls to one owned real runner.
- Acceptance: Concurrent start admits one run, using a new ID/output directory;
  session IDs reject stale controls. Stop uses existing cleanup, terminal state
  remains readable, and unresolved cleanup blocks replacement. Client detach
  has no effect on trading; pause/resume routes through RA-01's boundary.
- Verify: `python3 -m pytest -q tests/test_bot_service.py tests/test_bot_control.py`
  plus common checks. Use the real runner with bounded injected offline feeds.
- Dependencies: RA-02.
- Files: src/operator/service.py, src/operator/protocol.py,
  tests/test_bot_service.py, tests/test_bot_control.py.
- Scope: Medium. Coverage: RA02, RA03, RA07, RA09.

## RA-04: Read strategy and trade state from the real account

- [ ] Publish consistent copied overview and strategy DTOs.
- Acceptance: Multiple strategies, disabled/empty cases and partial orders,
  positions and fills match actual runtime/ledger state. PAPER is explicit;
  public data never implies LIVE. System activity is not misattributed, and
  private configuration/account objects do not escape.
- Verify: `python3 -m pytest -q tests/test_bot_runtime_access.py` plus common
  checks; compare observations to real fixture execution/accounting outcomes.
- Dependencies: RA-03.
- Files: src/operator/observations.py, src/operator/service.py,
  src/paper_trading/runtime.py, tests/test_bot_runtime_access.py.
- Scope: Medium. Coverage: RA04, RA05.

## RA-05: Page observations within finite memory limits

- [ ] Add session-bound collection cursors and enforce projection budgets.
- Acceptance: Every collection honors limits, ordering, scope and revision;
  foreign/malformed cursors fail explicitly. Repeated reads and session
  replacement remain bounded without evicting ledger obligations or scanning
  full capture history; incompatible identifiers/projections fail before start.
- Verify: `python3 -m pytest -q tests/test_bot_runtime_access.py tests/test_bot_service.py`
  plus common checks and reproducible retained-byte/count evidence.
- Dependencies: RA-04.
- Files: src/operator/protocol.py, src/operator/observations.py,
  src/operator/service.py, tests/test_bot_runtime_access.py,
  tests/test_bot_service.py.
- Scope: Medium. Coverage: RA04, RA10, RA12.

### Checkpoint: shared account observations

- [ ] Full suite/compile pass; trace controls and trade views to the one runner,
  confirm projection/queue high-water evidence and correct session identity.

## RA-06: Share bounded runtime events and diagnostic findings

- [ ] Expose recent events/fills and accept session-targeted plain-text findings.
- Acceptance: Bounded rings expose eviction metadata; findings have correct
  attribution and append-only artifact evidence. Artifact write failure rejects
  publication; redaction/terminal sanitization and text limits are tested.
  Findings cannot change readiness, accounting, configuration or code.
- Verify: `python3 -m pytest -q tests/test_bot_runtime_access.py tests/test_bot_service.py`
  plus common checks; exercise more records than each configured ring capacity.
- Dependencies: RA-05.
- Files: src/operator/observations.py, src/operator/service.py,
  src/paper_trading/runtime.py, tests/test_bot_runtime_access.py,
  tests/test_bot_service.py.
- Scope: Medium. Coverage: RA11, RA12.

## RA-07: Keep malformed and slow clients from starving the bot

- [ ] Complete backpressure, deadlines and simultaneous-client failure handling.
- Acceptance: Enforce request/response bytes, 8-client/32-queue caps, nesting,
  5-second deadlines and total projection/buffer budget. Slow or abusive local
  clients cannot prevent stop/risk progress. A timed-out control has explicit
  unknown outcome; a client restart never blindly retries a mutation.
- Verify: `python3 -m pytest -q tests/test_bot_service.py tests/test_bot_control.py`
  plus common checks. Record actual saturation high-water behavior and verify
  memory is released when clients or sessions are replaced.
- Dependencies: RA-06.
- Files: src/operator/protocol.py, src/operator/service.py,
  tests/test_bot_service.py, tests/test_bot_control.py.
- Scope: Medium. Coverage: RA09, RA10, RA12.

## RA-08: Launch serve explicitly and verify process lifetime

- [ ] Add the serve entrypoint, signals and real subprocess acceptance tests.
- Acceptance: Serve starts idle with mutually exclusive smoke/config modes;
  existing commands and exit codes remain compatible. Real subprocess tests
  detach/kill clients and reattach to the same active run, exercise service
  signals/replacement and preserve captures and honest cleanup status.
- Verify: `python3 -m pytest -q tests/test_bot_service.py tests/test_bot_cli_v1.py`
  plus common checks and fresh offline smoke/replay. Use controlled feeds/
  test harness synchronization for active-lifetime assertions, not timing luck.
- Dependencies: RA-07.
- Files: main.py, src/operator/service.py, tests/test_bot_service.py,
  tests/test_bot_cli_v1.py.
- Scope: Medium. Coverage: RA01, RA03, RA09, RA13, RA14.

### Checkpoint: complete provider behavior

- [ ] Full suite, compile, dependency check, fresh smoke/replay and real socket/
  subprocess tests pass. All RA01–RA13 criteria have concrete evidence.

## RA-09: Document operation and audit the completed module

- [ ] Publish service/protocol instructions and verify RA01–RA14 traceability.
- Acceptance: Docs explain setup, explicit start, detach vs service shutdown,
  SSH/tmux, pause/risk semantics, unknown outcomes, fresh accounts, paper-only
  limits and retained artifacts. Every acceptance claim resolves to passing
  test evidence. Review the actual diff and append corrections to the trail;
  disclose unavailable independent review and transcript access.
- Verify: Final task checks; `git diff --check`; validate local doc links and
  run the documented offline service path against actual sockets. Do not rerun
  unchanged passing tests without new changes or an unresolved concern.
- Dependencies: RA-08.
- Files: docs/BOT_CONTROL.md, docs/BOT_RUNTIME.md, docs/_index.md,
  tests/test_bot_service.py if a missing end-to-end acceptance case is found.
- Scope: Medium. Coverage: RA14 and final acceptance traceability.

### Checkpoint: ready for provider review

- [ ] No unchecked task or acceptance criterion remains; commits are scoped,
  working tree is clean and audit limitations are explicit. No deployment,
  TUI/client implementation, credentials or real-money execution occurred.
