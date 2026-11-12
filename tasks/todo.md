# Terminal-ui tasks

Plan: [plan.md](plan.md). Spec: [SPEC-terminal-ui.md](../SPEC-terminal-ui.md).
Status: Approved; TU-07 complete; TU-08 in progress.
Every behavioral task requires RED -> GREEN -> full regression -> compile ->
review -> scoped commit. Run common checks from plan.md before completion.
Acceptance IDs TU01–TU10 refer to the spec. No task changes bot ownership.

## TU-00: Verify Textual in the existing environment

- [x] Pin and install `textual==8.2.8`, without extras.
- Acceptance: Current baseline is verified; dependency resolution, imports,
  pip check and full regression pass with all existing requirements retained.
- Verify: `venv/bin/python -m pip install -r requirements.txt`, then
  `venv/bin/python -m pip show textual` and common checks. Preserve actual logs.
- Dependencies: Approved plan.
- Files: requirements.txt; task status and local audit artifacts.
- Scope: Small; setup, no behavioral test invention. Coverage: TU10 foundation.

## TU-01: Discover an idle provider through bounded sockets

- [x] Implement the reusable asynchronous transport and real service tests.
- Acceptance: Validated idle overview is inert; request IDs/targets and
  success/error shapes are checked. Enforce frame/object/depth/finite JSON
  limits, deadlines and cancellation; disconnect never retries a mutation.
- Verify: `venv/bin/python -m pytest -q tests/test_bot_client.py` and common
  checks. Use actual provider sockets plus malformed/fragmented reply fixtures.
- Dependencies: TU-00.
- Files: src/operator/client.py, tests/test_bot_client.py.
- Scope: Small. Coverage: TU01, TU05, TU07.

## TU-02: Attach and detach through a real Textual entrypoint

- [x] Add `tui --socket` and an idle overview with working quit.
- Acceptance: Lazy entrypoint requires a usable TTY, does not launch service,
  load credentials or create captures; missing service remains inspectable.
  q detaches, cleanup closes local work and the terminal restores normally.
- Verify: `venv/bin/python -m pytest -q tests/test_bot_tui.py tests/test_bot_tui_process.py`
  and common checks plus fresh offline smoke/replay. Pilot and a real PTY must
  exercise the CLI; process fixtures are introduced here for later reuse.
- Dependencies: TU-01.
- Files: main.py, src/operator/tui.py, src/operator/tui.tcss,
  tests/test_bot_tui.py, tests/test_bot_tui_process.py.
- Scope: Medium. Coverage: TU01, TU08, TU09.

### Checkpoint: safe terminal ownership

- [x] Full checks, idle socket attach and actual PTY quit pass; existing CLI
  commands remain compatible and the client cannot own the service lifetime.

## TU-03: Observe fresh state and reconnect honestly

- [x] Add the live overview, independent age timer and read-only reconnect.
- Acceptance: Actual overview fields and unknown values render correctly;
  non-overlapping one-second polling and three-second stale labels use local
  monotonic time. Service/session replacement invalidates prior view state.
- Verify: `venv/bin/python -m pytest -q tests/test_bot_tui.py tests/test_bot_client.py`
  and common checks. Delay/disconnect the read path with deterministic barriers;
  assert UI responsiveness, retained stale labels and bounded reconnect backoff.
- Dependencies: TU-02.
- Files: src/operator/tui.py, src/operator/tui_state.py,
  src/operator/tui.tcss, tests/test_bot_tui.py.
- Scope: Medium. Coverage: TU02, TU05, TU06.

## TU-04: Inspect strategies, current trades and diagnostics

- [x] Add strategy selection and separate paged detail collections.
- Acceptance: All configured strategies and modes/reasons match provider DTOs;
  orders/positions/fills/events/findings retain fields and source precision.
  Paging exposes scope, revision and eviction; long selected messages scroll.
- Verify: `venv/bin/python -m pytest -q tests/test_bot_tui.py` and common checks.
  Use multi-strategy real account fixtures, disabled/unrouted entries, retained
  window overflow and navigation tests; render remote text as literal data.
- Dependencies: TU-03.
- Files: src/operator/tui.py, src/operator/tui_state.py,
  src/operator/tui.tcss, tests/test_bot_tui.py.
- Scope: Medium. Coverage: TU02, TU03, TU09.

## TU-05: Apply deliberate exact-target controls

- [x] Add confirmed start/stop, pause/resume and pending/unknown outcome state.
- Acceptance: Confirmation names the frozen target and requires typed action;
  cancel/invalid states are harmless. Admission is distinct from completion.
  Replacements invalidate dialogs; timeout never retargets or repeats a command.
- Verify: `venv/bin/python -m pytest -q tests/test_bot_client.py tests/test_bot_tui.py`
  and common checks. Exercise real runner transitions and synchronized response
  loss/target changes; blocked reads must not prevent applicable stop or quit.
- Dependencies: TU-04.
- Files: src/operator/tui.py, src/operator/tui_state.py,
  src/operator/tui_dialogs.py, src/operator/tui.tcss, tests/test_bot_tui.py.
- Scope: Medium. Coverage: TU04, TU05, TU07.

### Checkpoint: complete human control path

- [x] Full checks pass; real state and controls agree with the provider.
  Review fresh-account/pause wording and races without adding trading logic.

## TU-06: Prove bounded and hostile-input failure behavior

- [x] Complete backpressure, resource accounting and malformed reply coverage.
- Acceptance: Client data/pages/queues stay within the approved 8 MiB profile;
  tasks/connections release on cancellation and detach. Oversized, malformed,
  stale-generation, markup and control-sequence payloads cannot become success.
- Verify: `venv/bin/python -m pytest -q tests/test_bot_client.py tests/test_bot_tui.py`
  and common checks. Record actual retained-byte/task high-water evidence under
  repeated paging, delayed replies, unknown controls and target replacement.
- Dependencies: TU-05.
- Files: src/operator/client.py, src/operator/client_schema.py,
  src/operator/tui.py, tests/test_bot_client.py, tests/test_bot_tui.py.
- Scope: Medium. Coverage: TU05, TU07, TU09.

## TU-07: Verify real terminal lifecycle and appearance

- [x] Complete PTY/process acceptance and presentation checks.
- Acceptance: q/Ctrl-C/client kill leave a persistent synthetic service on the
  same session with advancing inputs/risk; expected exit codes and terminal
  restoration hold. Resize, focus, text input, monochrome/ASCII and long text work.
- Verify: `venv/bin/python -m pytest -q tests/test_bot_tui_process.py tests/test_bot_tui.py`
  and common checks. Use actual PTYs at 100x28 and smaller/restored sizes, with
  explicit feed barriers and terminal attribute checks; retain safe output evidence.
- Dependencies: TU-06.
- Files: main.py, src/operator/tui.py, src/operator/tui.tcss,
  tests/test_bot_tui.py, tests/test_bot_tui_process.py.
- Scope: Medium. Coverage: TU08, TU09.

### Checkpoint: process independence and bounded failure

- [x] Full checks, resource bounds and real process acceptance pass. Review
  actual terminal output; headless tests alone cannot complete this checkpoint.

## TU-08: Document operation and audit the completed TUI

- [ ] Publish the verified offline/SSH workflow and acceptance trace.
- Acceptance: Commands, keys, fresh-account/pause semantics, stale/unknown
  outcomes and limits are documented. Every TU01–TU10 criterion resolves to
  passing evidence; review/trail limitations are explicit and branch is clean.
- Verify: Common checks plus fresh smoke/replay and documented service/TUI path;
  validate local links, review diff, audit canonical trail and check all criteria.
- Dependencies: TU-07.
- Files: docs/BOT_TUI.md, README.md, SPEC.md, SPEC-terminal-ui.md,
  CAPABILITY-MAP.md; task statuses and ignored audit artifacts.
- Scope: Medium; documentation/review. Coverage: TU01–TU10 final traceability.

### Checkpoint: ready for human review

- [ ] All tasks/criteria complete with scoped commits and recorded evidence.
  No deployment, public credentials, live execution or agent CLI occurred.
