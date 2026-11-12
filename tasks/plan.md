# Implementation plan: terminal-ui

Status: Proposed for one build-plan approval. Spec approved by the user after
commit 0612c95. No terminal-ui implementation or dependency installation yet.
Spec: [SPEC-terminal-ui.md](../SPEC-terminal-ui.md).
Tasks: [todo.md](todo.md). Initiative: [SPEC.md](../SPEC.md).
Trail: `.audit/trading-bot-tui.tsv`.

## Outcome and scope

Build the approved Textual human client for the existing local paper-bot
service. Provide overview, strategy/trade detail, diagnostics and deliberate
session controls. Detach never stops trading. Preserve the independent service,
provider protocol and sole runtime ownership of accounting. Agent CLI, agent
reasoning/repair, strategy editing and live execution remain outside this plan.

The previous runtime-access plan and task list are fully complete and preserved
in Git at b4b76a2 (`git show b4b76a2:tasks/plan.md` and
`git show b4b76a2:tasks/todo.md`). No unfinished plan was overwritten.

## Baseline and evidence

Branch: feature/trading-bot-tui. Planning started from a clean checkout at
0612c95. Python 3.12 and the existing `venv/` are available. Last application
verification: 450 passing tests, compilation, dependency check and offline
smoke/replay in `.audit/acceptance-trace.json`. Textual is not installed, as
confirmed by pip show; version 8.2.8 and Python compatibility were checked in
`.audit/textual-package-metadata.json`. Dependency resolution is still untested.
Do not reuse the 450-pass result as evidence for new code or dependencies.

## Architecture and dependency graph

```text
TU-00 dependency/baseline
  -> TU-01 bounded socket client
  -> TU-02 idle Textual app and CLI
  -> TU-03 fresh/reconnected overview
  -> TU-04 strategy and trade views
  -> TU-05 exact-target session controls
  -> TU-06 failure and buffer bounds
  -> TU-07 actual terminal/process acceptance
  -> TU-08 operating docs and final audit
```

Implement sequentially because the slices share client and UI state. No
parallel coding is needed. Each slice adds an end-to-end observable behavior
and tests; avoid building a generic UI framework before using it.

- `src/operator/client.py` owns finite request/response framing, deadlines,
  correlation, target validation and connection closure. It never imports
  Textual, starts a service or owns a runner. Later agent clients can reuse it.
- Use asynchronous Unix streams with independent read/control connections so
  stalled polling cannot occupy the command lane. At most one refresh sequence
  and one control in flight; bound and cancel work rather than queueing polls.
- `src/operator/tui_state.py` owns only client observations, target/navigation
  generation, data-age and pending/unknown outcome state. Keep it small and
  directly testable; it is not a second account model.
- `src/operator/tui.py` uses Textual widgets/screens and workers. TCSS lives
  in `src/operator/tui.tcss`. The main CLI imports the TUI lazily so existing
  commands and ordinary imports do not initialize a UI or require a terminal.
- Confirmation captures immutable service/session IDs. Reconnection and view
  changes invalidate outstanding reads by generation; out-of-order replies
  cannot replace a newer target/view. Unknown mutations are never retried.
- Render remote strings as literal text after sanitization, with no Rich markup
  interpretation. Monetary/quantity strings retain source precision.

## Ordered task index

Detailed acceptance, verification, dependencies and files live in todo.md.

| Task | Deliverable | Acceptance coverage |
|---|---|---|
| TU-00 | Install the pinned Textual dependency and verify baseline | TU10 foundation |
| TU-01 | Real bounded idle socket discovery and safe failure handling | TU01, TU05, TU07 |
| TU-02 | Usable idle Textual entrypoint with harmless detach | TU01, TU08, TU09 |
| TU-03 | Real overview with freshness, reconnect and target changes | TU02, TU05, TU06 |
| TU-04 | Paged strategies, trade detail and diagnostics | TU02, TU03, TU09 |
| TU-05 | Confirmed controls and honest pending/unknown outcomes | TU04, TU05, TU07 |
| TU-06 | Saturation, malformed replies and client retention bounds | TU05, TU07, TU09 |
| TU-07 | Real terminal lifecycle, resize and process independence | TU08, TU09 |
| TU-08 | Documented offline path, review and final traceability | TU01–TU10 |

## Execution and verification contract

After approval, commit the preparation checkpoint and execute every task in
order without further routine permission. For behavior changes, preserve
RED evidence, implement the smallest complete slice, run focused tests to
GREEN, then full regression and compilation before the task commit. Stage
only that task's files plus its status update. No skipped/weakened tests.
TU-00 is dependency setup and TU-08 documentation/review; do not invent tests
that mirror static edits. Failures must be investigated, not marked passed.

Use `venv/bin/python` consistently. Common task verification:

```bash
venv/bin/python -m pytest -q
venv/bin/python -m compileall -q main.py src examples tests
venv/bin/python -m pip check
git diff --check
```

Focused commands are in todo.md and refer to future test files where stated.
Compileall is the existing build check; no separate linter/build tool or
coverage percentage is established. Do not claim one. Run fresh offline
smoke/replay for the CLI slice and final integration, plus any new change to
runner/capture behavior (which is otherwise outside this plan):

```bash
tui_check_dir=$(mktemp -d)
venv/bin/python main.py smoke --output "$tui_check_dir/smoke"
venv/bin/python main.py replay --capture "$tui_check_dir/smoke" --output "$tui_check_dir/replay"
```

Use Textual App.run_test/Pilot for content, focus, input and resize assertions.
Use actual Unix sockets for transport and real PTYs for terminal modes, output,
exit codes and independence. Process acceptance must run a persistent synthetic
service with controlled feed barriers; a finite smoke run that ends before the
client exits cannot prove continued trading. Observe the same session and
advancing inputs/risk after detach/kill. Never load public credentials.

Record exact focused/full results and fixture high-water measurements under
`.audit/`. Continue the canonical TSV; do not replace previous evidence.

## Automatic checkpoints

- After TU-02: bounded inert attach and actual terminal quit work; existing
  commands remain unchanged. Inspect lifetime ownership before controls exist.
- After TU-05: overview/detail agree with the real provider; confirmation text,
  target invalidation and unknown outcomes are tested with races/barriers.
- After TU-07: backpressure, buffer limits, real process detach and terminal
  restoration pass. Headless UI tests do not substitute for PTY evidence.
- After TU-08: all TU01–TU10 criteria trace to passing tests and documented
  runtime verification; staged changes and decision trail audited.

These are automated reviews within the already requested /build auto, not
additional human gates after plan approval.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Textual resolution conflicts with current dependencies | Pin 8.2.8, run pip check and full regression before UI changes; add no optional extras or unrelated upgrades. |
| Stalled read blocks stop or quit | Separate bounded read/control tasks and connections; real slow-socket tests while exercising input and stop. |
| Old read or confirmation targets a new session | Explicit identities plus client view generations; invalidate on replacement and never replay controls. |
| Five-second request hides three-second staleness | Monotonic age timer independent of I/O completion; label retained views and disable mutations on schedule. |
| Polling resets navigation or mixes pages | Keep collection/filter/cursor explicit; refresh begins at the first page as specified; show scope, age and revision rather than claiming a stable multi-page snapshot. |
| Framework captures q/Ctrl-C or typed control letters | Scoped Textual bindings, confirmation-input Pilot tests and actual PTY exit/restoration tests. |
| Rich markup/escape sequences alter remote display | Literal text cells/widgets, sanitation and malicious payload tests. |
| Widget history grows despite bounded wire frames | Replace visible pages rather than appending; measure client data/queue retention separately from Textual/framework allocations. |
| Tests pass but screen is unusable | Verify actual terminal at 100x28, smaller and restored sizes; visible focus, contrast-independent labels, long-message scrolling and monochrome/ASCII. |

## Stop conditions, review and next gate

Stop for an unresolved failing check, ambiguity changing the approved contract,
or unapproved destructive/external actions. Fix routine failures within this
scope without repeated permission. Do not deploy, use real credentials, start
a user's configured public session, push or merge during this build.

Review the code and trail using the applicable review skills. Cross-family
review was unavailable in the capability result already obtained in this
thread; do not claim it occurred or install another provider. Disclose any
unavailable reviewer or complete host transcript. Prior GPT-6 Astra review
covered the provider, not this plan or future TUI code.

No open feature question remains. The next gate is approval of this concrete
plan, as required by the /build auto command's single checkpoint. Once approved,
continue through all tasks autonomously; agent-interface still needs its own spec.
