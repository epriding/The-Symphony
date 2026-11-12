# Implementation plan: runtime-access

Status: Approved by the user; completed and verified on 2026-11-12.
Spec: [SPEC-runtime-access.md](../SPEC-runtime-access.md), approved by the user
after commit c8b3ba8. Initiative: [SPEC.md](../SPEC.md).
Task list: [todo.md](todo.md). Decision trail: `.audit/trading-bot-tui.tsv`.

## Outcome and scope

Deliver only the shared local runtime service, with bounded observations,
session controls and diagnostic findings. Keep the existing paper runner as
the sole mutable owner of execution and accounting. Human TUI and structured
agent CLI are subsequent modules; do not implement them in this plan.

The approved contract includes global controls, pause with continuing order/
risk processing, fresh-session start, an explicit independent local service,
private Unix sockets and SSH client access. Approval of this plan authorizes
those offline implementation/test changes; it does not authorize deployment,
public-feed credential use, new role/auth systems or real trading.

## Baseline and environment

Initial branch is feature/trading-bot-tui at c8b3ba8 and Git status was clean
before creating this plan. No existing tasks/plan.md or tasks/todo.md existed.
Python is 3.12.3. The image has no pip, pytest, ensurepip, uv or virtualenv
command, and no existing project venv was found at the checked paths. Tests
have not run. Task RA-00 restores local Python tooling and records a real
baseline before any behavioral change; never treat missing tooling as a pass.

Do not change requirements or CI merely to work around the local image. Use
an isolated environment; if system venv tooling is unavailable, investigate
the supported bootstrap path and record it before installing anything. Do not
silently omit dependencies or tests. A baseline code failure must be reproduced
and handled under the debugging workflow before feature work proceeds.

## Architecture and dependency direction

```text
RA-00 baseline
  -> RA-01 runner pause/replay seam
  -> RA-02 local read-only socket service
  -> RA-03 explicit session lifecycle
  -> RA-04 shared strategy/trade observations
  -> RA-05 paged bounded collections
  -> RA-06 runtime events and findings
  -> RA-07 bounded load and failure behavior
  -> RA-08 serve CLI and process-lifetime acceptance
  -> RA-09 operating documentation and final verification
```

Implement sequentially. File sharing and runner ownership make parallel
implementation more costly than useful. `service.py` owns local sessions and
validates requests through `protocol.py`; `observations.py` owns copied views.
Extend the runner at serialized boundaries, not from client callbacks. Keep
the capture/replay control interpretation adjacent to existing replay code.
Add no general event bus, plugin loader, remote service or new dependencies.

Use only private local sockets, with the owner/mode, peer-UID and filesystem
checks explicitly approved in the provider spec. This is not authorization
to add user accounts, external authentication or public network listening.

## Per-task execution and evidence

### Authorized archive/replay prerequisite

After RA-03's full regression exposed nondeterministic global history sequence
numbers, the user approved fixing that existing defect and continuing the plan.
Version the detail export as history schema 2, using transactional one-based
sequence numbers within each stream; preserve input capture readers and account
calculations. Use the preserved failed capture and an interleaving regression
to verify every payload field and strict new-format hashes. Legacy schema-1
history hashes are not byte-identical promises for the version-2 output. Resolve
this prerequisite and repeat full verification before marking RA-03 complete.

After plan approval, commit these planning artifacts as one preparatory commit.
Record approval in the spec/map status headers in that same commit. Stage only
those named files. Then, for each behavior task:

1. Read its acceptance criteria and relevant code/spec sections.
2. Write and run focused failing acceptance tests; retain failure evidence.
3. Implement the smallest complete slice, then run focused tests to green.
4. Run the full suite and compile check; run smoke/replay when runner/capture/
   entrypoint behavior changes. Investigate failures, never weaken tests.
5. Review staged changes and commit only the slice plus its todo update.
6. Append a decision/checkpoint row with real test output/commit pointers.

Use the isolated environment's Python for these commands. For example, after
provisioning and activating `.venv`, run:

```bash
python3 -m pip check
python3 -m pytest -q --junitxml=reports/pytest.xml
python3 -m compileall -q main.py src examples tests
```

Focused tests are named in todo.md. A separate build system, lint command or
coverage target does not exist in CI; do not claim those checks ran. Existing
compileall is the build check. Every behavioral slice earns a test and commit.
RA-00 and RA-09 are setup/documentation verification, not reasons to invent
tests for static text. The excluded `.audit/` directory stores actual RED,
GREEN, baseline and checkpoint outputs alongside the canonical TSV.

## Automatic checkpoints

- After RA-02: pause/replay invariants and idle service/socket safety verified.
- After RA-05: independently running sessions, current-state views and paging
  agree with the ledger; check that clients cannot become account owners.
- After RA-08: bounded multi-client, process exit, control races, replay and
  full CLI tests pass with actual socket/subprocess evidence.
- After RA-09: all RA01–RA14 criteria traced to passing tests and docs. Audit
  the trail against current-thread transcript/evidence; disclose limitations.

These checkpoints are automated reviews within /build auto, not extra human
approval gates. Pause only for the build workflow's specified blockers.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Pre-pause callback acts after resume | Exercise the actual runner with deterministic callback barriers; invalidate by dispatch generation and preserve replay boundaries. |
| New control records diverge under replay | RA-01 covers recorded and fresh-strategy replay, including cancellation and existing-capture compatibility, before exposing controls. |
| Reads block market/risk progress | Produce bounded copied views at serialized boundaries; no socket/file wait under the ledger lock; bounded queues and saturation tests. |
| Process detach accidentally stops trading | Service owns the run; real subprocess tests kill/disconnect clients and reattach to the same session. |
| Stale controls target a replacement session | Bind commands/cursors to service/session IDs; observe after unknown timeouts, never automatically replay mutations. |
| Filesystem cleanup deletes foreign data | Exclusive output creation; owner/inode checks; reject symlinks/existing sockets; replacement-entry shutdown tests. |
| Inspection grows with session history | Bounded projections/rings, byte budget and paged DTOs; retained-range/eviction evidence; no capture scans. |
| Diagnostics leak secrets or execute terminal codes | Allowlist fields, reuse redaction, sanitize text, and test sentinels plus escape sequences. |
| Public runner has no production strategy factory | Keep existing registry behavior and validate it honestly; offline fixture tests do not pretend to deliver a trading algorithm. |

## Stop conditions and review

Stop for unresolved test/build failures, spec ambiguity requiring a changed
contract, or unapproved irreversible/high-risk operations. Fix ordinary failures
within the approved scope without requesting repeated permission. Never deploy,
start a user's public session, or add live execution during this build.

Cross-family trail review is required by show-me-your-work when a reviewer is
available. The capability result already obtained in this thread showed other
provider families unavailable. Do not install a provider or substitute a
self-review as independent review; disclose this if it remains unavailable.

## Open questions

None about the completed provider scope. RA-00 restored local tooling and all
tasks are complete. Final evidence: 450 passing tests, compilation/dependency
checks, fresh offline smoke/replay and the documented socket probe. See
`.audit/acceptance-trace.json` and `.audit/trading-bot-tui.tsv` for traceability.
The original baseline section above records pre-implementation conditions.
This completed plan does not authorize either client module.
