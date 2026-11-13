# Terminal-ui readability follow-up verification

Scope: [approved plan](../../tasks/tui-readability-plan.md) and
[task checklist](../../tasks/tui-readability-todo.md), approved for autonomous,
sequential TDD execution in the conversation on 2026-11-13. The original build's
[acceptance trace](terminal-ui.md) remains historical evidence, not verification
of this follow-up. [The operating guide](../BOT_TUI.md) describes current use.

## Reproduce

```bash
venv/bin/python -m pytest -q tests/test_bot_tui_readability.py tests/test_bot_tui_findings.py tests/test_bot_tui.py tests/test_bot_tui_process.py
venv/bin/python -m pytest -q
venv/bin/python -m compileall -q main.py src examples tests
venv/bin/python -m pip check
git diff --check
```

Tests use local Unix sockets, synthetic projections/feeds, Textual Pilot and
actual PTYs. No user bot, live trading, public feed credentials, dependency
upgrades, provider protocol changes, push or deployment are involved.

## Increment evidence

| Slice | Evidence and result |
|---|---|
| RD-01 Header/tabs | New missing-heading test failed first; keyboard/mouse collection navigation passed. 26 focused cases and 522 full tests passed; compileall passed. Commit `53dcbe1`. |
| RD-02 Layout | Missing-panel and expanded-refresh tests failed first; drag, keyboard sizing, expand/restore, resize and q passed. 27 focused and 523 full tests passed; compileall passed. Commit `0d11b11`. |
| RD-03 Diagnostic tables | Missing reader/component tests failed first; bounded full-window traversal, newest ordering and separate literal detail views passed. 32 focused and 528 full tests passed; compileall passed. Existing memory assertion was strengthened to include retained history/table rows. Commit `723e880`. |
| RD-04 Filters | Missing controls/selection tests failed first. Presets, multiple severities, matches beyond the first provider page, shared main-view pagination and distinct empty states passed. 34 focused and 530 full tests passed; compileall passed. Commit `555b3e3`. |
| RD-05 Finish | Three added cases exposed clipped active-view headings, missing timestamp presentation and hidden notices in expanded mode; fixed. The late-scope barrier passed. Fresh review reproduced an optional j/k focus bug; its added regression failed first and passed after correction. Final: **535 tests passed in 189.59 seconds**, no failures/errors/skips; 39 focused UI/process cases, compilation, pip check, fresh smoke/replay and the documented real-PTY CLI path passed. Final code commit `cadfb88`. |

The canonical `.audit/trading-bot-tui.tsv` is append-only and locally ignored.
Evidence artifacts use `rd-baseline.log`, `rd01-*` through `rd05-*`. Initial
failures, intermediate fixes and final logs remain available in this worktree.

## Behavior coverage

- [Readability tests](../../tests/test_bot_tui_readability.py) exercise prominent
  headings, visible tab activation with keyboard/mouse, divider drag and keyboard
  sizing, expansion/restoration preserving navigation, and small-terminal recovery.
- [Finding tests](../../tests/test_bot_tui_findings.py) cover all 128 retained
  findings across provider pages, moving windows, repeated/excessive cursors,
  severity labels/colors, separate modes, literal full messages/evidence,
  multi-select and keyboard presets, complete-window counts and local pagination,
  empty states, long names, unknown timestamps and late-scope response barriers.
- Existing [UI](../../tests/test_bot_tui.py),
  [transport](../../tests/test_bot_client.py) and
  [PTY process](../../tests/test_bot_tui_process.py) suites remain enabled.
  Actual q/Ctrl-C/client-kill tests reattach to the same synthetic session with
  advancing input/risk processing. PTYs also check ASCII/monochrome output,
  resizing, handled errors, exit codes and terminal mode restoration.

A controlled mutation inverted severity membership. The filter test failed;
restoring the exact source bytes made it pass. Hashes and results are in
`.audit/rd05-mutation.json`, `rd05-mutation.log` and
`rd05-mutation-restored.log`.

## Limits

The cooperative 8 MiB data/buffer assertion includes the new finding history and
table rows. It excludes Textual/framework memory and is not whole-process RSS.
The diagnostic Events view is explicitly the overview subset, not an exhaustive
session log. The 128-finding history is bounded retention, not archived history.
Default layout proportions apply to available workspace; minimum row sizes and
short-terminal compaction take precedence. SIGKILL cannot restore terminal modes.

Review limitations: no different-family provider was available from the earlier
capability check, and no complete host transcript was audited. The canonical
trail is checked against available conversation and actual local artifacts.
Synthetic process/geometry tests do not establish compatibility with every
terminal emulator or replace human visual usability feedback.

## Final review and render evidence

Fresh-context GPT-6 Astra review found no Required fixes and independently ran
the 13 then-existing readability/finding cases successfully. It reproduced the
optional j/k scroll-focus issue; parent fixed it, updated Help and ran the final
535-test suite. The reviewer did not independently rerun these final corrections.
This was different-model, same-family review, not cross-family review.

Actual Textual render exports for synthetic smoke data are retained locally as
`.audit/rd05-overview.svg`, `rd05-fills.svg`, and
`rd05-expanded-filtered.svg`. The render probe checks expanded panel geometry
at 100x28, visible control notice and the filtered row count. These are actual
framework exports, not a claim of human visual review. Reproduce with the
local `.audit/rd05-render-probe.py`; PTY tests separately verify real terminals.

Final machine-readable evidence: `.audit/rd05-acceptance-trace.json`,
`rd05-regression-final.xml`, `rd05-memory-profile.json`,
`rd05-doc-check.json`, `rd05-documented-path.json` and `rd05-review.md`.
Canonical audit history retains an explicit correction for reusing the earlier
CLI probe's output path: the actual new output is under rd05, and the original
tu08 record was restored from its earlier captured conversation output.
