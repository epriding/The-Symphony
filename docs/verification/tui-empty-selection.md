# TUI no-selection crash report

## Diagnosis

This is a reproducible client-side bug. The supplied traceback shows
`RowHighlighted(cursor_row=-1, row_key=None)` followed by an attempt to read
`_visible_rows[-1]` while that list is empty. The handler checked only
`index < len(rows)`: for an empty list, `-1 < 0` is true. Python then raises
`IndexError`. With populated rows, negative indexing can silently display the
last record instead of representing no selection.

The initial keyboard-only hypothesis was not confirmed: Down/End alone did
not reliably produce a negative cursor in the local test environment. The
exact sequence preceding the user's error remains unknown. Recreating the
reported framework state/event directly reproduced the same IndexError against
the original code; this establishes the defect without guessing those steps.

## Correction

Both the main table and diagnostics now require `0 <= index < len(rows)` for
selected-record rendering and selection preservation during refresh. Their
selection handlers ignore negative or out-of-range events, rather than opening
a strategy or expanding diagnostic details. No generic exception suppression,
service protocol changes, trading logic changes or dependency updates were added.

Six new cases cover empty and populated tables in both panes, repaint after a
no-selection highlight, and invalid selection events. They fail against the
original source and pass after the fix. The ordinary keyboard path and detach
are exercised as well; the independent synthetic service remains alive.

## Verification

**541 tests passed in 192.45 seconds**, with no failures, errors or skips.
The focused UI/process suite passed 45 cases; compilation and dependency checks
also passed. Reproduce with:

```bash
venv/bin/python -m pytest -q tests/test_bot_tui_empty_selection.py
venv/bin/python -m pytest -q
venv/bin/python -m compileall -q main.py src examples tests
venv/bin/python -m pip check
```

Local evidence: `.audit/empty-selection-reproduced.log`,
`empty-selection-green2.log`, `empty-selection-focused.log` and
`empty-selection-regression.xml`. The canonical append-only trail is
`.audit/trading-bot-tui.tsv`; it explicitly corrects the initial unproven
keyboard-trigger claim.

Fresh GPT-6 Astra review found no Required defects and independently passed the
six targeted cases. Its optional suggestion is to add a two-distinct-row fixture
asserting which row remains selected after refresh; current coverage checks
empty/single-row no-selection behavior. This is a coverage enhancement, not an
identified remaining defect. Review was same-family, focused and read-only;
no independent full-suite rerun or complete host-transcript audit occurred.

The patch is produced in the current feature worktree. The user's
`/home/admin/The-Symphony-tui` checkout must receive the fix before its TUI client
is reopened. There was no remote deployment or modification of that machine.
