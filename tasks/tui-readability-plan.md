# TUI readability follow-up

Status: Complete under user-approved autonomous execution, 2026-11-13.
Baseline: cbaefe4 on feature/trading-bot-tui. Original build tasks remain complete.
Reference: user's screenshot and agreed combination of a larger bottom panel
with an expanded diagnostic view. No additional approval gate is needed.

## Scope and design contract

Keep the existing dark/teal terminal style and independent service lifetime.
Make the strategy name and active Orders/Positions/Fills/Events/Findings view
prominent; metadata is secondary. Visible tabs support mouse and keyboard.
Allocate roughly the bottom third of a large terminal to diagnostics, with an
adjustable divider and full-screen expansion. At 100x28 preserve usable tables,
controls, q and resize behavior; hide unnecessary chrome in expanded mode.
Separate Findings, Events and Record details. Findings show severity, time,
strategy, author and summary; selected details retain full message/evidence/IDs.
Support info/cyan, warning/amber, error/red with textual labels and no-color
fallback. Filter multiple severities, All and Warnings+Errors; show matching
count, scope and honest loading/empty/error/stale states. Newest findings first.

Read all bounded retained findings (128 max, 50 per request) using the existing
provider. Enforce traversal/window bounds; invalidate late reads after target,
strategy or filter changes. No incomplete result may claim no matching findings.
Do not change protocol, trading, runner, severity levels or service ownership.
No dependencies, deployment, push or merge. Debug/Critical remain out of scope.

## Sequential slices

1. Header/tabs: prominent strategy + active view, secondary collection metadata;
   click and keyboard select the same view. Test idle/detail/ASCII/long labels.
2. Layout: larger proportional diagnostics, mouse-adjustable divider with keyboard
   alternatives, expand/restore preserving selection; resize/modal/q remain safe.
3. Diagnostics: separate views and structured severity-colored findings table;
   bounded full-retention loading, newest-first, selection shows inert full text.
4. Filters: multi-select Info/Warning/Error and presets, matching count, no
   findings vs no matches, reset appropriate cursors, no stale result replacement.
5. Finish: real PTY and Pilot interaction/resize/focus checks, hostile input and
   memory regressions, docs, independent review where available, audit and commits.

## Verification

For each behavioral slice: failing pytest test -> minimal code -> focused tests
-> full regression -> compileall -> diff review -> scoped commit. Use the
existing Python3.12 venv, Textual8.2.8, pytest/Pilot and actual synthetic services.
Run `venv/bin/python -m pytest -q`, `venv/bin/python -m compileall -q main.py src
examples tests`, and `venv/bin/python -m pip check`; retain logs under .audit.
Final checks include actual PTYs (no user bot/public credentials) and fresh
smoke/replay. Do not weaken old tests, deadlines or memory thresholds.
Continue the canonical `.audit/trading-bot-tui.tsv` append-only. Cross-family
review is unavailable per this thread's capability check; disclose limits.

## Completion

All five slices are implemented and committed in order. Final regression: 535
passed; 39 focused UI/process cases, compilation, dependency checks, fresh
smoke/replay and the documented real-terminal attach path passed. Fresh GPT-6
Astra review found no Required defects; its optional j/k finding was fixed and
regression-tested afterward. [Verification](../docs/verification/terminal-ui-readability.md)
records evidence, terminal/render probes, mutation proof and review limits.
