# Spec: terminal-ui

Status: Approved by the user after commit 0612c95. Module id: `terminal-ui`.
Library choice: Textual, explicitly requested by the user in place of curses.
Depends on: implemented `runtime-access`. Build plan approved and executed;
human client implemented and verified.
Sources: [confirmed intent](docs/intent/trading-bot-tui.md),
[approved capability map](CAPABILITY-MAP.md),
[provider contract](SPEC-runtime-access.md).

## Objective

Give the operator one terminal view that answers: Is the bot working as
intended, what strategies and trades are active, and what needs attention?
It works directly on the Linux bot host and through SSH from the laptop.
Opening, closing or losing the terminal never controls the service lifetime.
Agents use the separately specified structured interface, rather than
automating keystrokes. Agent diagnosis and code repair remain external.

## Approved defaults

- Use Textual with Python 3.12, as explicitly selected by the user.
- Show an overview and selectable strategy detail. Keep connection status,
  session state, execution mode, data age and available keys visible.
- Poll the visible view about once per second without overlapping refreshes.
  Mark connection observations stale after three seconds without a successful
  overview. Quote readiness is separate from connection freshness.
- Controls act on the whole session. Confirm start and stop; explain fresh
  accounts and that pause allows existing orders and risk actions to continue.
- `q` detaches immediately. Never launch the service automatically.

## Tech stack and interface

Python 3.12, Textual, bounded asyncio Unix-socket I/O, JSON, pytest and
pytest-asyncio. The direct dependency is pinned as `textual==8.2.8`, without
optional syntax-highlighting or development extras.
[Published package metadata](https://pypi.org/project/textual/8.2.8/) declares
Python >=3.9,<4 compatibility, including our Python 3.12 runtime. Dependency
resolution and full regression passed in the implementation environment; see
[the acceptance trace](docs/verification/terminal-ui.md) for evidence and limits.

Use Textual's built-in widgets, screens and
[TCSS stylesheets](https://textual.textualize.io/guide/CSS/) for consistent
spacing, a restrained color palette, clear table selection, visible keyboard
focus and visually distinct status panels. Use its asynchronous event loop
and [workers](https://textual.textualize.io/guide/workers/) to keep network I/O
off the input/render path, with bounded cancellation and separate read/control
work. Target Linux terminals; SSH provides laptop access. No browser, TCP
listener, daemon installation or new auth system.

Consume protocol version 1 from [the provider guide](docs/BOT_CONTROL.md).
Never import a runner to obtain state or change the ledger directly. Initial
overview discovers service/session identities; subsequent requests target those
identities. Control acknowledgment means admission, not completed transition.

## Views and navigation

The overview shows lifecycle/reason, operator state, service/session identity,
public/synthetic source, observation-only state, PAPER-only support, strategy
and venue readiness, active-order/position counts, recent runtime errors,
events and external findings. Unavailable counts are unknown, never zero.
Idle, starting, degraded, stopping, stopped and failed are distinct. Failed
and stopped sessions remain inspectable.

The paged strategy list includes every configured entry, including disabled
and unrouted entries: name, route/mode, enabled/readiness reason, operator
state, active orders and owned positions. Null routes display as unrouted,
never LIVE. Public data is labeled independently of execution mode.

Strategy detail has tabs for active orders, owned positions, recent fills,
runtime events and external findings. Show available venue/market/outcome,
side, price, quantity, status and timestamps without inventing P&L or missing
values. Preserve decimal strings. Show activity/errors where available.
Findings show author label, severity, message and inert evidence text; author
labels are not proof of identity.

Use provider pagination with 50 rows per page. Display active/recent scope,
retained range, eviction and next-page availability. An empty recent page is
not proof that the strategy never traded. Keep only the visible page and
bounded navigation metadata. Refresh returns to the first page; next uses
only its current collection/filter cursor. Back returns to that collection's
first page. Differing revisions must not imply a transactional multi-page view.

| Key | Action |
|---|---|
| Up/Down or j/k | Select a strategy or scroll the focused pane |
| Enter | Open selected strategy |
| Tab / Shift-Tab | Change detail tab or focused overview pane |
| n / b | Next page / return to first page |
| Esc | Back to overview or cancel confirmation |
| s | Confirm explicit fresh-session start |
| p / r | Request pause / resume when applicable |
| x | Confirm stop of displayed session |
| ? | Show keys and control semantics |
| q | Detach from any view, including confirmations |

Confirmations name the exact service/session. Start explains that the new
account does not resume held positions; stop explains that cleanup preserves
positions and can fail. Require typed `start` or `stop`, with cancel as the
default. Hide inapplicable actions and explain disabled ones. Show pending
control state until observations confirm the transition. Keep this pause note
visible: “Strategy decisions paused; existing orders and risk actions can continue.”

## Connection and failure behavior

Input and rendering remain responsive during I/O. One blocked read cannot
occupy the sole control path. Allow one refresh sequence and one control in
flight; an applicable stop remains available while reads stall. I/O deadlines
are at most five seconds, matching the provider. Detach cancels local work
and closes sockets without sending stop or signals to the service.

On connection failure, show a fixed useful diagnostic and allow quit/reconnect.
Automatic reconnect may perform read-only discovery with bounded backoff up
to five seconds. Never retry a mutation automatically. Unknown outcomes stay
visible with action/target until reconciled or acknowledged; fresh discovery
alone does not prove that the earlier control failed.

Service/session replacement cancels confirmations, invalidates pages/cursors
and pending action assumptions, and announces the new target before controls
are enabled. Never retarget and repeat an old action. On disconnect or stale
observations, retain the last view with age and a stale label; disable mutations
until fresh discovery. Handle conflict, busy, unsupported/malformed replies and
unavailable observations explicitly rather than displaying empty success.

Bound frames to 1 MiB and decoded response objects to the provider's 512 KiB
profile. Validate required types, version, request correlation, target identities
and collection shape before applying replies. Keep client-owned observation
data, pages and I/O queues within 8 MiB under fixture workloads; Textual's
framework/rendering allocations are separate, not a whole-process RSS promise.
Discard invalid payloads without retaining/rendering tracebacks. Strip terminal
control characters from every displayed field, even though the provider also
sanitizes text. Render service-supplied strings literally, without interpreting
Rich/Textual markup. Never execute findings or automatically open evidence paths.

## Terminal behavior

At 100 columns by 28 rows, show overview summary and strategy list together;
diagnostics use scrolling/focus. Smaller terminals show a resize message while
retaining quit; enlargement restores the view. Resizing never sends controls or
loses the target. All states have text labels independent of color. Support
monochrome and ASCII fallback, clip/wrap long text safely, and let the operator
scroll the full selected message.

Require a TTY and usable terminal definition; explain startup errors before
starting the Textual app. Restore terminal modes on normal exit, Ctrl-C and handled
failures. Exit 0 on q, 130 on Ctrl-C, 2 on invalid invocation/terminal setup,
and 1 on unrecoverable client failure. Service failure/disconnect remains an
inspectable UI state until the operator quits.

## Commands

Existing offline provider setup, in a separately managed persistent terminal:

```bash
venv/bin/python main.py serve --smoke --socket "$HOME/.local/state/symphony/control.sock" --output-root "$HOME/.local/state/symphony/runs"
```

Implemented TUI attach command:

```bash
venv/bin/python main.py tui --socket "$HOME/.local/state/symphony/control.sock"
```

`--socket` is required. Never infer a production socket or create a service.
Offline development needs no public-feed credentials. Verification commands:

```bash
venv/bin/python -m pytest -q tests/test_bot_client.py tests/test_bot_tui.py
venv/bin/python -m pytest -q
venv/bin/python -m compileall -q main.py src examples tests
venv/bin/python -m pip check
git diff --check
```

Focused tests above exist. Actual PTY/process checks also live in
`tests/test_bot_tui_process.py` and keep the service in a separate process.

## Project structure

- `src/operator/client.py`: reusable bounded protocol client at the
  provider boundary, usable by human and future agent clients without importing
  the TUI.
- `src/operator/tui.py`: terminal views, navigation and UI state. Split further
  only for clarity; no trading/accounting logic belongs here.
- `src/operator/tui.tcss`: Textual styles for layout, status, focus and tables.
- `requirements.txt`: pins the user-requested Textual dependency.
- `main.py`: explicit `tui` entrypoint; preserve all existing commands.
- `tests/test_bot_client.py`, `tests/test_bot_tui.py`,
  `tests/test_bot_tui_process.py`: transport, state, interaction and PTY process
  integration tests, reusing offline service fixtures.
- `docs/BOT_TUI.md`: setup, keys, SSH, freshness, controls and limitations.
- `tasks/plan.md`, `tasks/todo.md`: new scoped plan after approval; completed
  provider artifacts remain preserved in Git history.
- `.audit/trading-bot-tui.tsv`: continue the canonical append-only trail.

## Code style

Follow existing Python/pytest conventions, with typed transport boundaries
and explicit UI states. Keep formatting pure so it can be tested without a
terminal. Preserve unavailable values instead of inventing zeros:

```python
def count_label(value: int | None) -> str:
    return "unknown" if value is None else str(value)
```

Use Textual's existing widgets and actions rather than building another widget
framework. Scope action bindings so text inputs in confirmation screens receive
ordinary typing. Do not add a configurable keybinding system for v1. Use stable
error labels and safe messages, never configuration dumps.

## Testing strategy and success criteria

Write failing behavior tests before each slice. Use Textual's
[App.run_test() and Pilot](https://textual.textualize.io/guide/testing/) with
pytest-asyncio for headless keyboard, focus, confirmation, widget-content and
resize tests. Exercise actual Unix sockets and the real offline provider,
synchronize state races with barriers, and retain real PTYs for terminal
acceptance. Headless Pilot tests alone do not prove detach, terminal restoration
or process independence. No extra snapshot-testing dependency is required.

| ID | Acceptance |
|---|---|
| TU01 | Idle attach/read/quit does not start a session, load credentials or create captures; missing service never auto-launches one. |
| TU02 | Overview/detail reflect actual multi-strategy observations, including disabled/unrouted, unknown, public/PAPER and terminal states. |
| TU03 | Orders, positions, fills, events and findings show distinct fields, page scope, retention/eviction and sanitized inert evidence. |
| TU04 | Exact-target start/stop require confirmation; cancel is harmless. Pause/resume show pending/completed states and continuing order/risk semantics. |
| TU05 | Timeout/disconnect never retries a mutation. Replacement invalidates confirmations/cursors and cannot retarget an old action. |
| TU06 | One-second visible-view polling, three-second stale labeling and bounded read-only reconnect work; quote readiness remains separate. |
| TU07 | Slow reads cannot block input, detach or applicable stop. Malformed/oversized response fixtures keep buffers/pages/tasks within declared bounds. |
| TU08 | Actual process tests quit, Ctrl-C and kill a client, then reattach to the same bot session while input/risk processing continues. |
| TU09 | Pilot and PTY tests exercise navigation, focus, confirmations, resize, monochrome/ASCII, long text, literal markup and terminal-control payloads; real PTYs verify q/Ctrl-C/errors restore terminal modes. |
| TU10 | Full regression, compilation and dependency checks pass; documented offline service/TUI path is verified with no weakened/skipped tests. |

## Boundaries

- **Always:** Consume provider state, show unknown/stale honestly, preserve
  targeting, sanitize output, verify PTYs/sockets and update docs and trail.
- **Ask first:** Add dependencies beyond the user-requested Textual package,
  alter provider contract/accounting, add
  controls/configuration, edit CI, deploy or start a user's public session.
  Spec approval permits implementation and offline tests only.
- **Never:** Real-money execution, arbitrary order entry, strategy editing,
  agent repair/reasoning, mutation retries, account recovery, public TCP/auth,
  auto-start/stop on attach/detach, credential display, finding execution,
  capture deletion or weakened tests.

## Approval record

The user selected Textual, reviewed the revised spec at commit 0612c95 and
explicitly approved it in a later turn. The overview/detail layout, keys,
refresh/stale thresholds, confirmations and small-terminal behavior above are
now the approved contract. No open feature questions remain.

The new [terminal-ui plan](tasks/plan.md) and [tasks](tasks/todo.md) require the
single build-plan checkpoint, now approved for autonomous implementation. The completed
provider plan cannot authorize TUI code. Agent-interface remains a separate spec.

Implementation completed under the approved plan. See [operation](docs/BOT_TUI.md),
[acceptance and review limits](docs/verification/terminal-ui.md), and
[ADR-003](docs/decisions/003-textual-terminal-client.md).
