---
type: feature
title: Bot terminal interface
feature: terminal-ui
status: current
code:
  - main.py
  - src/operator/tui.py
  - src/operator/client.py
---
# Bot terminal interface

The Textual terminal shows the independent service's health, strategies, trades
and diagnostic findings. Opening it observes the service; it does not start
trading. `q` detaches without stopping the bot. All execution is **PAPER**;
public market data does not enable real-money execution.

## Run locally or over SSH

Use Python 3.12 and install the checkout's dependencies into its existing
virtual environment:

```bash
venv/bin/python -m pip install -r requirements.txt
```

In a persistent terminal on the bot host, explicitly launch the offline service:

```bash
venv/bin/python main.py serve --smoke \
  --socket "$HOME/.local/state/symphony/control.sock" \
  --output-root "$HOME/.local/state/symphony/runs"
```

In a second terminal in the same checkout, attach:

```bash
venv/bin/python main.py tui \
  --socket "$HOME/.local/state/symphony/control.sock"
```

The service begins idle. Press `s`, read the confirmation and type `start` to
create a new synthetic paper session. Smoke is a finite demonstration; it is
not a continuously trading strategy. Each start creates a fresh account and
capture, rather than resuming previous positions. For configured public
observation and service limits, read [the service guide](BOT_CONTROL.md).

From the laptop, SSH to the bot host and run the same attach command there.
Keep the service in your existing persistent terminal/process manager so an
SSH disconnect does not terminate its process. The client uses a private Unix
socket on that host; there is no TCP listener or new login system.

A real terminal and at least **100 columns by 28 rows** are required for the
main view. Smaller terminals show a resize message and allow detach; controls
are disabled until the terminal is large enough. Nonterminal input/output is
rejected with exit code 2. `q` returns 0; Ctrl-C returns 130. Handled failures
restore terminal modes and return a nonzero code. SIGKILL cannot run terminal
cleanup, although the independently running bot continues.

## Read and navigate

The overview distinguishes service lifecycle, operator state, PAPER execution,
data source, readiness, observation availability, capture location and cleanup
problems. Missing values say `unknown`. Strategy rows include configured,
disabled and unrouted strategies; mode and reason explain their actual state.
Select a row to inspect its orders, positions, fills, events and findings.
The selected record pane exposes fields that do not fit in the table.

| Key | Action |
|---|---|
| Up/Down or `j`/`k` | Select a record or scroll the focused pane |
| Enter | Open a strategy, or focus selected-record details |
| Tab / Shift-Tab | Change collection in detail; move focus on overview or dialogs |
| `n` | Next page, when available |
| `b` | Return to the first page and request a refresh/reconnect |
| Esc | Return to overview, cancel a dialog or acknowledge an unknown outcome |
| `?` | Show keys and control semantics |
| `s` | Confirm start of a fresh session |
| `p` / `r` | Pause / resume strategy decisions |
| `x` | Confirm stop |
| `q` / Ctrl-C | Detach from any view |

Visible views refresh about once per second, without overlapping polls.
Routine refresh retains the current page. Pages show their revision, active or
recent scope, and retention/eviction information. Recent events, fills and
findings are bounded windows; an empty page does not prove no older activity.
Pages read at different revisions are not a single historical snapshot.
Decimal strings retain provider precision; truncated table cells can be read
in the scrollable record pane.

Connection age uses the client's monotonic clock. At three seconds without a
successful overview, the display says **STALE** and disables controls. This
age is separate from quote readiness: a fresh service response can still report
unavailable market observations. Reconnect performs reads only, with bounded
backoff up to five seconds. Each request has a five-second deadline.

## Control deliberately

All controls apply to the **whole session**, never an individual strategy.
Start and stop require typed confirmation and name the exact service/session.
A replacement target invalidates old confirmations and navigation. The client
never retargets an old command or automatically retries a mutation.

Pause gates strategy decisions. Existing orders can fill and risk processing
continues while paused. Stop requests the existing cleanup procedure, preserves
positions, and may report unresolved cleanup. Acknowledgement means the service
accepted a request; pending remains visible until observations show the state
change. Read the lifecycle, readiness and cleanup fields before assuming success.

A lost or malformed control reply can leave the outcome **unknown**. Observe
the service before acting again; Esc acknowledges the warning, not the command's
success. Applicable stop remains available while an earlier outcome is unknown.
Detaching does not revoke an already sent command, which may still finish.

## Display and boundaries

Remote text is rendered literally after credential redaction and control-byte
removal. Finding authors are labels, not authenticated identities. Evidence is
inert text: the terminal does not open paths, execute suggestions, diagnose with
an agent or repair code. Agent reasoning and the coding workflow stay external.
The shared socket contract and reusable Python client exist; the separate
structured agent CLI still needs its own specification and implementation.

Set `NO_COLOR=1` for monochrome operation. A non-UTF-8 output encoding selects
ASCII display, preserving raw identities in requests. For example:

```bash
NO_COLOR=1 PYTHONIOENCODING=ascii venv/bin/python main.py tui \
  --socket "$HOME/.local/state/symphony/control.sock"
```

The client bounds frames, decoded objects, pages and concurrent work. Its
8 MiB cooperative data/buffer budget excludes framework allocations and is
not a promise about process RSS. Request/response and provider limits are in
[the provider specification](../SPEC-runtime-access.md).

[Verification and acceptance trace](verification/terminal-ui.md) records the
tested behavior, actual PTY lifecycle checks and review limitations. No live
execution, strategy editing, repair workflow or deployment is included.
