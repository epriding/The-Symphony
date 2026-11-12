# ADR-003: Textual client with independent service lifetime

## Status and date

Accepted, 2026-11-12. The user selected Textual instead of curses and approved
the terminal-ui specification and autonomous build plan.

## Context

The operator needs one place to inspect health, strategies, current trades and
findings from the bot host or an SSH laptop session. Closing a terminal must
not stop market/risk processing. The runtime-access module already owns the
runner, account, snapshots and exact-target control protocol. Agent reasoning
and repairs remain in the external coding workflow.

## Decision

Use Python 3.12 with `textual==8.2.8`, without optional extras. Textual supplies
widgets, keyboard focus, modal confirmations, terminal restoration and headless
Pilot testing. Use its [workers](https://textual.textualize.io/guide/workers/)
for asynchronous requests and [testing API](https://textual.textualize.io/guide/testing/)
for interaction tests. Real PTYs separately verify terminal/process behavior.

The TUI is an attach-only client. A Textual-independent RuntimeClient consumes
the existing Unix-socket provider. Separate read/control lanes prevent slow
polling from occupying control I/O. Frames, JSON depth, decoded objects and
schemas are bounded and validated before data reaches widgets. Display strings
are sanitized; raw targets remain unchanged in requests.

Refresh only the visible collection, retaining one current page. Observation
age advances independently of network completion. Target/navigation generations
reject old replies. Start/stop confirmations freeze service/session identities;
controls are never retried automatically. Admission, observed completion and
unknown outcomes are distinct states.

## Alternatives considered

Curses would require more manual rendering, focus and interaction plumbing;
the user explicitly requested Textual's presentation instead. A browser UI
would introduce another transport and deployment surface beyond the approved
SSH workflow. Embedding the runner in the UI would couple trading lifetime to
the client and duplicate ownership. Automating terminal keystrokes for agents
would be less explicit than the shared structured provider contract.

## Verification and consequences

The [acceptance trace](../verification/terminal-ui.md) links transport, Pilot,
real-provider and PTY tests. Detach, Ctrl-C and client kill are followed by
reattachment to the same synthetic session with advancing inputs and risk.
An experimental mutation verifies detection of a command sent after closure.
The measured cooperative buffer reservation excludes Textual/RSS overhead.

Textual adds dependencies and terminal-specific behavior; pinning, dependency
checks and actual PTY tests cover the installed environment. SIGKILL cannot
restore terminal modes. SSH users must keep the service independently alive.
This module delivers the human UI; a dedicated structured agent CLI remains
separate. No provider/accounting protocol or live-execution capability changes.
