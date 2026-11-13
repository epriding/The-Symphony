# Spec: trading bot TUI initiative

Status: Runtime provider and human Textual TUI implemented and verified.
Structured agent CLI specification is pending.

## Objective

Help the human operator answer “Is the bot working as intended, and does
anything need my attention?” Give humans and agents the same bot state and
diagnostic context, with current trades, strategies, errors, findings, clear
paper/live labels, and basic controls. Closing the TUI must never stop trading.

Source: [confirmed intent](docs/intent/trading-bot-tui.md).
Approved decomposition: [CAPABILITY-MAP.md](CAPABILITY-MAP.md).

## Active module and build selection

Completed module: **terminal-ui**, specified in
[SPEC-terminal-ui.md](SPEC-terminal-ui.md), approved by the user after commit
0612c95. Its new [plan](tasks/plan.md) and [task list](tasks/todo.md) have
received the single /build auto approval and have been executed. See the
[operating guide](docs/BOT_TUI.md) and [acceptance trace](docs/verification/terminal-ui.md).

[Runtime-access](SPEC-runtime-access.md) is complete: 450 tests and offline
verification passed. See [the service guide](docs/BOT_CONTROL.md).
Completing this provider alone does not deliver the requested TUI. The
`agent-interface` client still needs its own spec and approval.

## Shared constraints

- Python 3.12 on the Linux bot host; reuse the existing runner and ledger.
- Paper execution only in v1. Existing public observation remains available.
- SSH is the laptop access path; no listening TCP service, web app, or new
  authentication/session system.
- The service, TUI, and structured client share one owner and one contract.
- Strategy editing, strategy algorithms, order entry, live execution, agent
  reasoning, error repair, and automatic recovery are outside this initiative.

## Commands, structure, style, testing, and boundaries

The active module spec defines all six required specification areas: objective,
full commands, project structure, code style, testing strategy, and boundaries.
Its commands and focused test files are implemented. The structured agent CLI
has not been implemented.

## Review decisions

Approved provider behavior, preserved for the future clients:

1. Start/pause/resume/stop act on the whole bot session, not individual strategies.
2. Pause stops strategy decisions. Existing orders may fill; market processing
   and risk handling continue. Pause is not a guarantee of zero trading activity.
3. Start uses the service's fixed validated configuration and always creates a
   new session/output directory. It does not resume an old ledger or position.
4. An explicitly launched local service owns the bot. Opening a client does
   not launch the service or start trading. SSH/tmux or an existing process
   manager keeps the service independent of a client terminal's lifecycle.

Terminal-ui spec is approved. Its concrete build plan is also approved.

The approved [readability follow-up](tasks/tui-readability-plan.md) is also
complete: clearer view navigation, adjustable/expanded diagnostic views, and
structured finding severity filters. See [verification](docs/verification/terminal-ui-readability.md).
