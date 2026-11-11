# Spec: trading bot TUI initiative

Status: Draft provider specification awaiting user approval. No implementation
is authorized by this document alone.

## Objective

Help the human operator answer “Is the bot working as intended, and does
anything need my attention?” Give humans and agents the same bot state and
diagnostic context, with current trades, strategies, errors, findings, clear
paper/live labels, and basic controls. Closing the TUI must never stop trading.

Source: [confirmed intent](docs/intent/trading-bot-tui.md).
Approved decomposition: [CAPABILITY-MAP.md](CAPABILITY-MAP.md).

## Active module and build selection

Active module: **runtime-access**.
Its complete requirements and acceptance criteria are in
[SPEC-runtime-access.md](SPEC-runtime-access.md).

When `/build auto` reads this file, plan and implement only `runtime-access`
after the user approves that spec and the build plan. Do not infer approval
for the two client modules or silently implement them from their names.
Their specs follow approval of the provider contract, in dependency order.
Completing runtime-access alone does not deliver the requested TUI.

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
Its commands distinguish existing verification commands from proposed commands
that do not exist yet. No application tests or build have run during drafting.

## Review decisions

Review the following proposed behavior before approving the active module:

1. Start/pause/resume/stop act on the whole bot session, not individual strategies.
2. Pause stops strategy decisions. Existing orders may fill; market processing
   and risk handling continue. Pause is not a guarantee of zero trading activity.
3. Start uses the service's fixed validated configuration and always creates a
   new session/output directory. It does not resume an old ledger or position.
4. An explicitly launched local service owns the bot. Opening a client does
   not launch the service or start trading. SSH/tmux or an existing process
   manager keeps the service independent of a client terminal's lifecycle.

Approve or revise the provider specification before planning or implementation.
