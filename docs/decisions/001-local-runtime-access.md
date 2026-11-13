# ADR-001: Keep bot ownership independent of local clients

## Status and date

Accepted, 2026-11-11. The module boundaries and provider contract were approved
before implementation. See [the confirmed intent](../intent/trading-bot-tui.md)
and [capability map](../../CAPABILITY-MAP.md).

## Context

Humans and agents need the same bot status, trade context and basic controls.
Closing a terminal client must not interrupt trading. The existing Python
runner already owns execution, risk, account state, bounded history and replay;
creating another mutable owner would introduce inconsistent answers and races.
The supported runner is paper-only, even when its market source is public.

## Decision

Use one explicitly launched foreground service with a private Linux Unix socket.
The bot remains the sole mutable owner. Observations are copied at serialized
runner boundaries and bounded separately; external controls request transitions
instead of mutating account internals. Service/session identities reject stale
commands. Small controls have a separate queue so slow readers or diagnostic
writes cannot prevent stop. Shared plain-text findings are separate artifacts,
not causal inputs or executable agent instructions.

Existing user/SSH access and Unix ownership are the access boundary. Laptop
users run clients on the bot host through SSH. No public listener, new login,
agent role policy, daemon installation or automatic process/account resume is
introduced. The clients depend on this provider, not on each other.

Operator-enabled runs use causal capture schema 4; ordinary existing commands
retain their established schema 3 production path. Input schemas 1–4 remain
readable. Explicit pause/resume records invalidate pre-pause callback results
and are interpreted by replay; pause does not suppress existing risk handling.

## Alternatives considered

- Embedding trading in the TUI would make the terminal its process owner and
  couple client crashes or disconnects to trading.
- Scraping logs independently would give human and agent clients different
  partial answers and require growing-history scans for routine status.
- A new public web/TCP service would add a network/authentication system beyond
  the approved first version.
- A second account database would duplicate the runner's ledger rather than
  provide an observation boundary.

## Consequences

The operator must host the service independently, for example in an existing
persistent terminal. Attaching is inert; starting a fresh account is explicit.
Linux/SSH access and same-user authority are deliberate v1 limits. Read bursts
can receive busy responses under the conservative memory reservations; consumers
retry observations, never mutations whose outcomes are unknown. The provider
does not itself implement the full-screen TUI, agent reasoning or repairs.

## Review follow-up, 2026-11-12

Independent GPT-6 Astra review found that waiting for pause cancellation inside
the driver could stall market/risk work, and that asyncio's interpreter cleanup
could hang on nonterminating owned tasks after the service reported failure.
Cancellation now progresses across ordinary driver ticks. The standalone CLI
uses a failed-cleanup exit policy only after ordinary cleanup deadlines, while
the embeddable service reports an exception without exiting its host. Actual
subprocess tests never release the failed callback/I/O blocker and require exit
code 1. Encoding reservations also survive request timeout until worker completion.
