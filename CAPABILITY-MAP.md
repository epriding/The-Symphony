# Trading bot TUI capability map

Status: Module boundaries, dependency direction, and first-version assumptions
approved by the user in the feature interview. Module specifications require
their own review before implementation.

Intent: [confirmed intent](docs/intent/trading-bot-tui.md).

| Module id | Responsibility | Depends on |
|---|---|---|
| runtime-access | Independent bot session lifecycle; shared bounded status, strategy, trade, error and finding data; validated control requests | Existing BotRunner |
| terminal-ui | Human overview, strategy detail, visible findings, execution-mode labels and basic controls; detach without stopping trading | runtime-access |
| agent-interface | Machine-readable access to the same state and controls; publish diagnostic findings for display | runtime-access |

Build order: `runtime-access` first, then `terminal-ui` and `agent-interface`.
Clients do not depend on each other. These are modules in the existing Python
project, not three separately deployed services.

## Approved first-version assumptions

- Integrate the existing paper-only runner. Show PAPER explicitly and report
  real-money execution as unsupported. Public market data is not live trading.
- Laptop access uses SSH to run the terminal client on the bot's machine.
  There is no new public network service or authentication system.
- Agents use structured commands against shared state rather than terminal
  keystrokes. Findings can be published; agent reasoning and repair are external.
- Retain Python and existing test conventions. Propose any new TUI dependency
  and version in its module spec before installing it.

## Specification index

| Module | Spec | State |
|---|---|---|
| runtime-access | [SPEC-runtime-access.md](SPEC-runtime-access.md) | Implemented and verified; 450 tests passed |
| terminal-ui | [SPEC-terminal-ui.md](SPEC-terminal-ui.md) | Spec approved; build plan awaiting approval |
| agent-interface | Not written | Specify after the provider contract is approved |

The root [SPEC.md](SPEC.md) selects the active module for `/build auto`.
