---
type: feature
title: Local bot control service
feature: runtime-access
description: Share bounded paper-bot observations and explicit controls across independent local clients
status: current
code:
  - main.py
  - src/operator/service.py
  - src/operator/protocol.py
  - src/operator/observations.py
---
# Local bot control service

The service owns one paper-bot session at a time and provides the same bounded
observations and controls to human and agent clients. Its process runs
independently of its clients. This is the provider module: the full-screen TUI
and structured agent command-line client are separate, not yet implemented.
The [approved module map](../CAPABILITY-MAP.md) records those boundaries.

## Start a service, then a session

Use Python 3.12 and the repository's existing dependencies. From the feature
checkout, launch an offline service in a persistent terminal you manage:

```bash
venv/bin/python main.py serve --smoke \
  --socket "$HOME/.local/state/symphony/control.sock" \
  --output-root "$HOME/.local/state/symphony/runs"
```

The service announces readiness on stderr and remains **idle**. It does not
load credentials or start a strategy until an explicit `start` request.
`--smoke` runs the finite synthetic fixture, not a trading algorithm or a
calibrated simulation. After it ends, the service retains the terminal view
and remains available; starting again creates a fresh paper account and capture.

For configured public observation or a deliberately code-registered strategy:

```bash
venv/bin/python main.py serve --config local-bot.json \
  --socket "$HOME/.local/state/symphony/control.sock" \
  --output-root "$HOME/.local/state/symphony/runs"
```

`--config` requires a public data source and `--smoke` is mutually exclusive.
The [runtime guide](BOT_RUNTIME.md) describes configuration, factory registration
and credentials. Public session start follows the existing dotenv behavior;
public data does not imply real-money execution. The bundled CLI registry still
has only the synthetic smoke factory; JSON cannot import code or enable live
execution. Observation-only configuration remains supported.

Use a dedicated directory owned by your user. Missing service directories are
created with mode 0700 and the socket has mode 0600. Existing insecure, foreign
or symlinked paths are rejected, not repaired automatically. An existing socket
is never taken over or deleted during startup. Verify ownership and that the
old service has stopped before manually dealing with a stale path.

The service is a foreground process. Keep it in an existing persistent terminal
or process manager; it does not install a daemon or unit. From a laptop, SSH to
the bot host and use a client there. Closing that **client** SSH session leaves
the independently hosted service running. Closing the terminal that hosts the
service itself is different. No public TCP listener or SSH implementation is
added by this feature.

## Inspect without starting anything

This standard-library probe sends only an overview request. It neither starts
the service nor starts trading:

```bash
venv/bin/python - "$HOME/.local/state/symphony/control.sock" <<'PY'
import json
import socket
import sys
from uuid import uuid4

request = dict(version=1, request_id=str(uuid4()), op="overview",
               service_id=None, session_id=None, params={})
with socket.socket(socket.AF_UNIX) as connection:
    connection.settimeout(5)
    connection.connect(sys.argv[1])
    with connection.makefile("rwb", buffering=0) as stream:
        stream.write((json.dumps(request) + "\n").encode("utf-8"))
        reply = stream.readline(1024 * 1024 + 1)
        if len(reply) > 1024 * 1024 or not reply.endswith(b"\n"):
            raise ValueError("Incomplete or oversized response")
        print(json.dumps(json.loads(reply), indent=2))
PY
```

## Wire contract

Each request/response is one UTF-8 JSON object plus newline on a private Unix
socket. All request fields below are required; `params` is an object. The first
overview discovers the target identities:

```json
{"version":1,"request_id":"inspection-1","op":"overview","service_id":null,"session_id":null,"params":{}}
```

Success responses include `version`, `request_id`, `ok: true`, `service_id`,
`session_id`, `revision`, `observed_at`, and `data`. Errors use `ok: false`,
`data: null` and `error: {code, message}`. Error codes are `invalid_request`,
`unsupported_version`, `not_found`, `conflict`, `busy`, `limit_exceeded`, and
`internal_error`. Messages do not include raw configuration or tracebacks.

For every subsequent operation, send the discovered service ID and current
session ID. Idle has a null session ID. A terminal session keeps its ID until
the next start. Initial discovery can be repeated when reconnecting. Stale
service/session targets are rejected before any effect; identifiers in trade
rows must be interpreted within their session, not as globally unique records.

| Operation | Parameters | Result |
|---|---|---|
| `overview` | Empty | Lifecycle/reason, operator state, source/mode, readiness counts, active order/position counts, capture association, snapshot clocks, recent diagnostics and service limits |
| `strategies` | Optional `limit`, `cursor` | Page of every configured strategy, including disabled/unrouted entries |
| `strategy` | Required `name`; optional `collection` | Strategy summary, or a page of `orders`, `positions`, `fills` or `events`; collections accept `limit` and `cursor` |
| `start` | Empty | Admission of one fresh configured paper session; reject if active or cleanup/diagnostic append remains unresolved |
| `pause` | Empty | Admission of global operator pause; observe until `paused` |
| `resume` | Empty | Resume eligible strategy work; cannot override readiness, disablement or stop |
| `stop` | Empty | Graceful stop request; observe terminal status to confirm cleanup; service remains running |
| `findings` | Optional strategy `name`, `limit`, `cursor` | Page of recent external findings |
| `publish_finding` | Required `author`, `severity`, `message`; optional strategy `name`, `evidence` | Accepted server-assigned finding after the private artifact append completes |

For example, an explicit start frame replaces the service ID with the value
from discovery; use null only if the service is idle, otherwise its terminal ID:

```json
{"version":1,"request_id":"start-1","op":"start","service_id":"REPLACE_WITH_SERVICE_ID","session_id":null,"params":{}}
```

A control response acknowledges **admission**, not completed startup, pause or
shutdown. Request IDs are correlation labels, not idempotency keys. If a socket
fails or a request times out, a mutation may already have happened. Observe
before deciding what to do; never automatically retry a mutation. A conflicting
target is not permission to discover a replacement and blindly repeat it.

Lifecycle is `idle`, `starting`, `ready`, `degraded`, `stopping`, `stopped` or
`failed`. Operator state is separately `running`, `pausing` or `paused`.
Start/pause/resume/stop affect the entire session, not an individual strategy.
Repeated pause/resume/stop are no-ops in their applicable states. Resume during
pausing or stopping conflicts. Stop is authoritative.

**Pause suspends strategy decisions, not all trading activity.** Pre-pause
callback results cannot act after the barrier or a later resume. Existing orders
may fill and existing market, report and risk processing continue; risk actions
can still occur. Pause does not cancel orders or liquidate inventory. Stop uses
the existing public/synthetic cleanup rules, preserves held positions and reports
unresolved obligations. A new start is never an account or process resume.
Cancellation acknowledgement is tracked across driver ticks: the `pausing`
interval also keeps processing market data, reports and risk, rather than
blocking the driver while callbacks clean up.

## Interpret observations and diagnostics

Strategies show route, actual execution mode, readiness/reason, operator state,
active-order and owned-position counts, and recorded activity/errors. Supported
execution mode is PAPER and `live_execution_supported` is false. A disabled,
unrouted entry can have a null mode; it is not a running live strategy.

Orders, fills and positions are separate collections. They include stable
identities, attribution, venue/market/outcome, side, relevant quantity/price,
status and available timestamps. Monetary/quantity fields are decimal strings;
missing values are null. Position ownership comes from the per-strategy ledger,
not a guessed owner of netted inventory. System activity is labeled separately.
The service does not invent timestamps, P&L or complete-history statistics.

`observed_at` is response wall time in UTC epoch seconds. `snapshot_at` is the
copied view's wall timestamp; `market_time` is the bot clock and can be synthetic.
Neither a fresh response nor an advancing bot clock proves fresh quotes. Check
readiness and reasons separately. Clients should measure connection age with a
local monotonic clock. Snapshots coalesce to at most ten routine refreshes per
second, with an explicit final refresh; a response is not a new ledger snapshot.
If a view cannot be produced, `observation_available` is false and counts can
be null; do not reinterpret that as zero inventory.

Pages default to 50 rows and allow 1–100. Use a non-null `next_cursor` only with
the same service, session, collection and filter. Cursors are opaque and expire
on session replacement. Pages are individually consistent, not a multi-page
transaction; each response includes its revision. Oversized pages can require a
smaller limit. `scope: active` means current active state; `scope: recent` means
only a bounded window. Recent pages and overview windows disclose retained
sequence ranges and eviction. Filtered strategy pages share their collection's
global retention window; an empty page is not proof there were no older trades.

Findings have a server ID, session ID, sequence/time, author display label,
optional strategy, severity (`info`, `warning`, `error`), message/evidence and
`source_request_id` correlation label. Author labels are not proof of identity.
Text is redacted for recognized secret patterns and stripped of terminal control
characters; it is never executed. Do not put credentials in findings: arbitrary
caller-supplied secrets cannot be recognized reliably. Evidence is an inert
label, not a path the service opens or a command it runs.

Accepted findings append to `operator_findings.jsonl` in that session's capture
directory, independently of causal input and account state. I/O failure rejects
publication, makes `finding_publication_available` false and can leave partial
or uncertain artifact evidence. Pending publication is visible and blocks
session replacement. An append that outlasts a client timeout may complete;
reconcile through findings and its source request label without treating the
label as a uniqueness guarantee. Prior artifacts remain after restart but are
not automatically reloaded into a new session. There is no repair agent here.

## Limits, shutdown and verification

The control profile bounds requests to 64 KiB, response wire frames to 1 MiB,
copied response objects to 512 KiB, clients to eight and total queued requests
to 32. Identifiers are at most 256 UTF-8 bytes; the configured capture prefix
is at most 128 bytes to leave room for generated suffixes. JSON depth and
decoded request sizes are bounded. Configuration/projection admission rejects
profiles too large for control mode before trading starts; resolved metadata
identifiers are checked before strategy activation.

The service reserves 16 MiB for its cooperative control-state/buffer model:
4 MiB for copied model/configuration, 5 MiB for bounded input/parser/small-control
buffers and up to two 3.5 MiB large-response builds/writes. Large reads can
return `busy` while eight clients remain attached. Small controls use a separate
bounded queue so blocked reads or diagnostic writes cannot starve stop. These
reservations are not a whole-process RSS ceiling; the existing runner has its
own budget and third-party/native allocations remain separate.
Encoding has at most eight owned jobs. A timeout/disconnect cannot cancel an
executing Python thread, so its read reservation remains occupied until the
actual worker finishes; completed response graphs are released before reuse.

Recent retention is 256 runtime events, 256 fills and 128 findings. Finding
message/evidence are each at most 2 KiB and author is at most 128 bytes. Sockets
and ordinary requests/writes have five-second deadlines. No new captures or
ledgers are made to satisfy a read. Disk artifacts can grow and are never
automatically pruned or deleted by this feature.

SIGINT/SIGTERM to the service requests shutdown and ordinary runner cleanup.
Service exit is 0 for clean closure, 2 for command/configuration/binding errors
and 1 for failed cleanup/runtime service failure. A failed bot session remains
observable without killing the independent service. Uncooperative callbacks
remain tracked as unresolved cleanup; positions and uncertain commitments are
not discarded to make shutdown look clean. Linux same-UID clients have the
operator's authority; this is not a sandbox for arbitrary strategy code or a
separate agent permission system.

If cleanup deadlines expire with owned callback or I/O work still pending, the
standalone `serve` CLI exits the service process with code 1 rather than entering
Python's unbounded interpreter task/thread joins. It does not delete captures,
claim finalized output, kill unrelated processes or restart the account. Already
clean captures can remain finalized even when separate diagnostic I/O fails.
The embeddable `ControlService.close()` reports failure by exception and never
exits its caller's process; its host owns any process-level escalation policy.

```bash
venv/bin/python -m pytest -q tests/test_bot_control.py tests/test_bot_service.py tests/test_bot_runtime_access.py tests/test_bot_history_order.py
venv/bin/python -m pytest -q
venv/bin/python -m compileall -q main.py src examples tests
venv/bin/python -m pip check
```

Tests use offline feeds, real Unix sockets, actual slow-reader backpressure,
blocked artifact writes and separate processes that kill/reconnect clients.
The [decision record](decisions/001-local-runtime-access.md) explains ownership
and access choices; [history schema 2](decisions/002-deterministic-history-order.md)
explains the archive/replay prerequisite. API and acceptance details live in
[the provider specification](../SPEC-runtime-access.md).
