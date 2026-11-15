# Local bot control protocol reference

This reference describes the service lifecycle, observation contract and bounded
Unix-socket protocol. For setup and commands, see [the service guide](BOT_CONTROL.md).
The implementation lives in [protocol.py](../src/operator/protocol.py),
[service.py](../src/operator/service.py) and
[observations.py](../src/operator/observations.py).

## Session ownership and lifecycle

- The service is initially `idle`, even if configured with enabled strategies.
  It validates configuration without loading credentials or connecting feeds.
  Public session start follows the existing dotenv/credential behavior; smoke
  never loads dotenv, resolves credentials, or uses external network transport.
- The service has a random `service_id` per process lifetime and a new
  `session_id` per run, separate from any existing configured capture identity.
- Exactly one session can be starting/running/stopping. Concurrent starts are
  serialized; at most one wins. Clients can connect concurrently.
- Start never overwrites output. Allocate a new child of the fixed output root
  for each run; retain the service/session IDs and resulting capture association.
  Do not accept configuration, factories, output paths, or Python expressions
  in RPC requests.
- After a session finishes, retain its last bounded view and terminal status
  until another explicit start or service shutdown. Before replacing a terminal
  session, release its runner, feeds and tasks. Do not accumulate past runners.
- New start means a new configured paper account. It is not a restart journal,
  reconnect of a crashed process, continuation of held positions, or replay.
  Existing captures and held-position evidence are retained on disk. Clients
  must show that starting again creates a new paper session.
- Losing all clients, an SSH connection, or the TUI never stops the service
  or changes the bot's operator state. No disconnect callback requests stop.
- SIGINT/SIGTERM sent to the service itself requests graceful session cleanup
  before closing the listener. Service failure and machine failure cannot be
  hidden as normal stops; no automatic process restart or account resume.

## Control semantics

Controls are **global to the current session**. Per-strategy controls are out
of scope. Keep readiness/failure separate from operator pause: resuming cannot
override stale feeds, disabled strategies, risk validation, or a stopping run.

| Operation | Allowed state and effect |
|---|---|
| start | Idle or terminal; launch a new run from the fixed configuration, initially unpaused. Reject while a run is active or unresolved cleanup remains. |
| pause | Starting/ready/degraded; prevent new strategy dispatch and admission of strategy results after the pause barrier. Acknowledge accepted, then expose `pausing` until callbacks quiesce; only then report `paused`. |
| resume | Active session; clear operator pause and arrange one fresh eligible snapshot notification. Repeated resume is a no-op. Resume during `pausing` returns conflict. |
| stop | Any active session; latch stop, cease strategy work, and use existing `request_stop()`/cleanup behavior. Repeated stop is a no-op; stop takes precedence over pause/resume. |

Repeated pause while pausing/paused is a no-op. Pause/resume against a terminal
session returns conflict. Stop against that same terminal session returns its
terminal status without starting or modifying anything.

Pause must invalidate results from callbacks dispatched before the barrier,
including callbacks that finish after a later resume. Use a dispatch generation
or equivalent serialized guard; cancellation alone is not proof that a result
cannot act. Cooperative callbacks must quiesce within configured shutdown grace;
an uncooperative callback fails the session and follows existing failure cleanup.

**Pause is not an order-cancel or liquidation command.** Existing orders may
fill, pending execution/report timers continue, feeds and readiness continue,
and the existing periodic risk policy remains active. Risk-originated actions
can still occur. Clients must display this distinction. Stop follows existing
public/synthetic cleanup differences and reports unresolved obligations honestly.
Neither control liquidates positions or releases uncertain commitments.

Record operator pause/resume and admission boundaries in the causal capture.
Extend schema/replay handling explicitly with compatibility for existing
captures; never silently ignore operator controls during replay. Manual control
must not invalidate deterministic replay of accepted decisions and accounting.

## Shared observation contract

Every response identifies the service, target session (nullable while idle),
schema version, observation revision, and UTC wall timestamp. Expose bot/market
clock separately: synthetic market time is not connection freshness. Clients
measure communication age locally with a monotonic clock. A successful read
advances the communication timestamp even if market data is stale; report
market readiness and reasons separately.

`overview` supplies lifecycle (`idle`, `starting`, `ready`, `degraded`,
`stopping`, `stopped`, `failed`), reason, operator state (`running`, `pausing`,
`paused`), source (`synthetic`/`public`), observe-only flag, strategy and venue
readiness counts, active order/position counts, bounded recent events/findings,
capture identity and cleanup outcome. Null means unavailable, not zero.

`strategies` includes every configured strategy, even disabled ones: name,
factory, enabled state, portfolio route, execution mode, readiness/reason,
operator state, active-order and position counts, most recent activity/error
when recorded. No activity or error is an explicit empty/null state.

`strategy` selects one exact configured name and returns its summary by
default, or a page of active orders, open positions, recent fills or events
when a collection is requested.
System/risk activity without defensible strategy attribution is labeled
system/portfolio activity, never attributed to an unrelated strategy.

Trade rows distinguish **orders**, **fills**, and **positions**. Include their
stable identity, strategy/portfolio attribution, venue, market/outcome, side,
quantity, limit/entry/fill price as applicable, lifecycle status and time when
available. Order rows include filled and remaining quantity. Missing data is
explicit; no fabricated fills, prices, timestamps, returns, or strategy P&L.
Use bounded decimal strings for quantity/money fields and finite numeric UTC
timestamps; preserve existing ledger precision without recomputing accounting.

Execution mode comes from the validated route/account, never from data source.
All supported strategies are PAPER. The contract declares
`live_execution_supported: false`; unexpected live routes fail validation,
not reinterpretation as paper. Do not expose inactive internal LIVE account
scaffolding as a running live strategy.

Views are copied consistently at the runner's serialized boundary, without
network/file waits under the ledger lock. No raw account, config, orderbook,
private credentials, or growing internal objects escape. Healthy idle clients
must not cause unbounded snapshots or per-client histories.

## Protocol and limits

Use newline-delimited UTF-8 JSON on a local Unix socket. Version 1 request:

```json
{"version":1,"request_id":"caller-42","op":"overview","service_id":null,"session_id":null,"params":{}}
```

The initial overview discovers identity. Subsequent reads and all mutations
require the discovered `service_id`; session-scoped operations require an exact
`session_id`. Start targets null if idle, or the current terminal session ID.
This prevents a stale start/stop from controlling a different session. Wrong
service/session returns conflict before any effect.

Success envelope: `version`, `request_id`, `ok: true`, `service_id`, `session_id`,
`revision`, `observed_at`, `data`. Error envelope uses `ok: false`, `data: null`,
and `error: {code, message}`; use null identities if unavailable. Allowed codes:
`invalid_request`, `unsupported_version`, `not_found`, `conflict`, `busy`,
`limit_exceeded`, `internal_error`. Do not return tracebacks or raw config.

Operations: `overview`, `strategies`, `strategy`, `start`, `pause`, `resume`,
`stop`, `findings`, and `publish_finding`. Unknown fields, operations, malformed
UTF-8, duplicate JSON keys, nonfinite numbers, and invalid types are rejected.
Request IDs are correlation values, not an exactly-once promise. Controls are
serialized and state-conditional as above. Never automatically retry a mutation
after timeout; its result is unknown until observation confirms the state.

The `params` object has these exact operation-specific fields. Reject extras.
Pagination fields are optional `limit` and `cursor` as defined below.

| Operation | Params and data |
|---|---|
| overview | Empty params. Data contains `lifecycle`, `reason`, `operator_state`, `source`, `observe_only`, `live_execution_supported`, strategy/venue counts, order/position counts, capture identity, cleanup outcome, and the latest eight events/findings with retained-range metadata. |
| strategies | Pagination fields only; data is a page of strategy summaries. |
| strategy | Required `name`; optional `collection` is `orders`, `positions`, `fills`, or `events`. Without collection return a summary; with collection accept pagination fields and return that collection's page. |
| start, pause, resume, stop | Empty params; data contains `accepted`, current `lifecycle`, and `operator_state`. This acknowledges admission, not completed startup, pause, or cleanup. Observe to determine completion. |
| findings | Optional exact strategy `name` filter and pagination fields; return a finding page. |
| publish_finding | Required `author`, `severity`, `message`; optional strategy `name` and plain-text `evidence` (2 KiB). Data is the accepted server-assigned finding. |

Strategy-scoped reads or findings with an unknown `name` return `not_found`.
Request IDs must be nonempty strings of at most 128 UTF-8 bytes. Limit JSON
nesting to 16 levels and convert decode/validation recursion failures to an
invalid request rather than terminating the service. Treat parser capacity as
part of the bounded service budget.

Each collection defaults to 50 rows, accepts limits 1–100 and an opaque cursor,
and returns `items`, `next_cursor`, `has_more`, `scope` and revision. Pages have
stable documented ordering (strategy name; object identity for active state;
sequence for events). Cursor binds service, session, collection and filters;
reject malformed/foreign cursors. Pages are individually consistent, not an
atomic multi-page transaction; return the actual revision on every page.
`scope` distinguishes current active state from bounded recent history. If
recent rows were evicted, expose the retained sequence range and an eviction
indicator. Clients cannot infer complete session history from a recent page.
No full-history browser or synchronous scan of capture files is added here.

Finite defaults: maximum request 64 KiB including newline; maximum response
1 MiB including newline; 8 connected clients; 32 queued requests; 256 retained
runtime events; 256 recent fill records; 128 retained findings; finding text
2 KiB, author 128 bytes, and IDs/names 256 bytes; 5-second read/write/request
deadlines. Strings and limits are measured in UTF-8 bytes. Reject oversized
frames before decoding; on an unframeable request close that connection.
Return busy/limit errors rather than blocking trading or silently truncating
active state. Disclose bounded recent-history eviction; never drop live ledger
obligations to satisfy observation limits. Configurations with identifiers or
control projections that cannot fit these bounds fail explicitly before start.

The DTO/event/finding storage must be bounded and charged separately from the
existing runner's memory accounting: at most 16 MiB retained service projections,
queues and client frame buffers. Reserve and validate this service budget before
accepting a run; enforce per-buffer bounds and release on disconnect/session
replacement. It does not claim a whole-process RSS ceiling. Serialization and
slow socket writes must never await within the market/ledger owner. Coalesce
redundant observation refreshes; process controls at driver boundaries so reads
cannot starve stop or periodic risk. No unbounded producer task per poll.

## Findings and diagnostic trust

Findings contain server-assigned ID/time/sequence, author label, severity
(`info`, `warning`, `error`), optional configured strategy, plain-text message,
optional bounded evidence label, and target session. Author is a display label,
not proof of provenance. Validate against the same session and text limits.
Keep findings in a bounded service-session ring and append accepted findings to
a separate capture artifact, `operator_findings.jsonl`; do not mix them into the
market/replay input stream. Failure to append rejects publication and reports
an operational error. Disk consumption is not automatically capped or deleted.
After a service crash/restart, prior finding artifacts remain but are not
automatically reloaded or asserted to be current.

Allowlist published fields and reuse the logger's redaction for operational
messages. Escape/remove terminal control characters before returning display
text; findings and venue text are untrusted data, never commands or instructions
to execute. Do not publish configuration/settings, environment values, headers,
private keys, or unrestricted traceback/file contents. Redaction is not a
guarantee against arbitrary caller-supplied secrets; document that findings
must not contain them. Publication cannot modify code, configuration, account
state, strategy readiness, or initiate a repair workflow.

## Local access and filesystem boundaries

Use a socket directory owned by the current user with mode 0700 and socket
0600. Create missing private directories deliberately; reject symlinks,
wrong-owner or insecure existing socket directories. Check peer UID on the
Linux host. Same-UID clients have the operator's authority; no separate agent
role or permission model is introduced.

Refuse to bind an existing socket path. Do not unlink an unknown/stale socket,
kill a PID from a file, delete captures, or take over another service. Cleanup
can remove only the socket created and still owned by this service (verify
identity), never a replacement entry. Allocate session directories under the
validated output root exclusively and refuse collisions/symlink escapes.
RPC never accepts filesystem write targets, shell commands, imports, or secrets.
