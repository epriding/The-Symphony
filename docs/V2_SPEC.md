# v2 trading bot technical specification

The architecture in [ARCHITECTURE.md](../ARCHITECTURE.md) defines the target system. This document defines the v2 initiative boundary and the stable module IDs used by its child specifications. A child specification owns its module's requirements and acceptance criteria.

## Objective

v2 provides a shared architecture for strategies that observe Polymarket US and Kalshi, run against recorded data, and place paper orders through shared risk and venue gateways. The first delivery is raw capture for after-session review. Later modules build on the records it creates.

The bot must preserve the information available to a strategy at decision time. It must route each strategy order through portfolio risk and a venue gateway. It must retain strategy attribution on every order, fill, and profit-and-loss record.

## Capability map

| Module ID | Responsibility | Depends on |
| --- | --- | --- |
| `raw-capture` | Write versioned raw inputs and capture integrity records without delaying venue feeds. | None |
| `event-time` | Define normalized events, receipt and delivery timing, and the replay clock. | `raw-capture` |
| `market-data` | Deliver normalized events through a bus and discover markets through universe providers. | `event-time` |
| `paper-gateway` | Provide per-account paper order management, account checks, and depth-aware simulated fills. | `market-data` |
| `portfolio-risk` | Apply global limits, track atomic reservations, and operate the persistent kill switch. | `paper-gateway` |
| `live-readiness` | Reconcile venue accounts and enforce shared per-account rate limits before any future real order routing. | `portfolio-risk` |
| `processed-capture` | Record normalized events and gateway decisions alongside raw captures. | `raw-capture`, `portfolio-risk` |
| `replay-import` | Replay complete captures and import external historical data into the capture format. | `processed-capture` |
| `control-api` | Expose process state and controls to the TUI and other clients. | `portfolio-risk`, `paper-gateway` |
| `outside-data` | Publish external feeds and staleness events through the market-data bus. | `market-data` |
| `multiprocess` | Connect strategy processes to the core process through a network transport. | `control-api`, `live-readiness` |

Build modules in this order. `raw-capture`, `event-time`, and `market-data` come first. `paper-gateway` and `portfolio-risk` follow. `live-readiness` completes the paper-to-live safety boundary. The remaining modules may proceed when their listed dependencies are complete.

## Shared rules

- Initial venues are Polymarket US and Kalshi.
- Strategies use only a bus client and a gateway client. They neither call venue APIs nor retain private order state.
- A replay delivers events in recorded availability order. It advances the strategy clock to each record's availability time and preserves separate source and receive times.
- Capture assigns the receipt sequence before queueing. It records queue overflow, storage failures, and unverified tails as incomplete ranges. Trading continues during those failures.
- The portfolio risk manager reserves exposure atomically before sending orders. It holds an unknown order's reservation until reconciliation confirms its outcome.
- The kill switch persists across restarts, blocks new and queued orders, requests cancellations, and requires both confirmed outstanding-order resolution and an explicit operator reset before trading resumes.
- The first gateway release supports paper execution only. Future live routing requires account reconciliation, global reservations, the kill switch, and shared rate limiting.

## Technology and commands

The repository uses Python 3.12, `pytest`, and asyncio. Existing modules remain the source for compatible representations until a child specification replaces their boundary.

```bash
venv/bin/python -m compileall -q main.py src examples tests
venv/bin/python -m pytest -q
venv/bin/python main.py smoke --output /tmp/bot-smoke
venv/bin/python main.py replay --capture /tmp/bot-smoke --output /tmp/bot-replay
```

Source code belongs under `src/`. Tests belong under `tests/`. Lasting operator documentation belongs under `docs/`. Each retained child specification lives beside this parent as `docs/V2_<MODULE>_SPEC.md`, with a readable companion named `docs/V2_<MODULE>_SPEC.human.md`.

Use explicit, typed dataclasses for records that cross module boundaries. Each record constructor must validate values at the boundary. Preserve timestamp and sequence fields as named values instead of passing unstructured dictionaries between modules.

```python
@dataclass(frozen=True, kw_only=True)
class RawMessage:
    seq: int
    venue: str
    stream: str
    connection_epoch: int
    received_wall: float          # time.time(): comparable across machines, may jump
    received_monotonic_ns: int    # time.monotonic_ns(): never goes backward, process-local
    payload: str | bytes          # text frames stay text; only binary frames are bytes
```

## Testing strategy

Each module supplies focused tests under `tests/` for its contract and failure paths. Integration tests cover composition with the existing smoke and replay flows. For any delivery guarantee, tests must exercise retry, full queues, writer errors, partial files, and process restart where they apply. The existing full test suite, source compilation, synthetic smoke run, and replay remain required checks.

## Boundaries

Always preserve strategy attribution, capture versioning, receipt ordering, and compatibility for existing capture readers unless a child specification includes an explicit migration. Keep queues bounded. Keep venue calls out of strategies.

Ask first before adding dependencies, changing continuous integration, introducing a database or network service, changing existing capture schemas, or enabling real order routing.

Never commit credentials, silently discard a capture gap, allow a strategy to call a venue trading API, or let a kill-switch reset resume trading without operator action.

## Success criteria

- Each module has an approved human and technical specification before implementation begins.
- Each completed module meets its child specification and passes its stated checks.
- The existing paper-trading smoke and replay commands pass throughout the migration.
- The first completed module records raw venue messages and lets an operator identify incomplete capture ranges after a session.

## Open questions

- Per-account Polymarket US and Kalshi rate limits must be measured from current venue documentation before `live-readiness`.
- `multiprocess` must select either gRPC or a message broker.
- Cross-venue recovery must define its hedge, wait, or unwind policy before a strategy can submit paired live orders.
