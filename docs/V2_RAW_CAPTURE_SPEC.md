# v2 raw-capture technical specification

Module ID: `raw-capture`. Parent: [V2_SPEC.md](V2_SPEC.md). Readable companion: [V2_RAW_CAPTURE_SPEC.human.md](V2_RAW_CAPTURE_SPEC.human.md).

## Objective

Record every inbound WebSocket data frame from Polymarket US and Kalshi exactly as received, before any parsing, so a session can be re-examined after an adapter bug is fixed. Capture must never delay a venue feed, and it must never present a capture with missing records as complete.

## Scope

In scope:

- Inbound WebSocket data frames on the Polymarket US markets stream and the Kalshi public stream.
- A bounded, non-blocking capture tap, a background writer, a raw-capture manifest, and a validator.
- Opt-in capture through an optional `raw_capture` section in the bot config file, used by `main.py run`.
- Capture schema version 5, which allows that section in the config embedded in paper-trading captures.

Out of scope for this slice:

- Outbound frames and WebSocket control frames (ping, pong, close).
- Normalized-event capture and replay from raw captures.
- Any change to the record format of `inputs.jsonl`, or to `manifest.json` beyond the schema version 5 rule below.
- REST responses. This was decided after review: settlement polling and market-metadata responses are not recorded in this slice. Whether to record them is decided before the `processed-capture` specification.
- The `serve` and `smoke` commands, databases, Parquet, and dashboards.

## Directory layout

Raw capture writes only inside a `raw/` subdirectory of the session output directory:

```text
<output>/
  manifest.json      existing paper-trading capture (schema_version 5 when raw_capture is configured)
  inputs.jsonl       existing paper-trading capture, unchanged
  raw/
    manifest.json    raw-capture status and integrity summary
    records.jsonl    message and gap records, one JSON object per line
```

Raw capture does not write any file outside `raw/`. It creates `raw/` exclusively and refuses to start if `raw/` already exists. A session-level manifest that links raw and processed captures belongs to `processed-capture` and must not reuse the name `manifest.json`.

## Record format (`records.jsonl`, format version 1)

Each line is a UTF-8 JSON object serialized with sorted keys, compact separators, `ensure_ascii=False` and `allow_nan=False`, terminated by `\n`. The `type` field selects one of two shapes.

Message record:

```json
{"v":1,"type":"message","seq":17,"venue":"kalshi","stream":"public_ws","connection_epoch":3,
 "received_wall":1795190587.412,"received_monotonic_ns":81234567890123,
 "encoding":"utf8","payload":"{\"type\":\"orderbook_delta\",...}"}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `seq` | int ≥ 1 | Raw-capture receipt sequence, assigned by the tap before queueing. Unique and strictly increasing within one capture. |
| `venue` | `polymarket_us`, `kalshi`, `synthetic` | Source venue. |
| `stream` | string, ≤ 64 chars | Logical stream name, such as `markets_ws` or `public_ws`. Never a URL, header, or credential. |
| `connection_epoch` | int ≥ 0 | The adapter's connection counter, so reconnects are visible. |
| `received_wall` | finite float | `time.time()` when the tap was called. Comparable across machines, but it can jump. |
| `received_monotonic_ns` | int | `time.monotonic_ns()` at the same moment. Only moves forward, but means nothing outside this process. |
| `encoding` | `utf8` or `base64` | `utf8` when the frame arrived as `str` (text frame). `base64` (standard alphabet, padded) when it arrived as `bytes`. |
| `payload` | string | The frame content. For `utf8`, the received string unchanged. For `base64`, the encoded bytes. |

Gap record:

```json
{"v":1,"type":"gap","first_seq":18,"last_seq":40,"reason":"queue_full","detected_wall":1795190588.001}
```

`reason` is one of:

- `queue_full`: the tap dropped the frames because the queue was at capacity.
- `oversize`: the payload exceeded `max_payload_bytes`, so the tap stored no content.
- `writer_error`: the frames were accepted but never durably written.

A gap record appears in the file before any message with a higher `seq`.

Decoding a `utf8` payload with `json.loads` returns a string equal to the received frame. Decoding a `base64` payload returns the received bytes. That round-trip is the exactness guarantee.

## Manifest (`raw/manifest.json`)

```json
{
  "format": "raw-capture", "format_version": 1,
  "session_id": "...", "code_revision": "...",
  "started_wall": 1795190500.0, "ended_wall": null,
  "status": "writing",
  "finalized": false, "complete": false,
  "config": {"queue_capacity": 10000, "queue_bytes": 16777216, "max_payload_bytes": 262144,
             "fsync_interval_seconds": 1.0, "retry_interval_seconds": 1.0},
  "max_line_bytes": 1573888,
  "message_count": 0, "gap_count": 0, "missing_count": 0,
  "dropped": {"queue_full": 0, "oversize": 0, "writer_error": 0},
  "last_assigned_seq": 0,
  "records_sha256": null, "records_bytes": null
}
```

- `status` is one of `writing`, `degraded` (a writer error is unresolved), `closed`, `failed` (closing could not complete).
- `finalized` becomes true only after a clean close: the writer drained, wrote the final gap record, fsynced `records.jsonl`, and recorded `records_sha256` and `records_bytes`.
- `complete` is `finalized and missing_count == 0`.
- `max_line_bytes` is `6 × max_payload_bytes + 1024`. That covers worst-case JSON escaping and the envelope fields.
- The writer replaces the manifest atomically (write a temporary file, fsync it, `os.replace`, fsync the directory). It does so at start, at every `status` change, and at close. It does not rewrite the manifest for each dropped frame. Gap detail lives in `records.jsonl`.

## Components

Code lives in a new package `src/raw_capture/`:

- `records.py`: frozen, keyword-only dataclasses `RawMessage` and `RawGap`. Their constructors validate the field rules above. They also handle serialization to and from one JSON line.
- `capture.py`: `RawCapture`, which owns the tap, the queue, and the writer thread.
- `validator.py`: `validate_raw_capture(directory) -> RawCaptureReport` and `open_raw_capture(directory)`. The second returns an iterator of records and raises `ValueError` unless the capture is complete.

```python
@dataclass(frozen=True, kw_only=True)
class RawMessage:
    seq: int
    venue: str
    stream: str
    connection_epoch: int
    received_wall: float
    received_monotonic_ns: int
    payload: str | bytes          # encoding is derived from the type
```

### Tap: `RawCapture.offer(venue, stream, connection_epoch, payload) -> None`

- Called synchronously from the event-loop thread, at frame arrival, before any parsing.
- Stamps both clocks, assigns `seq = last_assigned_seq + 1`, then calls `queue.Queue.put_nowait`.
- If `payload` exceeds `max_payload_bytes` (measured as UTF-8 bytes for `str`), the tap enqueues a content-free oversize marker for that `seq`.
- The queue is bounded by count (`queue_capacity`) and by payload bytes (`queue_bytes`). The tap tracks queued bytes under a lock; the writer subtracts each item's bytes when it takes the item. If either limit would be exceeded, the tap increments `dropped.queue_full` and returns. The writer infers the gap from the jump in sequence numbers.
- It never blocks, never performs I/O, and never raises because of capture state (full queue, degraded or failed writer, or after close). A programming error in the arguments, such as a wrong type, may raise.

### Writer thread

- A single daemon thread owns `records.jsonl` and `raw/manifest.json`. No other thread writes either file.
- It takes items with a timeout so it can fsync every `fsync_interval_seconds` even when idle.
- On seeing `seq` greater than `expected_seq`, it first writes `RawGap(first_seq=expected_seq, last_seq=seq-1, reason="queue_full")`.
- It updates the running SHA-256 only after each line is handed to the file. After each successful fsync it keeps a checkpoint: the byte offset, the last `seq`, and a `hashlib` digest copy.

### Writer failure and recovery

The trigger is an `OSError` from write, flush, or fsync.

1. Set `status = degraded` and attempt a manifest update. That update can fail, and the failure is ignored.
2. Every `seq` after the last fsync checkpoint is now unverified. Keep pulling items from the queue and discarding them, counting each into `dropped.writer_error`, so the queue does not fill up.
3. Every `retry_interval_seconds`, attempt recovery:
   - truncate `records.jsonl` to the checkpoint offset and restore the checkpoint digest;
   - write `RawGap(first_seq=checkpoint_seq+1, last_seq=<last discarded seq>, reason="writer_error")`;
   - fsync.
4. On success, set `status = writing` and continue. The capture can no longer be `complete`.

### Close: `RawCapture.close(ended_wall, timeout) -> bool`

- Called after all feeds have stopped, so `last_assigned_seq` is final.
- Signals the writer through a stop event, not through the queue, so a full queue cannot block shutdown.
- The writer drains the queue and writes a trailing `queue_full` gap if `last_assigned_seq` exceeds the last item it saw. It then fsyncs, records the digest and byte count, and writes the manifest with `status = closed` and `finalized = true`.
- If the writer is still degraded at close, it makes one final recovery attempt. If that fails, it writes `status = failed` with `finalized = false`, if it can.
- If the thread has not finished within `timeout`, `close` returns `False` and the capture stays unfinalized. Bot shutdown continues either way.

### Hard crash

After `kill -9` or a power loss, frames that were sequenced but not yet written cannot be counted, because `last_assigned_seq` was never persisted. The validator reports such a capture as unfinalized, gives the last durable `seq`, and flags a truncated final line if one exists. It does not invent a missing range.

## Venue integration

- **Kalshi** (`src/api/interfaces/kalshi.py`, `stream`): for each `aiohttp.WSMsgType.TEXT` frame, call `offer(..., frame.data)` immediately after the frame is read, before the size check and `json.loads`. Non-text frames are not captured.
- **Polymarket US** (`src/api/interfaces/bounded_public.py`, `BoundedMarketsWebSocket._handle_message`): call `offer(..., data)` as the first statement, before the size check and `_finite_json`. The SDK's unbounded `sdk.ws.markets()` path does not expose raw frames. Starting a run with raw capture enabled and no `max_event_bytes` limit is a configuration error.
- Adapters receive an optional `raw_capture` attribute, which is `None` by default. With `None`, behavior is unchanged.

## Configuration and CLI

Raw capture is configured in the bot config file, in a new optional top-level section. It does not go inside `runtime.limits`, which keeps its strict exact-key check.

```json
"raw_capture": {
  "enabled": true,
  "queue_capacity": 10000,
  "queue_bytes": 16777216,
  "fsync_interval_seconds": 1.0
}
```

```bash
venv/bin/python main.py run --config CFG --output OUT
venv/bin/python main.py raw-validate --capture OUT
```

Validation, in `src/paper_trading/runtime_config.py`:

- `validate_bot_config` adds `raw_capture` to its supported top-level keys. When the section is absent, raw capture is off and behavior is unchanged.
- When the section is present, it must contain exactly the four keys above:
  - `enabled` is a bool;
  - `queue_capacity` and `queue_bytes` are positive integers;
  - `fsync_interval_seconds` is a positive finite number.
- `queue_bytes` must be at least `max_payload_bytes`, so a single maximum-size frame can always be queued.
- `queue_bytes` is added to the existing `required_bytes` estimate that is checked against `memory_budget_bytes`. A count-only bound would not work: the example configuration allows 256 KiB frames under a 64 MiB budget, so 10,000 of them would need about 2.6 GB.
- `raw_capture.enabled = true` requires `runtime.limits.max_event_bytes`. Without it, the Polymarket bounded WebSocket path that exposes raw frames is not used.
- A `serve` session rejects any `raw_capture` section in this slice, even one with `enabled: false`, so a version 4 capture can never contain it (see Capture schema version).

Runtime behavior:

- When `enabled` is true, `run` creates `OUT/raw/` before feeds start. If the directory cannot be created, the run fails before any trading begins.
- `max_payload_bytes` equals `runtime.limits.max_event_bytes`.
- The provisional defaults shown above (10,000 items, 16 MiB) go in `examples/bot_config.example.json`. The implementation plan confirms or replaces them with a synthetic burst test.
- `retry_interval_seconds` is fixed at 1.0 in this slice and is not configurable.

## Capture schema version

The recorder copies the full runtime config into the paper-trading `manifest.json`, and replay validates it again when it opens a capture. Adding the `raw_capture` section is therefore a capture-schema change, and it gets a new version. There is no recorded production data yet, so no migration of existing sessions is needed. Older versions still load so that existing tests and `tests/fixtures/paper_trading/history_order_capture.json.gz` keep working unchanged.

- A runtime capture whose config contains a `raw_capture` section is written with `schema_version = 5`. Captures without the section keep writing 3 (`run`) or 4 (`serve` with operator controls), exactly as today.
- Version 5 means "version 3 plus an optional `raw_capture` config section". Operator-controlled sessions with raw capture would need a combined version, and are out of scope for this slice.
- `SUPPORTED_SCHEMA_VERSIONS` in `src/paper_trading/recording.py` becomes `{1, 2, 3, 4, 5}`. Every branch in `recording.py` and `replay.py` that currently checks for `(3, 4)` or for 3 treats 5 the same as 3.
- Replay never performs raw capture. When it rebuilds the session config from a version 5 manifest, it removes the `raw_capture` section before running, so a replay never creates `raw/`.
- Code from before this change rejects a version 5 capture with its existing "Unsupported or incomplete capture" error. It does not fail later on an unknown config key.

`raw-validate` prints a JSON report and exits with:

- `0` when the capture is complete;
- `1` when it is structurally valid but incomplete (gaps, unfinalized, or `failed`);
- `2` when it is invalid (unknown format or version, malformed line, sequence violation, checksum or count mismatch).

## Validator rules

The report contains: `format_version`, `status`, `finalized`, `complete`, `message_count`, `first_seq`, `last_seq`, `last_assigned_seq`, `missing_ranges` (each with `first_seq`, `last_seq`, `reason`), `dropped`, `checksum` (`ok`, `mismatch`, `unavailable`), `truncated_tail`, `errors`.

- The manifest must be at most 16 MiB, with `format == "raw-capture"` and a supported `format_version`.
- Each line must be at most `max_line_bytes`, end with `\n`, parse as JSON, and construct a valid `RawMessage` or `RawGap`. Exception: an unfinalized capture may end in one truncated line, reported as `truncated_tail`.
- Message `seq` values and gap ranges together must cover `1..N` exactly once, in increasing order, with no overlap. `N` is the highest covered `seq`.
- When `finalized` is true:
  - `N` must equal `last_assigned_seq`;
  - `records_sha256`, `records_bytes`, `message_count`, `gap_count`, `missing_count` and the `dropped` totals must match the file;
  - no truncated tail is allowed.
- The validator reads in bounded memory: it streams the file once and keeps only counters, the digest, and the missing-range list.

## Testing

New tests in `tests/test_raw_capture_*.py` use a synthetic in-process feed with `venue = "synthetic"`. They cover:

1. **Round trip.** `str` payloads, including non-ASCII, quotes, backslashes and control characters, and `bytes` payloads come back exactly from `open_raw_capture` in `seq` order.
2. **Queue overflow.** With the writer paused and a small `queue_capacity`, and separately a small `queue_bytes`, `offer` returns without blocking. The result has `queue_full` gaps whose counts match `dropped.queue_full`, and `complete` is false.
3. **Trailing drop at close.** Frames dropped after the writer's last item produce a trailing gap.
4. **Oversize.** An oversize frame produces an `oversize` gap and no payload.
5. **Writer failure.** An injected `OSError` on write and on fsync never blocks or raises in `offer`. It produces a `writer_error` gap covering the unverified tail and recovers once the error stops. A failure that persists through close yields `status = failed`.
6. **Close under a full queue.** Close finishes within its timeout.
7. **Validator.** Covers each invalid case: corrupted byte, removed line, reordered lines, a duplicate or overlapping `seq`, an unknown version, an oversize line. Also an unfinalized capture with a truncated tail, which must exit `1` and not `2`.
8. **Capture schema.**
   - A run with `raw_capture` configured writes `schema_version = 5`, and that capture loads with `read_capture` and replays without creating `raw/`.
   - Versions 1–4, including the committed history fixture, still load and replay unchanged.
   - Invalid `raw_capture` sections are rejected: missing or extra keys, wrong types, `queue_bytes` below `max_payload_bytes` or over the memory budget, enabled without `max_event_bytes`, any section in a `serve` session.
   - A version 3 or 4 manifest that contains the section is rejected, and so is a version 5 manifest without it.
9. **Adapter taps.** Each tap is called before parsing, and that includes frames the normalizer ignores or rejects.

The parent spec's required checks also pass: `compileall`, the full `pytest` suite, `smoke`, and `replay`.

## Acceptance criteria

- `main.py run` with `raw_capture.enabled = true`, against a synthetic or live public feed, produces `raw/` with a finalized, complete capture on clean shutdown. `raw-validate` exits `0`.
- Injected overflow, oversize and writer errors each produce correctly labelled missing ranges. Trading continues, and `raw-validate` exits `1`.
- Tampering with `records.jsonl` causes `raw-validate` to exit `2`.
- Captures without a `raw_capture` section are written and read exactly as before. Version 5 is the only schema change.

## Open questions

- Real Polymarket US and Kalshi message rates and payload sizes, to set `queue_capacity` and decide whether chunked checksums or file rotation are needed. Until then, there is one file and one digest.
- Whether `processed-capture` links normalized events to raw records through `seq` (which requires the adapters to carry it forward) or through timestamps and connection epoch.
- Whether REST responses, such as settlement polling, get raw capture in a later slice. Decide before writing the `processed-capture` specification. Doing so needs a request/response record type, credential redaction for signed requests, and hooks beneath the SDK.
