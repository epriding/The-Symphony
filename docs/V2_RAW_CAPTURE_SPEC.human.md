# Raw capture specification

## What this slice delivers

Raw capture gives us a faithful record of what the bot received from Polymarket US and Kalshi. It starts before normalized events, strategies, paper fills, or risk decisions. That order matters because a later bug in an adapter can be fixed by reading the original messages again.

Each capture session writes versioned records to a new output directory. Every accepted record includes a receipt sequence, wall-clock receive time, monotonic receive time, source identity, and raw message content. The receipt sequence begins before the record enters the queue, so it identifies records that could not be stored as well as records that were written.

## Behavior during pressure and failure

Venue feeds must not wait for storage. The capture client offers a record to a bounded in-memory queue and returns immediately. If that queue is full, it drops the record, increments a visible counter, and marks the missing sequence range. A disk error or writer failure marks the capture degraded and records the unverified tail when storage is available again.

The bot continues to observe markets, manage orders, process fills, and accept cancellation requests while capture is degraded. It will not present a capture with gaps as complete. A later replay or backtest must reject incomplete data unless an operator deliberately selects a recovery mode in a future specification.

The initial slice does not add QuestDB, Parquet, SQLite metadata, normalized event capture, or a dashboard. It must fit the current local file-based workflow and preserve existing capture readers until their migration is specified.

## What done looks like

An operator can start a capture, stop it cleanly, and run a validator on its directory. The validator reports the capture format version, record count, sequence range, missing ranges, writer status, and whether the capture is complete. It rejects malformed records, incomplete finalization, and a checksum mismatch.

The implementation includes a synthetic test feed. Tests prove that records retain their original input order, a full queue leaves an explicit gap, a writer failure does not block the feed, and the validator detects an incomplete capture. Existing smoke, replay, compilation, and test commands still pass.

## Decisions settled in the technical specification

- Raw capture writes only to a `raw/` folder in the session directory. No second top-level manifest is added.
- Text messages are stored as text. Only binary messages use base64.
- Gaps are written into the record file itself, with a reason: queue full, oversize, or writer error. The manifest is a summary that the validator checks against the file.
- One SHA-256 checksum covers the whole record file. Chunking or file rotation waits for real message-rate measurements.
- Raw capture is turned on and tuned in the bot config file, in its own optional `raw_capture` section. The queue is limited by both message count (default 10,000) and total size (default 16 MiB), and a burst test confirms the defaults.
- Because the config is saved inside each paper-trading capture, captures that use this section are marked schema version 5. Older versions still load, so existing tests and fixtures keep working. Replay ignores the section and never creates raw files.
- REST responses, such as settlement polling, are not recorded in this slice. Whether to add them is decided before the processed-capture work.
- After a hard crash, the capture is reported as unfinalized up to its last durable record. The number of messages lost in memory cannot be known.
