"""Non-blocking raw-frame capture: a bounded queue drained by one writer thread.

`offer()` runs on the caller's thread (the asyncio loop). It stamps the frame,
assigns the next sequence number and enqueues without waiting. Only the
writer thread touches `records.jsonl` after start-up, and only the writer
thread writes the manifest once it is running. A frame the queue cannot take
is dropped; the writer sees the jump in sequence numbers and writes a gap
record in its place.

The queue is bounded twice: by item count (`queue_capacity`) and by the
payload bytes it holds (`queue_bytes`). A frame larger than
`max_payload_bytes` is never stored; a content-free marker takes its place so
the writer can record an `oversize` gap for that one sequence number.

The writer opens `records.jsonl` without Python's file buffer and batches
lines itself, so it always knows exactly which bytes it has handed to the
operating system. After each successful fsync it keeps a checkpoint: the
file size, the sequence covered, the digest and the counters at that point.

When a write, flush or fsync fails, nothing after the checkpoint can be
trusted (on Linux a failed fsync may already have dropped the data). The
writer rolls its state back to the checkpoint and becomes `degraded`. It
keeps draining the queue, discarding frames, so the tap never fills up
because of a broken disk. Every `retry_interval_seconds` it reopens the
file, truncates it to the checkpoint, writes one `writer_error` gap covering
everything from the checkpoint to the last frame it discarded, and fsyncs.
If that works it goes back to `writing`. It retries until shutdown, where it
makes a final attempt; if that fails too, the capture ends `failed`.
"""
from hashlib import sha256
from pathlib import Path
import json
import os
import queue
import threading
import time

from .records import FORMAT_VERSION, MAX_STREAM_CHARS, VENUES, RawGap, RawMessage
from .validator import FORMAT_NAME, max_line_bytes

# How long the writer waits for a frame before checking the fsync timer and the stop flag.
POLL_SECONDS = 0.05
# The writer hands its batch to the operating system once it reaches this size, or when idle.
BATCH_BYTES = 64 * 1024


class FileOps:
    """The file operations the writer uses. Tests replace them to inject disk errors."""

    def open(self, path, *, create):
        flags = os.O_WRONLY | os.O_APPEND | os.O_CLOEXEC
        if create:
            flags |= os.O_CREAT | os.O_EXCL
        return os.open(path, flags, 0o644)

    def write(self, fd, data):
        return os.write(fd, data)

    def fsync(self, fd):
        os.fsync(fd)

    def truncate(self, fd, size):
        os.ftruncate(fd, size)

    def size(self, fd):
        return os.fstat(fd).st_size

    def close(self, fd):
        os.close(fd)


def _positive_integer(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(f'{name} must be a positive integer')


class RawCapture:
    def __init__(self, directory, *, session_id, queue_capacity, queue_bytes, max_payload_bytes,
                 fsync_interval_seconds, retry_interval_seconds=1.0, code_revision='unknown',
                 on_status=None, file_ops=None, wall=time.time, monotonic_ns=time.monotonic_ns,
                 _gate=None):
        for name, value in (('queue_capacity', queue_capacity), ('queue_bytes', queue_bytes),
                            ('max_payload_bytes', max_payload_bytes)):
            _positive_integer(value, name)
        for name, value in (('fsync_interval_seconds', fsync_interval_seconds),
                            ('retry_interval_seconds', retry_interval_seconds)):
            if type(value) not in (int, float) or not 0 < value < float('inf'):
                raise ValueError(f'{name} must be a positive finite number')
        if queue_bytes < max_payload_bytes:
            raise ValueError('queue_bytes must be at least max_payload_bytes')
        # The validator requires both to be strings; check now rather than write an unreadable manifest.
        if type(session_id) is not str or not session_id:
            raise ValueError('session_id must be a non-empty string')
        if type(code_revision) is not str:
            raise ValueError('code_revision must be a string')
        self.directory = Path(directory) / 'raw'
        self.config = dict(queue_capacity=queue_capacity, queue_bytes=queue_bytes,
                           max_payload_bytes=max_payload_bytes,
                           fsync_interval_seconds=fsync_interval_seconds,
                           retry_interval_seconds=retry_interval_seconds)
        self._session_id = session_id
        self._code_revision = code_revision
        self._on_status = on_status
        self._files = file_ops or FileOps()
        self._wall = wall
        self._monotonic_ns = monotonic_ns
        self._gate = _gate

        # Shared with offer(), guarded by _lock.
        self._lock = threading.Lock()
        self._queue = queue.Queue(maxsize=queue_capacity)
        self._last_assigned = 0
        self._queued_bytes = 0
        self._closed = False
        self.dropped_queue_full = 0

        # Owned by the writer thread once it starts.
        self._expected = 1
        self._digest = sha256()
        self._bytes = 0                  # bytes appended, whether still batched or already written
        self._batch = bytearray()        # appended but not yet handed to the operating system
        self._message_count = 0
        self._gap_count = 0
        self._dropped = {'oversize': 0, 'queue_full': 0, 'writer_error': 0}
        self._status = 'writing'
        self._finalized = False
        self._started_wall = wall()
        self._ended_wall = None
        self._final_assigned = None
        self._last_fsync = time.monotonic()
        self._checkpoint = self._snapshot()
        self._lost_from = None           # while degraded: first and last seq to cover with a writer_error gap
        self._lost_to = None
        self._next_retry = 0.0
        self.writer_error = None

        self.directory.mkdir(parents=True, exist_ok=False)
        self._fd = None
        try:
            self._fd = self._files.open(self.directory / 'records.jsonl', create=True)
            self._write_manifest()
            self._stop = threading.Event()
            self._thread = threading.Thread(target=self._run, name='raw-capture-writer', daemon=True)
            self._thread.start()
        except BaseException:
            self._remove_partial_start()
            raise

    def _remove_partial_start(self):
        """Undo a failed start so the same output directory can be used again.

        Only removes the files this constructor creates, then the folder if it is empty.
        """
        if self._fd is not None:
            try:
                self._files.close(self._fd)
            except OSError:
                pass
        for name in ('records.jsonl', 'manifest.tmp', 'manifest.json'):
            try:
                (self.directory / name).unlink()
            except FileNotFoundError:
                pass
            except OSError:
                return
        try:
            self.directory.rmdir()
        except OSError:
            pass

    # ---- caller thread -------------------------------------------------

    def offer(self, venue, stream, connection_epoch, payload):
        """Record one inbound frame. Never blocks and never raises because of capture state."""
        if venue not in VENUES or type(stream) is not str or not 0 < len(stream) <= MAX_STREAM_CHARS:
            raise ValueError('offer needs a known venue and a 1-64 character stream name')
        if type(connection_epoch) is not int or connection_epoch < 0:
            raise ValueError('connection_epoch must be a non-negative integer')
        if type(payload) not in (str, bytes):
            raise ValueError('payload must be str or bytes')
        wall, monotonic_ns = self._wall(), self._monotonic_ns()
        size = len(payload) if type(payload) is bytes else len(payload.encode('utf-8'))
        if size > self.config['max_payload_bytes']:
            payload, size = None, 0  # oversize marker: the writer records a gap, not the content
        with self._lock:
            if self._closed:
                return
            self._last_assigned += 1
            if self._queued_bytes + size > self.config['queue_bytes']:
                self.dropped_queue_full += 1
                return
            item = (self._last_assigned, venue, stream, connection_epoch, wall, monotonic_ns, payload, size)
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                self.dropped_queue_full += 1
                return
            self._queued_bytes += size

    @property
    def last_assigned_seq(self):
        with self._lock:
            return self._last_assigned

    @property
    def status(self):
        return self._status

    def close(self, ended_wall=None, timeout=5.0):
        """Stop accepting frames, let the writer drain and finalize, and wait up to `timeout`.

        Returns True only if the capture was finalized in time.
        """
        with self._lock:
            if not self._closed:
                self._closed = True
                self._final_assigned = self._last_assigned
                self._ended_wall = self._wall() if ended_wall is None else ended_wall
        self._stop.set()
        self._thread.join(timeout)
        return not self._thread.is_alive() and self._finalized

    # ---- writer thread -------------------------------------------------

    def _run(self):
        try:
            interval = self.config['fsync_interval_seconds']
            while True:
                if self._gate is not None:
                    self._gate.wait()
                try:
                    item = self._queue.get(timeout=POLL_SECONDS)
                except queue.Empty:
                    item = None
                if self._status == 'degraded':
                    if item is not None:
                        self._discard(item)
                    if time.monotonic() >= self._next_retry:
                        self._recover()
                else:
                    try:
                        if item is not None:
                            self._write_item(item)
                        if item is None or len(self._batch) >= BATCH_BYTES:
                            self._flush()
                        if (self._bytes != self._checkpoint['bytes']
                                and time.monotonic() - self._last_fsync >= interval):
                            self._sync()
                    except OSError as error:
                        self._degrade(error)
                if item is None and self._stop.is_set() and self._queue.empty():
                    break
            self._finish()
        except BaseException as error:
            # Decision 1a: an unexpected error stops capture and is reported, never hidden as a gap.
            self._fail(error)

    def _fail(self, error):
        self.writer_error = error
        self._finalized = False
        if self._fd is not None:
            try:
                self._files.close(self._fd)
            except OSError:
                pass
            self._fd = None
        self._set_status('failed')

    def _write_item(self, item):
        seq, venue, stream, epoch, wall, monotonic_ns, payload, size = item
        with self._lock:
            self._queued_bytes -= size
        if seq > self._expected:
            self._write_gap(self._expected, seq - 1, 'queue_full')
        if payload is None:
            self._write_gap(seq, seq, 'oversize')
        else:
            self._append(RawMessage(seq=seq, venue=venue, stream=stream, connection_epoch=epoch,
                                    received_wall=wall, received_monotonic_ns=monotonic_ns,
                                    payload=payload).to_line())
            self._message_count += 1
        self._expected = seq + 1

    def _write_gap(self, first, last, reason):
        self._append(RawGap(first_seq=first, last_seq=last, reason=reason,
                            detected_wall=self._wall()).to_line())
        self._gap_count += 1
        self._dropped[reason] += last - first + 1

    def _append(self, line):
        self._batch += line
        self._digest.update(line)
        self._bytes += len(line)

    def _flush(self):
        """Hand the whole batch to the operating system, looping over short writes."""
        while self._batch:
            written = self._files.write(self._fd, self._batch)
            del self._batch[:written]

    def _sync(self):
        self._flush()
        self._files.fsync(self._fd)
        self._last_fsync = time.monotonic()
        self._checkpoint = self._snapshot()

    def _snapshot(self):
        return dict(bytes=self._bytes, expected=self._expected, digest=self._digest.copy(),
                    message_count=self._message_count, gap_count=self._gap_count,
                    dropped=dict(self._dropped))

    # ---- disk-error recovery (writer thread) ----------------------------

    def _discard(self, item):
        with self._lock:
            self._queued_bytes -= item[-1]
        self._lost_to = item[0]   # decision 4a: earlier drops of any kind fall inside the writer_error range

    def _degrade(self, error):
        """Roll back to the last checkpoint and start discarding until recovery works."""
        self.writer_error = error
        self._lost_from = self._checkpoint['expected']
        self._lost_to = self._expected - 1
        self._restore_checkpoint()
        self._next_retry = time.monotonic() + self.config['retry_interval_seconds']
        self._set_status('degraded')

    def _restore_checkpoint(self):
        checkpoint = self._checkpoint
        self._bytes = checkpoint['bytes']
        self._expected = checkpoint['expected']
        self._digest = checkpoint['digest'].copy()
        self._message_count = checkpoint['message_count']
        self._gap_count = checkpoint['gap_count']
        self._dropped = dict(checkpoint['dropped'])
        self._batch.clear()

    def _recover(self):
        """Reopen, truncate to the checkpoint, write the writer_error gap and fsync. True on success.

        A file shorter than its checkpoint means data already confirmed durable has
        gone; that is not a disk hiccup to retry, so it raises and fails the capture.
        """
        try:
            if self._fd is not None:
                try:
                    self._files.close(self._fd)
                except OSError:
                    pass
                self._fd = None
            self._fd = self._files.open(self.directory / 'records.jsonl', create=False)
            if self._files.size(self._fd) < self._checkpoint['bytes']:
                raise RuntimeError('records.jsonl is shorter than its last durable checkpoint')
            self._files.truncate(self._fd, self._checkpoint['bytes'])
            if self._lost_to >= self._lost_from:
                self._write_gap(self._lost_from, self._lost_to, 'writer_error')
                self._expected = self._lost_to + 1
            self._sync()
        except OSError as error:
            self.writer_error = error
            self._restore_checkpoint()
            self._next_retry = time.monotonic() + self.config['retry_interval_seconds']
            return False
        self._lost_from = self._lost_to = None
        self._set_status('writing')
        return True

    def _finish(self):
        """Drain is done: write the trailing gap, fsync and finalize.

        Each disk error here gets one immediate recovery attempt, at most two in
        total, so shutdown stays bounded. If they fail, the capture ends failed.
        """
        for _ in range(2):
            if self._status == 'degraded' and not self._recover():
                break
            try:
                if self._final_assigned >= self._expected:
                    self._write_gap(self._expected, self._final_assigned, 'queue_full')
                    self._expected = self._final_assigned + 1
                self._sync()
            except OSError as error:
                self._degrade(error)
                continue
            break
        if self._status == 'degraded':
            self._fail(self.writer_error)
            return
        try:
            self._files.close(self._fd)
        except OSError:
            pass   # the fsync above already confirmed the data is on disk
        self._fd = None
        # Publish "closed" in memory only after the manifest saying so is on disk.
        try:
            self._write_manifest(status='closed', finalized=True)
        except OSError as error:
            self._fail(error)
            return
        self._finalized = True
        self._status = 'closed'
        self._notify('closed')

    def _set_status(self, status):
        """Record a status change in the manifest and tell the owner. Never raises."""
        if status == self._status:
            return
        try:
            self._write_manifest(status=status)
        except OSError:
            pass   # the status still changes in memory; the next manifest write will carry it
        self._status = status
        self._notify(status)

    def _notify(self, status):
        if self._on_status is not None:
            try:
                self._on_status(status)
            except Exception:
                pass

    def _write_manifest(self, *, status=None, finalized=None):
        """Atomically replace the manifest; `status` and `finalized` override the current values."""
        status = self._status if status is None else status
        finalized = self._finalized if finalized is None else finalized
        missing = sum(self._dropped.values())
        manifest = dict(
            format=FORMAT_NAME, format_version=FORMAT_VERSION, session_id=self._session_id,
            code_revision=self._code_revision, started_wall=self._started_wall,
            ended_wall=self._ended_wall if finalized else None, status=status,
            finalized=finalized, complete=finalized and missing == 0,
            config=self.config, max_line_bytes=max_line_bytes(self.config['max_payload_bytes']),
            message_count=self._message_count, gap_count=self._gap_count, missing_count=missing,
            dropped=dict(self._dropped), last_assigned_seq=self._expected - 1,
            records_sha256=self._digest.hexdigest() if finalized else None,
            records_bytes=self._bytes if finalized else None)
        path = self.directory / 'manifest.json'
        temporary = path.with_suffix('.tmp')
        with open(temporary, 'w', encoding='utf-8') as output:
            json.dump(manifest, output, indent=2, allow_nan=False)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
