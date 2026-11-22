"""Non-blocking raw-frame capture: a bounded queue drained by one writer thread.

`offer()` runs on the caller's thread (the asyncio loop). It stamps the frame,
assigns the next sequence number and enqueues without waiting. Only the
writer thread touches `records.jsonl` after start-up, and only the writer
thread writes the manifest once it is running. A frame the queue cannot take
is dropped; the writer sees the jump in sequence numbers and writes a gap
record in its place.
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


def _positive_integer(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(f'{name} must be a positive integer')


class RawCapture:
    def __init__(self, directory, *, session_id, queue_capacity, queue_bytes, max_payload_bytes,
                 fsync_interval_seconds, retry_interval_seconds=1.0, code_revision='unknown',
                 opener=open, wall=time.time, monotonic_ns=time.monotonic_ns, _gate=None):
        for name, value in (('queue_capacity', queue_capacity), ('queue_bytes', queue_bytes),
                            ('max_payload_bytes', max_payload_bytes)):
            _positive_integer(value, name)
        for name, value in (('fsync_interval_seconds', fsync_interval_seconds),
                            ('retry_interval_seconds', retry_interval_seconds)):
            if type(value) not in (int, float) or not 0 < value < float('inf'):
                raise ValueError(f'{name} must be a positive finite number')
        self.directory = Path(directory) / 'raw'
        self.config = dict(queue_capacity=queue_capacity, queue_bytes=queue_bytes,
                           max_payload_bytes=max_payload_bytes,
                           fsync_interval_seconds=fsync_interval_seconds,
                           retry_interval_seconds=retry_interval_seconds)
        self._session_id = session_id
        self._code_revision = code_revision
        self._wall = wall
        self._monotonic_ns = monotonic_ns
        self._gate = _gate

        # Shared with offer(), guarded by _lock.
        self._lock = threading.Lock()
        self._queue = queue.Queue(maxsize=queue_capacity)
        self._last_assigned = 0
        self._closed = False
        self.dropped_queue_full = 0

        # Owned by the writer thread once it starts.
        self._expected = 1
        self._digest = sha256()
        self._bytes = 0
        self._message_count = 0
        self._gap_count = 0
        self._dropped = {'oversize': 0, 'queue_full': 0, 'writer_error': 0}
        self._status = 'writing'
        self._finalized = False
        self._started_wall = wall()
        self._ended_wall = None
        self._final_assigned = None
        self._last_fsync = time.monotonic()
        self.writer_error = None

        self.directory.mkdir(parents=True, exist_ok=False)
        self._records = opener(self.directory / 'records.jsonl', 'xb')
        try:
            self._write_manifest()
        except BaseException:
            self._records.close()
            raise
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name='raw-capture-writer', daemon=True)
        self._thread.start()

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
        with self._lock:
            if self._closed:
                return
            self._last_assigned += 1
            item = (self._last_assigned, venue, stream, connection_epoch, wall, monotonic_ns, payload)
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                self.dropped_queue_full += 1

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
                if item is not None:
                    self._write_item(item)
                if time.monotonic() - self._last_fsync >= interval:
                    self._fsync()
                if item is None and self._stop.is_set() and self._queue.empty():
                    break
            self._finish()
        except BaseException as error:  # Recorded for the owner; task-level recovery handles OSError.
            self.writer_error = error

    def _write_item(self, item):
        seq, venue, stream, epoch, wall, monotonic_ns, payload = item
        if seq > self._expected:
            self._write_gap(self._expected, seq - 1, 'queue_full')
        self._write_line(RawMessage(seq=seq, venue=venue, stream=stream, connection_epoch=epoch,
                                    received_wall=wall, received_monotonic_ns=monotonic_ns,
                                    payload=payload).to_line())
        self._message_count += 1
        self._expected = seq + 1

    def _write_gap(self, first, last, reason):
        self._write_line(RawGap(first_seq=first, last_seq=last, reason=reason,
                                detected_wall=self._wall()).to_line())
        self._gap_count += 1
        self._dropped[reason] += last - first + 1

    def _write_line(self, line):
        self._records.write(line)
        self._digest.update(line)
        self._bytes += len(line)

    def _fsync(self):
        self._records.flush()
        os.fsync(self._records.fileno())
        self._last_fsync = time.monotonic()

    def _finish(self):
        if self._final_assigned >= self._expected:
            self._write_gap(self._expected, self._final_assigned, 'queue_full')
            self._expected = self._final_assigned + 1
        self._fsync()
        self._records.close()
        self._status = 'closed'
        self._finalized = True
        self._write_manifest()

    def _write_manifest(self):
        missing = sum(self._dropped.values())
        manifest = dict(
            format=FORMAT_NAME, format_version=FORMAT_VERSION, session_id=self._session_id,
            code_revision=self._code_revision, started_wall=self._started_wall,
            ended_wall=self._ended_wall if self._finalized else None, status=self._status,
            finalized=self._finalized, complete=self._finalized and missing == 0,
            config=self.config, max_line_bytes=max_line_bytes(self.config['max_payload_bytes']),
            message_count=self._message_count, gap_count=self._gap_count, missing_count=missing,
            dropped=dict(self._dropped), last_assigned_seq=self._expected - 1,
            records_sha256=self._digest.hexdigest() if self._finalized else None,
            records_bytes=self._bytes if self._finalized else None)
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
