"""Session-local, bounded SQLite history. This is an archive, never an account.

A single owned thread creates and uses the connection. Requests and result pages
are finite; failed transactions roll back before their exception reaches callers.
Callers may discard authoritative hot evidence only after an awaited commit.
"""
import asyncio
import hashlib
from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import fields, is_dataclass
from decimal import Decimal
from enum import Enum
from functools import lru_cache
import json
import os
from pathlib import Path
import queue
import sqlite3
import threading
import time


class HistoryStore:
    schema_version = 2
    detail_streams = ('executions', 'markouts', 'order_timings', 'groups')

    def __init__(self, directory, *, limits=None, max_cache_records=8, page_size=64,
                 max_record_bytes=262144, request_capacity=64, timeout=5.0,
                 connection_factory=sqlite3.Connection):
        limits = limits or {}
        self.max_cache_records = limits.get('max_archive_cache_records', max_cache_records)
        self.page_size = min(page_size, self.max_cache_records)
        self.max_record_bytes = limits.get('max_event_bytes', max_record_bytes)
        for value in (self.max_cache_records, self.page_size, self.max_record_bytes, request_capacity):
            if type(value) is not int or value < 1:
                raise ValueError('History capacities must be positive integers')
        if isinstance(timeout, bool) or not 0 < timeout < float('inf'):
            raise ValueError('History timeout must be finite and positive')
        self.timeout = timeout
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        self.path = path / 'history.sqlite3'
        # Exclusive claim; even an empty previous database is evidence.
        with self.path.open('xb'):
            pass
        self._factory = connection_factory
        self._requests = queue.Queue(maxsize=request_capacity)
        self._closed = False
        self._failed = None
        self._connection = None
        self._change_count = 0
        self._stop = threading.Event()
        self._cache = OrderedDict()
        self._detail_sequence = 0
        self._detail_outputs = {}
        self._detail_hashes = {kind: hashlib.sha256() for kind in self.detail_streams}
        self._detail_counts = dict.fromkeys(self.detail_streams, 0)
        self._notification_pipe = None
        self._notification_loop = None
        self._thread = threading.Thread(target=self._work, name='paper-history', daemon=True)
        self._thread.start()
        try:
            self._call(self._initialize)
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup_error:
                error.add_note(f'History startup cleanup failed: {type(cleanup_error).__name__}: {cleanup_error}')
            raise

    def _work(self):
        while True:
            request = self._requests.get()
            if request is None:
                break
            future, operation = request
            if not future.set_running_or_notify_cancel():
                continue
            try:
                self._deadline = time.monotonic() + self.timeout
                result = operation(self._connection)
                if self._connection is not None:
                    # Publish the worker-owned SQLite counter before completing
                    # the request, including real commits preceding export failure.
                    self._change_count = self._connection.total_changes
                    if not self._connection.in_transaction:
                        self._sync_details(self._connection)
                future.set_result(result)
            except BaseException as error:
                if self._connection is not None:
                    try:
                        self._connection.rollback()
                        # SQLite includes rolled-back writes in total_changes.
                        # Those cause conservative extra reads, never a missed one.
                        self._change_count = self._connection.total_changes
                    except Exception as rollback_error:
                        error.add_note(f'History rollback failed: {type(rollback_error).__name__}')
                future.set_exception(error)
            finally:
                notification_pipe = self._notification_pipe
                if notification_pipe is not None:
                    try:
                        os.write(notification_pipe[1], b'\0')
                    except (BlockingIOError, OSError):
                        pass  # An unread notification already wakes the owner.
        close_errors = []
        for output in self._detail_outputs.values():
            try:
                output.close()
            except BaseException as error:
                close_errors.append(error)
        if self._connection is not None:
            try:
                self._connection.close()
            except BaseException as error:
                close_errors.append(error)
            self._connection = None
        if close_errors:
            self._failed = close_errors[0]
            for error in close_errors[1:]:
                self._failed.add_note(f'History cleanup also failed: {type(error).__name__}: {error}')

    def _initialize(self, _):
        conn = sqlite3.connect(self.path, timeout=self.timeout, factory=self._factory)
        self._connection = conn
        # FULL WAL commits sync the append-only journal once, avoiding repeated
        # delete-journal directory syncs on every small accounting transition.
        # Frequent finite checkpoints bound the native WAL index and dirty work.
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=FULL')
        conn.execute('PRAGMA wal_autocheckpoint=64')
        conn.execute('PRAGMA mmap_size=0')
        conn.execute('PRAGMA cache_size=-256')
        conn.execute('PRAGMA temp_store=FILE')
        conn.execute(f'PRAGMA busy_timeout={max(1, int(self.timeout * 1000))}')
        conn.set_progress_handler(lambda: int(self._stop.is_set() or time.monotonic() > self._deadline), 1000)
        conn.execute('CREATE TABLE metadata (version INTEGER NOT NULL)')
        conn.execute('INSERT INTO metadata VALUES (?)', (self.schema_version,))
        conn.execute('CREATE TABLE entries (namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(namespace,key)) WITHOUT ROWID')
        conn.execute('CREATE TABLE records (sequence INTEGER PRIMARY KEY AUTOINCREMENT, stream TEXT NOT NULL, value TEXT NOT NULL)')
        conn.execute('CREATE INDEX records_stream_sequence ON records(stream,sequence)')
        conn.execute('CREATE TABLE stream_counts (stream TEXT PRIMARY KEY, records INTEGER NOT NULL) WITHOUT ROWID')
        conn.execute('CREATE TABLE pending_marks (identity TEXT PRIMARY KEY, due REAL NOT NULL, value TEXT NOT NULL, exchange TEXT NOT NULL, market TEXT NOT NULL) WITHOUT ROWID')
        conn.execute('CREATE INDEX pending_due ON pending_marks(exchange,market,due,identity)')
        for kind in self.detail_streams:
            self._detail_outputs[kind] = (self.path.parent / f'{kind}.jsonl').open('xb')
        conn.commit()

    def _check_deadline(self):
        if self._stop.is_set() or time.monotonic() >= self._deadline:
            raise TimeoutError('History operation exceeded its deadline')

    def _sync_details(self, conn):
        """Export only newly committed rows, on the owned I/O worker.

        SQLite remains authoritative if exporting fails. The checkpoint and
        hashes advance only after a complete flushed row, so a retry does not
        duplicate committed rows or retire evidence ahead of its detail stream.
        """
        while True:
            rows = conn.execute('SELECT sequence,stream,value FROM records WHERE sequence>? ORDER BY sequence LIMIT ?',
                                (self._detail_sequence, self.page_size)).fetchmany(self.page_size)
            if not rows:
                return
            self._check_deadline()
            for sequence, kind, raw in rows:
                if kind in self._detail_outputs:
                    encoded = (json.dumps(self._decode(raw), sort_keys=True,
                        allow_nan=False, separators=(',', ':')) + '\n').encode('utf-8')
                    output = self._detail_outputs[kind]
                    offset = output.tell()
                    try:
                        if output.write(encoded) != len(encoded):
                            raise OSError('Incomplete history detail write')
                        output.flush()
                    except BaseException as error:
                        try:
                            output.seek(offset)
                            output.truncate()
                        except BaseException as cleanup_error:
                            error.add_note(f'History detail rollback failed: {type(cleanup_error).__name__}: {cleanup_error}')
                        raise
                    self._detail_hashes[kind].update(encoded)
                    self._detail_counts[kind] += 1
                self._detail_sequence = sequence

    def _detail_references(self, conn):
        self._sync_details(conn)
        return [dict(kind=kind, path=f'{kind}.jsonl', records=self._detail_counts[kind],
                     sha256=self._detail_hashes[kind].hexdigest()) for kind in self.detail_streams]

    def detail_references(self):
        return self._call(self._detail_references)

    async def adetail_references(self):
        return await self.run(self._detail_references)

    def _submit(self, operation):
        if self._closed:
            raise RuntimeError('History store is closed')
        future = Future()
        try:
            self._requests.put_nowait((future, operation))
        except queue.Full:
            raise BufferError('history_request_capacity_exceeded') from None
        return future

    def _call(self, operation):
        future = self._submit(operation)
        try:
            return future.result(timeout=self.timeout + .1)
        except TimeoutError:
            self.interrupt()
            raise TimeoutError('History operation exceeded its deadline') from None

    async def run(self, operation):
        """Execute a bounded operation on the owned connection, off the loop."""
        if self._closed:
            raise RuntimeError('History store is closed')
        self._attach_notifications()
        future = self._submit(operation)
        wrapped = asyncio.wrap_future(future)
        try:
            return await asyncio.wait_for(asyncio.shield(wrapped), self.timeout + .1)
        except (TimeoutError, asyncio.CancelledError):
            self.interrupt()
            wrapped.add_done_callback(lambda completed: completed.exception() if not completed.cancelled() else None)
            # The operation is still responsible for rollback; no hot eviction
            # may follow this exception.
            raise

    def _attach_notifications(self):
        loop = asyncio.get_running_loop()
        if self._notification_loop is loop:
            return
        if self._notification_loop is not None and not self._notification_loop.is_closed():
            raise RuntimeError('History store already belongs to another event loop')
        if self._notification_pipe is None:
            self._notification_pipe = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
        self._notification_loop = loop
        # Use a local pipe rather than a socket. This also works on restricted
        # hosts that disallow asyncio's socket-based cross-thread wakeup. One
        # pipe/reader per store keeps both descriptors and wake buffers bounded.
        loop.add_reader(self._notification_pipe[0], self._drain_notifications)

    def _drain_notifications(self):
        try:
            while os.read(self._notification_pipe[0], 4096):
                pass
        except BlockingIOError:
            pass

    def _close_notifications(self):
        if self._notification_pipe is None:
            return
        if self._notification_loop is not None and not self._notification_loop.is_closed():
            self._notification_loop.remove_reader(self._notification_pipe[0])
        for descriptor in self._notification_pipe:
            os.close(descriptor)
        self._notification_pipe = None

    def interrupt(self):
        if self._connection is not None:
            self._connection.interrupt()

    @staticmethod
    @lru_cache(maxsize=1)
    def _types():
        from ..utilities import classes
        from . import models
        return {name: value for module in (classes, models) for name, value in vars(module).items()
                if isinstance(value, type) and (is_dataclass(value) or issubclass(value, Enum))}

    def _encode(self, value):
        remaining = self.max_record_bytes
        def consume(amount):
            nonlocal remaining
            remaining -= amount
            if remaining < 0:
                raise ValueError('History record exceeds max_record_bytes')

        def text_size(value):
            # Reject large strings before making their UTF-8/JSON copies.
            if len(value) > remaining:
                raise ValueError('History record exceeds max_record_bytes')
            consume(len(value.encode('utf-8')))

        def convert(item, depth=0):
            if depth > 40:
                raise ValueError('History record nesting exceeds limit')
            consume(4)
            if isinstance(item, Enum):
                if type(item).__name__ not in self._types():
                    raise ValueError('Unsupported history enum type')
                text_size(type(item).__name__)
                return {'__enum__': type(item).__name__, 'value': item.value}
            if isinstance(item, Decimal):
                if not item.is_finite():
                    raise ValueError('Nonfinite history amount')
                text_size(str(item))
                return {'__decimal__': str(item)}
            if is_dataclass(item) and not isinstance(item, type):
                if type(item).__name__ not in self._types():
                    raise ValueError('Unsupported history record type')
                return {'__type__': type(item).__name__, 'fields': {
                    f.name: convert(getattr(item, f.name), depth + 1) for f in fields(item)}}
            if isinstance(item, tuple):
                return {'__tuple__': [convert(x, depth + 1) for x in item]}
            if isinstance(item, (list, dict)) and len(item) > self.max_record_bytes:
                raise ValueError('History record exceeds capacity')
            if isinstance(item, list):
                return [convert(x, depth + 1) for x in item]
            if isinstance(item, dict):
                if any(not isinstance(k, str) for k in item):
                    raise ValueError('History object keys must be strings')
                if any(k.startswith('__') for k in item):
                    raise ValueError('Reserved history object key')
                result = {}
                for key, item_value in item.items():
                    text_size(key)
                    result[key] = convert(item_value, depth + 1)
                return result
            if isinstance(item, str):
                text_size(item)
                return item
            if item is None or isinstance(item, (str, int, float, bool)):
                return item
            raise ValueError('Unsupported history value')
        try:
            result = json.dumps(convert(value), separators=(',', ':'), sort_keys=True, allow_nan=False)
        except (TypeError, OverflowError, RecursionError) as error:
            raise ValueError('Invalid history record') from error
        if len(result.encode('utf-8')) > self.max_record_bytes:
            raise ValueError('History record exceeds max_record_bytes')
        return result

    def _decode(self, raw):
        if len(raw) > self.max_record_bytes or len(raw.encode('utf-8')) > self.max_record_bytes:
            raise ValueError('Oversized archived record')
        types = self._types()
        def restore(item, depth=0):
            if depth > 40:
                raise ValueError('History record nesting exceeds limit')
            if isinstance(item, list):
                return [restore(x, depth + 1) for x in item]
            if not isinstance(item, dict):
                return item
            if '__decimal__' in item:
                return Decimal(item['__decimal__'])
            if '__tuple__' in item:
                return tuple(restore(x, depth + 1) for x in item['__tuple__'])
            if '__enum__' in item:
                return types[item['__enum__']](item['value'])
            if '__type__' in item:
                return types[item['__type__']](**{k: restore(v, depth + 1) for k, v in item['fields'].items()})
            return {k: restore(v, depth + 1) for k, v in item.items()}
        return restore(json.loads(raw))

    def _identity(self, namespace, key):
        if not isinstance(namespace, str) or not namespace or len(namespace) > 256:
            raise ValueError('Invalid history namespace')
        return namespace, self._encode(key)

    def _get(self, conn, namespace, key, default=None):
        row = conn.execute('SELECT value FROM entries WHERE namespace=? AND key=?',
                           self._identity(namespace, key)).fetchone()
        return default if row is None else self._decode(row[0])

    def get(self, namespace, key, default=None):
        return self._call(lambda conn: self._get(conn, namespace, key, default))

    async def aget(self, namespace, key, default=None):
        return await self.run(lambda conn: self._get(conn, namespace, key, default))

    def contains(self, namespace, key):
        identity = self._identity(namespace, key)
        return self._call(lambda conn: conn.execute('SELECT 1 FROM entries WHERE namespace=? AND key=?', identity).fetchone() is not None)

    async def acontains(self, namespace, key):
        identity = self._identity(namespace, key)
        return await self.run(lambda conn: conn.execute('SELECT 1 FROM entries WHERE namespace=? AND key=?', identity).fetchone() is not None)

    def _put(self, conn, namespace, key, value, replace=False):
        identity = self._identity(namespace, key)
        raw = self._encode(value)
        old = conn.execute('SELECT value FROM entries WHERE namespace=? AND key=?', identity).fetchone()
        if old is not None and not replace:
            if self._decode(old[0]) != value:
                raise ValueError('History identity reused with different content')
            return False
        if old is not None and old[0] == raw:
            return False
        if namespace == 'engine_orders':
            self._update_order_totals(conn, None if old is None else self._decode(old[0]), value)
        elif namespace.startswith('cash_credits:') and old is None:
            pid = namespace.split(':', 1)[1]
            total = self._get(conn, '_summary', ('credits', pid), Decimal(0))
            self._put(conn, '_summary', ('credits', pid), total + Decimal(str(value)), True)
        conn.execute('INSERT INTO entries VALUES (?,?,?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value', (*identity, raw))
        return old is None or old[0] != raw

    def put(self, namespace, key, value):
        def operation(conn):
            result = self._put(conn, namespace, key, value)
            conn.commit()
            return result
        return self._call(operation)

    def upsert(self, namespace, key, value):
        def operation(conn):
            result = self._put(conn, namespace, key, value, True)
            conn.commit()
            return result
        return self._call(operation)

    def delete(self, namespace, key):
        identity = self._identity(namespace, key)
        def operation(conn):
            count = conn.execute('DELETE FROM entries WHERE namespace=? AND key=?', identity).rowcount
            conn.commit()
            return bool(count)
        return self._call(operation)

    def _append(self, conn, stream, record):
        self._identity(stream, '')
        sequence = conn.execute('INSERT INTO records(stream,value) VALUES (?,?)', (stream, self._encode(record))).lastrowid
        if stream in {'executions', 'markouts', 'order_timings', 'groups'} and isinstance(record, dict):
            # The global SQLite sequence also counts unrelated streams whose
            # flush cadence can differ during replay. Export only this stream's
            # transactional ordinal; keep the global key for internal paging.
            current = conn.execute('SELECT records FROM stream_counts WHERE stream=?', (stream,)).fetchone()
            record = dict(record, accounting_order=(current[0] if current else 0) + 1)
            conn.execute('UPDATE records SET value=? WHERE sequence=?', (self._encode(record), sequence))
        conn.execute('INSERT INTO stream_counts VALUES (?,1) ON CONFLICT(stream) DO UPDATE SET records=records+1', (stream,))
        if stream == 'executions':
            self._update_execution_totals(conn, record)
        return sequence

    def _update_order_totals(self, conn, old, row):
        totals = self._get(conn, '_summary', 'orders', dict(
            counts={}, tif={}, venues={}, strategies={}, submitted=0, full=0, partial=0, zero=0))
        for item, direction in ((old, -1), (row, 1)):
            if item is None:
                continue
            quantity, size = Decimal(item['filled_quantity']), Decimal(item['size'])
            totals['submitted'] += direction
            for name, selected in (('full', quantity == size), ('partial', 0 < quantity < size), ('zero', quantity == 0)):
                totals[name] += direction * selected
            for name, key in (('counts', item['status']), ('tif', item['time_in_force']), ('venues', item['exchange'])):
                totals[name][key] = totals[name].get(key, 0) + direction
            strategy = totals['strategies'].setdefault(item['strategy'], dict(orders=0, statuses={}))
            strategy['orders'] += direction
            strategy['statuses'][item['status']] = strategy['statuses'].get(item['status'], 0) + direction
        self._put(conn, '_summary', 'orders', totals, True)

    def _update_execution_totals(self, conn, row):
        names = ('fees', 'credits', 'turnover', 'execution_cost_vs_mid',
                 'slippage_vs_decision_quote', 'spread', 'delay', 'depth_walking', 'missing_benchmark_quantity')
        aggregate = self._get(conn, '_summary', 'executions', dict(
            totals=dict.fromkeys(names, Decimal(0)), accounts={}, roles={}, executions=0))
        totals = aggregate['totals']
        pid = row.get('portfolio_id', 'PAPER')
        amounts = aggregate['accounts'].setdefault(pid, dict(fees=Decimal(0), credits=Decimal(0)))
        direction = 1 if row.get('side', 'BUY') == 'BUY' else -1
        if row.get('kind') == 'correction':
            fee, delta = Decimal(row['fee_delta']), Decimal(row['notional_delta'])
            totals['fees'] += fee
            amounts['fees'] += fee
            totals['turnover'] += delta
            for name in ('execution_cost_vs_mid', 'slippage_vs_decision_quote', 'depth_walking'):
                totals[name] += direction * delta
        else:
            aggregate['executions'] += 1
            fee, credit = Decimal(row['fee']), Decimal(row['credit'])
            totals['fees'] += fee
            totals['credits'] += credit
            amounts['fees'] += fee
            amounts['credits'] += credit
            totals['turnover'] += Decimal(row['notional'])
            quantity, price = Decimal(row['quantity']), Decimal(row['price'])
            roles = aggregate['roles'].setdefault(row['exchange'], {})
            roles[row['role']] = roles.get(row['role'], 0) + 1
            for field, name in (('decision_mid', 'execution_cost_vs_mid'), ('decision_quote', 'slippage_vs_decision_quote')):
                if row.get(field) is not None:
                    totals[name] += direction * (price - Decimal(str(row[field]))) * quantity
            if all(row.get(key) is not None for key in ('decision_mid', 'decision_quote', 'arrival_quote')):
                totals['spread'] += direction * (Decimal(str(row['decision_quote'])) - Decimal(str(row['decision_mid']))) * quantity
                totals['delay'] += direction * (Decimal(str(row['arrival_quote'])) - Decimal(str(row['decision_quote']))) * quantity
                totals['depth_walking'] += direction * (price - Decimal(str(row['arrival_quote']))) * quantity
            else:
                totals['missing_benchmark_quantity'] += quantity
        self._put(conn, '_summary', 'executions', aggregate, True)

    def append(self, stream, record):
        def operation(conn):
            sequence = self._append(conn, stream, record)
            conn.commit()
            return sequence
        return self._call(operation)

    def _page_limit(self, page_size):
        limit = self.page_size if page_size is None else page_size
        if type(limit) is not int or not 0 < limit <= self.page_size:
            raise ValueError('History page exceeds configured page size')
        return limit

    def iter_pages(self, namespace, *, page_size=None):
        limit = self._page_limit(page_size)
        after = ''
        while True:
            def operation(conn):
                cursor = conn.execute('SELECT key,value FROM entries WHERE namespace=? AND key>? ORDER BY key LIMIT ?', (namespace, after, limit))
                return cursor.fetchmany(limit)
            rows = self._call(operation)
            if not rows:
                return
            after = rows[-1][0]
            yield [(self._decode(key), self._decode(value)) for key, value in rows]

    def iter_records(self, stream, *, after=0, page_size=None):
        limit = self._page_limit(page_size)
        while True:
            rows = self._call(lambda conn: conn.execute('SELECT sequence,value FROM records WHERE stream=? AND sequence>? ORDER BY sequence LIMIT ?', (stream, after, limit)).fetchmany(limit))
            if not rows:
                return
            after = rows[-1][0]
            for _, raw in rows:
                yield self._decode(raw)

    def count(self, stream):
        return self._call(lambda conn: self._count(conn, stream))

    @staticmethod
    def _count(conn, stream):
        row = conn.execute('SELECT records FROM stream_counts WHERE stream=?', (stream,)).fetchone()
        return row[0] if row else 0

    def flush(self):
        return self._call(lambda conn: conn.commit())

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        self.interrupt()
        try:
            self._requests.put_nowait(None)
        except queue.Full:
            # Outstanding finite requests drain before close; never an unbounded
            # join even when a third-party connection ignores interrupt.
            self._requests.put(None, timeout=self.timeout)
        self._thread.join(timeout=self.timeout)
        if self._thread.is_alive():
            self._close_notifications()
            raise TimeoutError('History worker did not close before deadline')
        self._close_notifications()
        if self._failed is not None:
            raise self._failed

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        self.interrupt()
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self._requests.put_nowait(None)
                break
            except queue.Full:
                if time.monotonic() >= deadline:
                    raise TimeoutError("History close queue did not drain")
                await asyncio.sleep(.001)
        while self._thread.is_alive():
            if time.monotonic() >= deadline:
                self._close_notifications()
                raise TimeoutError("History worker did not close before deadline")
            await asyncio.sleep(.001)
        self._close_notifications()
        if self._failed is not None:
            raise self._failed

    async def aiter_pages(self, namespace, *, page_size=None):
        limit = self._page_limit(page_size)
        after = ''
        while True:
            rows = await self.run(lambda conn: conn.execute(
                'SELECT key,value FROM entries WHERE namespace=? AND key>? ORDER BY key LIMIT ?',
                (namespace, after, limit)).fetchmany(limit))
            if not rows:
                return
            after = rows[-1][0]
            yield [(self._decode(key), self._decode(value)) for key, value in rows]

    async def aiter_records(self, stream, *, after=0, page_size=None):
        limit = self._page_limit(page_size)
        while True:
            rows = await self.run(lambda conn: conn.execute(
                'SELECT sequence,value FROM records WHERE stream=? AND sequence>? ORDER BY sequence LIMIT ?',
                (stream, after, limit)).fetchmany(limit))
            if not rows:
                return
            after = rows[-1][0]
            for _, raw in rows:
                yield self._decode(raw)

    async def acount(self, stream):
        return await self.run(lambda conn: self._count(conn, stream))
