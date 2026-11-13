"""Owned logging handlers with credential-safe rendering and bounded runtime I/O."""
import copy
import logging
import os
from pathlib import Path
import queue
import re
import sys
import threading
import time
from typing import Optional

TRADE_LOG_FILE: Optional[str] = None
ERROR_LOG_FILE: Optional[str] = None
MAIN_LOG_FILE: Optional[str] = None
_SECRET_NAME = re.compile(r"token|secret|password|private.?key|access.?key|api.?key|authorization|chat.?id|key.?id", re.I)
_PATTERNS = (
    (re.compile(r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----", re.S), "[REDACTED PRIVATE KEY]"),
    (re.compile(r"\b\d{7,}:[A-Za-z0-9_-]{20,}"), "[REDACTED]"),
    (re.compile(r"(?i)(\bbearer\s+)[A-Za-z0-9._~+/=-]+"), r"\g<1>[REDACTED]"),
)
_FIELD_PREFIX = re.compile(r'''(?<![\w.-])([\w.-]+)["']?\s*[:=]\s*["']?''')
_FIELD_SECRET = re.compile(r'secret|token|password|api[_-]?key|access[_-]?key|authorization|signature|private[_-]?key', re.I)
_FIELD_VALUE = re.compile(r'''[^\s,;}'"&]+''')


def _redact_assignments(text):
    # Scan each whole identifier once. The old unanchored greedy prefix tried
    # every suffix of a long word, even when it contained no credential name.
    parts = []
    consumed = 0
    for match in _FIELD_PREFIX.finditer(text):
        if match.start() < consumed or not _FIELD_SECRET.search(match[1]):
            continue
        value = _FIELD_VALUE.match(text, match.end())
        if value is None:
            continue
        parts.extend((text[consumed:match.end()], '[REDACTED]'))
        consumed = value.end()
    parts.append(text[consumed:])
    return ''.join(parts)


def redact(value):
    """Redact after interpolation and traceback rendering, including nested causes."""
    text = str(value)
    # Environment values are read in memory only; no environment dump is written.
    secrets = {v for k, v in os.environ.items() if _SECRET_NAME.search(k) and len(v) >= 4}
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, "[REDACTED]")
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return _redact_assignments(text)


class SafeFormatter(logging.Formatter):
    def format(self, record):
        local = copy.copy(record)
        local.exc_text = None
        return redact(super().format(local))


def _formatter(simple=False):
    return SafeFormatter('%(message)s' if simple else
                         '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                         datefmt='%Y-%m-%d %H:%M:%S')


def close_logger(logger):
    """Close only handlers installed here; preserve callers' attached handlers."""
    errors = []
    for handler in list(logger.handlers):
        if getattr(handler, '_bot_owned', False):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception as error:
                errors.append(error)
    if errors:
        raise RuntimeError('owned log handler cleanup failed') from errors[0]


def setup_logger(name='Nelliel', log_level='INFO', log_file=None, console=True):
    logger = logging.getLogger(name)
    level = getattr(logging, log_level.upper())
    destination = str(Path(log_file).resolve()) if log_file else None
    signature = (destination, bool(console))
    current = [h for h in logger.handlers if getattr(h, '_bot_owned', False)]
    if current and all(getattr(h, '_bot_signature', None) == signature for h in current):
        logger.setLevel(level)
        return logger
    handlers = []
    try:
        if console:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(level)
            handlers.append(handler)
        if destination:
            Path(destination).parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(destination))
        for handler in handlers:
            handler.setFormatter(_formatter())
            handler._bot_owned = True
            handler._bot_signature = signature
    except BaseException:
        for handler in handlers:
            handler.close()
        raise
    try:
        close_logger(logger)
    except BaseException:
        for handler in handlers:
            handler.close()
        raise
    logger.setLevel(level)
    logger.propagate = False
    for handler in handlers:
        logger.addHandler(handler)
    return logger


class BoundedLogHandler(logging.Handler):
    """One finite I/O worker; admission or I/O failure is visible to the owner.

    Capacity includes an in-flight record. Runtime callers must also bound their
    input objects; this handler does not sandbox arbitrary __str__ methods.
    """
    def __init__(self, path, *, max_records=64, max_bytes=262144,
                 max_record_bytes=16384, timeout=2.0):
        super().__init__()
        for value in (max_records, max_bytes, max_record_bytes):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError('Positive finite logging capacities required')
        if timeout <= 0 or not __import__('math').isfinite(timeout):
            raise ValueError('Positive finite log timeout required')
        self.capacity, self.byte_limit = max_records, max_bytes
        self.record_limit, self.timeout = min(max_record_bytes, max_bytes), timeout
        self._queue = queue.Queue(maxsize=max_records)
        self._pending = self._bytes = 0
        self._guard = threading.Condition()
        self._failure = None
        self._closing = False
        self._worker = None
        self._bot_owned = True
        self.setFormatter(_formatter())
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open('x', encoding='utf-8')
        try:
            worker = threading.Thread(target=self._work, name='bot-log-writer', daemon=True)
            worker.start()
        except BaseException as error:
            # A partially constructed handler is still known to logging's
            # shutdown registry. Leave it closed with no unstarted thread to
            # join, and release the destination before preserving the failure.
            self._closing = True
            try:
                self._file.close()
            except BaseException as cleanup_error:
                error.add_note('partial log close: ' + redact(cleanup_error))
            finally:
                super().close()
            raise
        self._worker = worker

    def emit(self, record):
        text = self.format(record)
        encoded = text.encode('utf-8')
        if len(encoded) > self.record_limit:
            raise RuntimeError('runtime log record capacity exceeded')
        with self._guard:
            if self._failure is not None:
                raise RuntimeError('runtime log write failed') from self._failure
            if self._closing:
                raise RuntimeError('runtime log is closed')
            if self._pending >= self.capacity or self._bytes + len(encoded) > self.byte_limit:
                raise RuntimeError('runtime log queue capacity exceeded')
            self._pending += 1
            self._bytes += len(encoded)
            self._queue.put_nowait((text, len(encoded)))

    def _work(self):
        try:
            while True:
                try:
                    item = self._queue.get(timeout=.05)
                except queue.Empty:
                    with self._guard:
                        if self._closing and not self._pending:
                            break
                    continue
                text, size = item
                try:
                    self._file.write(text + '\n')
                    self._file.flush()
                finally:
                    with self._guard:
                        self._pending -= 1
                        self._bytes -= size
                        self._guard.notify_all()
                    self._queue.task_done()
        except BaseException as error:
            with self._guard:
                self._failure = error
                self._guard.notify_all()
        finally:
            try:
                self._file.close()
            except BaseException as error:
                # Worker exceptions must reach the runtime through its bounded
                # owner, never through an unredacted thread traceback.
                with self._guard:
                    if self._failure is None:
                        self._failure = error
                    self._guard.notify_all()

    def flush(self):
        if not hasattr(self, '_guard') or getattr(self, '_closed', False):
            return
        deadline = time.monotonic() + self.timeout
        with self._guard:
            while self._pending and self._failure is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError('runtime log flush deadline exceeded')
                self._guard.wait(remaining)
            if self._failure is not None:
                raise RuntimeError('runtime log write failed') from self._failure

    def close(self, timeout=None):
        if getattr(self, '_closed', False):
            return
        try:
            if hasattr(self, '_guard'):
                with self._guard:
                    self._closing = True
                    self._guard.notify_all()
                if self._worker is not None:
                    self._worker.join(self.timeout if timeout is None else timeout)
                if self._worker is not None and self._worker.is_alive():
                    raise RuntimeError('runtime log worker cleanup deadline exceeded')
                if self._failure is not None:
                    raise RuntimeError('runtime log write failed') from self._failure
        finally:
            # Report a failure to the owner once. logging.shutdown must not later
            # emit an unformatted replay of a credential-bearing error chain.
            super().close()


def setup_multi_logger(main_log_file='logs/bot.log', trade_log_file='logs/trades.log',
                       error_log_file='logs/errors.log', log_level='INFO'):
    global MAIN_LOG_FILE, TRADE_LOG_FILE, ERROR_LOG_FILE
    MAIN_LOG_FILE, TRADE_LOG_FILE, ERROR_LOG_FILE = main_log_file, trade_log_file, error_log_file
    main = setup_logger('Nelliel', log_level, main_log_file, console=False)
    trades = setup_logger('Nelliel.Trades', 'INFO', trade_log_file, console=True)
    errors = setup_logger('Nelliel.Errors', 'WARNING', error_log_file, console=False)
    for handler in trades.handlers:
        if getattr(handler, '_bot_owned', False) and isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            handler.setFormatter(_formatter(simple=True))
    # Dedicated warnings/errors also reach the main logger, without handler leaks.
    target = str(Path(error_log_file).resolve())
    if not any(getattr(h, '_bot_error_target', None) == target for h in main.handlers):
        handler = logging.FileHandler(target)
        handler.setLevel(logging.WARNING)
        handler.setFormatter(_formatter())
        handler._bot_owned = True
        handler._bot_signature = (str(Path(main_log_file).resolve()), False)
        handler._bot_error_target = target
        main.addHandler(handler)


def get_trade_logger():
    logger = logging.getLogger('Nelliel.Trades')
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def get_error_logger():
    logger = logging.getLogger('Nelliel.Errors')
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger
