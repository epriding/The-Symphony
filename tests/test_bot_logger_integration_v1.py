"""Useful formatted errors and handler ownership, without runtime secrets."""
import logging
from pathlib import Path
import threading
import time

import pytest

from src.utilities.logger import setup_logger


class Observer(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []
        self.was_closed = False

    def emit(self, record):
        self.messages.append(record.getMessage())

    def close(self):
        self.was_closed = True
        super().close()


def clear_logger(logger):
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


def test_file_setup_installs_one_owned_handler_when_unrelated_handler_exists(tmp_path):
    name = "bot-v1-owner-" + tmp_path.name
    logger = logging.getLogger(name)
    observer = Observer()
    logger.addHandler(observer)
    path = tmp_path / "bot.log"
    try:
        assert setup_logger(name, log_file=str(path), console=False) is logger
        assert setup_logger(name, log_file=str(path), console=False) is logger
        assert observer in logger.handlers and not observer.was_closed
        logger.warning("one durable line")
        for handler in logger.handlers:
            handler.flush()
        assert path.read_text().count("one durable line") == 1
        assert observer.messages == ["one durable line"]
    finally:
        clear_logger(logger)


def test_rendered_exception_chain_args_and_headers_redact_environment_secret(tmp_path, monkeypatch):
    secret = "DUMMY_SDK_CREDENTIAL_593210_PRIVATE"
    monkeypatch.setenv("POLYMARKET_US_SECRET_KEY", secret)
    logger = logging.getLogger("bot-v1-redact-" + tmp_path.name)
    path = tmp_path / "errors.log"
    try:
        setup_logger(logger.name, log_file=str(path), console=False)
        try:
            try:
                raise ValueError("SDK Authorization: Bearer " + secret)
            except ValueError as cause:
                raise RuntimeError("request headers={'api_key': '" + secret + "'}") from cause
        except RuntimeError:
            logger.exception("operation %s failed for credential %s", "public-read", secret)
        for handler in logger.handlers:
            handler.flush()
        rendered = path.read_text()
        assert secret not in rendered
        assert "public-read" in rendered
        assert "ValueError" in rendered and "RuntimeError" in rendered
        assert Path(__file__).name in rendered
        assert "direct cause" in rendered
    finally:
        clear_logger(logger)


def record(message):
    return logging.LogRecord("runtime-fixture", logging.ERROR, __file__, 1,
                             message, (), None)


def test_log_inflight_record_counts_toward_capacity_and_cleanup_has_finite_deadline(tmp_path):
    from src.utilities.logger import BoundedLogHandler
    handler = BoundedLogHandler(tmp_path / "queue.log", max_records=1,
        max_bytes=4096, max_record_bytes=2048, timeout=.03)
    entered, release = threading.Event(), threading.Event()
    original_file = handler._file

    class PausedFile:
        def write(self, text):
            entered.set()
            assert release.wait(2), "Test failed to release its actual file-write barrier"
            return original_file.write(text)

        def flush(self):
            return original_file.flush()

        def close(self):
            return original_file.close()

    handler._file = PausedFile()
    try:
        handler.emit(record("retained-first-record"))
        assert entered.wait(1), "Log worker did not reach the real write boundary"
        with pytest.raises(RuntimeError, match="capacity"):
            handler.emit(record("must-not-be-enqueued"))
        with pytest.raises(RuntimeError, match="deadline"):
            handler.flush()
        before = time.monotonic()
        with pytest.raises(RuntimeError, match="deadline"):
            handler.close(timeout=.03)
        assert time.monotonic() - before < .5
    finally:
        release.set()
        handler.close(timeout=1)
        # A previous close deadline may leave a cooperative writer completing
        # its one in-flight write. The fixture must not leak that writer into
        # another test, even if the handler treats a second close as idempotent.
        handler._worker.join(timeout=1)
        assert not handler._worker.is_alive()
    text = (tmp_path / "queue.log").read_text()
    assert text.count("retained-first-record") == 1
    assert "must-not-be-enqueued" not in text


def test_actual_log_flush_failure_is_visible_to_flush_admission_and_close(tmp_path):
    from src.utilities.logger import BoundedLogHandler
    handler = BoundedLogHandler(tmp_path / "failed.log", max_records=2,
        max_bytes=4096, max_record_bytes=2048, timeout=.1)
    original_file = handler._file

    class FailedFlush:
        def write(self, text):
            return original_file.write(text)

        def flush(self):
            raise OSError("dummy-full-log-disk")

        def close(self):
            return original_file.close()

    handler._file = FailedFlush()
    try:
        handler.emit(record("write-before-failed-flush"))
        handler._worker.join(timeout=1)
        assert not handler._worker.is_alive()
        for operation in (handler.flush, lambda: handler.emit(record("later")), handler.close):
            with pytest.raises(RuntimeError) as failure:
                operation()
            assert isinstance(failure.value.__cause__, OSError)
            assert "dummy-full-log-disk" in str(failure.value.__cause__)
    finally:
        # close correctly reports the same I/O failure on repeated ownership
        # cleanup; suppress only that already asserted failure in the fixture.
        try:
            handler.close()
        except RuntimeError:
            pass


def test_failed_log_destination_construction_does_not_start_worker_or_overwrite(tmp_path):
    from src.utilities.logger import BoundedLogHandler
    path = tmp_path / "existing.log"
    path.write_text("preserve")
    before = {thread.ident for thread in threading.enumerate() if thread.name == "bot-log-writer"}
    with pytest.raises(FileExistsError):
        BoundedLogHandler(path, max_records=1, max_bytes=1024, max_record_bytes=512)
    assert path.read_text() == "preserve"
    after = {thread.ident for thread in threading.enumerate() if thread.name == "bot-log-writer"}
    assert after == before


def test_log_byte_capacity_counts_utf8_inflight_bytes_before_next_admission(tmp_path):
    from src.utilities.logger import BoundedLogHandler
    handler = BoundedLogHandler(tmp_path / "bytes.log", max_records=3,
        max_bytes=24, max_record_bytes=20, timeout=.1)
    handler.setFormatter(logging.Formatter("%(message)s"))
    entered, release = threading.Event(), threading.Event()
    original_file = handler._file

    class PausedFile:
        def write(self, text):
            entered.set()
            assert release.wait(2), "Test failed to release its real write barrier"
            return original_file.write(text)

        def flush(self):
            return original_file.flush()

        def close(self):
            return original_file.close()

    handler._file = PausedFile()
    try:
        # Sixteen encoded bytes fit individually; two must exceed the byte
        # budget even though record-count capacity still has two spare slots.
        handler.emit(record("é" * 8))
        assert entered.wait(1)
        with pytest.raises(RuntimeError, match="capacity"):
            handler.emit(record("ø" * 8))
        with pytest.raises(RuntimeError, match="capacity"):
            handler.emit(record("ß" * 11))
    finally:
        release.set()
        handler.close(timeout=1)
    assert (tmp_path / "bytes.log").read_text().splitlines() == ["é" * 8]
    assert not handler._worker.is_alive() and original_file.closed


def test_successful_log_close_drains_accepted_rows_and_rejects_late_admission(tmp_path):
    from src.utilities.logger import BoundedLogHandler
    handler = BoundedLogHandler(tmp_path / "closed.log", max_records=8,
        max_bytes=4096, max_record_bytes=512, timeout=1)
    handler.setFormatter(logging.Formatter("%(message)s"))
    try:
        for number in range(6):
            handler.emit(record(f"accepted-{number}"))
        handler.close()
        assert not handler._worker.is_alive() and handler._file.closed
        assert (tmp_path / "closed.log").read_text().splitlines() == [
            f"accepted-{number}" for number in range(6)]
        with pytest.raises(RuntimeError, match="closed"):
            handler.emit(record("late-unowned-record"))
        handler.close()
    finally:
        handler.close()


def test_log_worker_launch_failure_closes_already_open_destination(tmp_path, monkeypatch):
    from src.utilities.logger import BoundedLogHandler
    destination = tmp_path / "worker-did-not-start.log"
    opened = []
    original_open = Path.open

    def observe_open(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path == destination:
            opened.append(stream)
        return stream

    def fail_launch(worker):
        raise RuntimeError("dummy-log-thread-launch-failure")

    monkeypatch.setattr(Path, "open", observe_open)
    monkeypatch.setattr(threading.Thread, "start", fail_launch)
    try:
        with pytest.raises(RuntimeError, match="dummy-log-thread-launch-failure"):
            BoundedLogHandler(destination, max_records=1, max_bytes=1024,
                              max_record_bytes=512)
        assert len(opened) == 1, "Failure must follow opening the actual destination"
        assert opened[0].closed, "Partial log construction retained an owned file handle"
    finally:
        for stream in opened:
            stream.close()


@pytest.mark.parametrize('raw,expected', [
    ('ordinary=keep API_KEY="dummy-value"', 'ordinary=keep API_KEY="[REDACTED]"'),
    ('outer=inner=token=dummy&safe=keep', 'outer=inner=token=[REDACTED]&safe=keep'),
    ('auth.password = dummy; plain:value', 'auth.password = [REDACTED]; plain:value'),
    ('my-secret-field=one inner.api_key:two', 'my-secret-field=[REDACTED] inner.api_key:[REDACTED]'),
])
def test_secret_assignments_preserve_nested_and_prefixed_masking(raw,expected):
    from src.utilities.logger import redact
    assert redact(raw)==expected


def test_long_plain_record_does_not_backtrack_beyond_service_deadline():
    import subprocess
    import sys
    # A bounded log record must not stall parsing for longer than an RPC deadline.
    result=subprocess.run([sys.executable,'-c',
        "from src.utilities.logger import redact; s='x'*16000; assert redact(s)==s"],
        capture_output=True,timeout=5)
    assert result.returncode==0,result.stderr
