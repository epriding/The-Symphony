"""SQLite archive contracts from approved P03, independent of cache layout."""
from decimal import Decimal
import importlib
import sqlite3

import pytest


def store_type():
    return importlib.import_module("src.paper_trading.history").HistoryStore


def test_history_namespaces_duplicates_exact_decimals_and_bounded_pages(tmp_path):
    store = store_type()(tmp_path, page_size=3, max_cache_records=2)
    try:
        for index in range(19):
            assert store.put("PAPER:A:orders", str(index),
                             {"notional": Decimal("0.100000000000000001") * index})
        assert store.put("PAPER:B:orders", "0", {"notional": Decimal("9.99")})
        assert store.put("PAPER:A:orders", "0", {
            "notional": Decimal("0.100000000000000001") * 0}) is False
        with pytest.raises(ValueError):
            store.put("PAPER:A:orders", "0", {"notional": Decimal(1)})
        pages = store.iter_pages("PAPER:A:orders", page_size=2)
        keys = set()
        for page in pages:
            assert 0 < len(page) <= 2
            for key, value in page:
                assert key not in keys
                keys.add(key)
                assert value["notional"] == Decimal("0.100000000000000001") * int(key)
        assert keys == {str(index) for index in range(19)}
        assert store.get("PAPER:B:orders", "0")["notional"] == Decimal("9.99")
        assert not store.contains("PAPER:C:orders", "0")
        store.flush()
        # Verify committed database evidence, independently of a hot cache.
        with sqlite3.connect(tmp_path / "history.sqlite3") as connection:
            assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    finally:
        store.close()


@pytest.mark.parametrize("value", [object(), {"bad": float("nan")},
                                   {"large": "x" * 4096}])
def test_invalid_history_row_does_not_partially_commit_or_poison_store(tmp_path, value):
    store = store_type()(tmp_path, max_record_bytes=1024)
    try:
        with pytest.raises((ValueError, TypeError)):
            store.put("order", "bad", value)
        assert not store.contains("order", "bad")
        assert store.put("order", "good", {"quantity": Decimal("1.25")})
        assert store.get("order", "good") == {"quantity": Decimal("1.25")}
    finally:
        store.close()


class CommitFailureConnection(sqlite3.Connection):
    instances = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_next_commit = False
        self.__class__.instances.append(self)

    def commit(self):
        if self.fail_next_commit:
            self.fail_next_commit = False
            raise sqlite3.OperationalError("injected interruption at actual commit")
        return super().commit()


def test_failed_actual_commit_rolls_back_before_cache_or_membership_changes(tmp_path):
    store = store_type()(tmp_path, connection_factory=CommitFailureConnection)
    try:
        store.put("execution", "determined", {"cash": Decimal("1.01")})
        connection = CommitFailureConnection.instances[-1]
        connection.fail_next_commit = True
        with pytest.raises(sqlite3.OperationalError, match="actual commit"):
            store.put("execution", "uncommitted", {"cash": Decimal("2.02")})
        assert not store.contains("execution", "uncommitted")
        assert store.get("execution", "determined") == {"cash": Decimal("1.01")}
        assert store.put("execution", "uncommitted", {"cash": Decimal("2.02")})
    finally:
        store.close()


class TinyDiskConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.execute("PRAGMA max_page_count=64")


def test_real_sqlite_full_preserves_previously_committed_evidence(tmp_path):
    store = store_type()(tmp_path, max_record_bytes=16384,
                         connection_factory=TinyDiskConnection)
    written = []
    try:
        for index in range(256):
            key = str(index)
            try:
                store.put("fills", key, {"payload": "x" * 8192})
            except sqlite3.OperationalError as error:
                assert error.sqlite_errorcode == sqlite3.SQLITE_FULL
                assert not store.contains("fills", key)
                break
            written.append(key)
        else:
            pytest.fail("SQLite max_page_count did not constrain the actual archive")
        assert written, "The failure must follow an actual successful archive commit"
        assert all(store.contains("fills", key) for key in written)
    finally:
        store.close()


class InterruptConnection(sqlite3.Connection):
    instances = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interrupt_next_statement = False
        self.__class__.instances.append(self)

    def set_progress_handler(self, callback, instructions):
        # Compose with the archive's own deadline handler; installing that
        # legitimate handler must not accidentally disable this SQLite fault.
        return super().set_progress_handler(lambda: self._progress() or callback(), 1)

    def _progress(self):
        if self.interrupt_next_statement:
            self.interrupt_next_statement = False
            return 1
        return 0


def test_sqlite_interrupt_cannot_leave_a_half_written_membership_key(tmp_path):
    store = store_type()(tmp_path, connection_factory=InterruptConnection)
    try:
        store.put("intent", "old", {"quantity": 1})
        InterruptConnection.instances[-1].interrupt_next_statement = True
        with pytest.raises(sqlite3.OperationalError):
            store.put("intent", "interrupted", {"quantity": 2})
        assert not store.contains("intent", "interrupted")
        assert store.get("intent", "old") == {"quantity": 1}
    finally:
        store.close()


def test_history_stream_order_and_exclusive_destination(tmp_path):
    store = store_type()(tmp_path, page_size=2)
    try:
        sequence = [store.append("corrections", {"delta": Decimal(str(index))})
                    for index in range(9)]
        assert len(set(sequence)) == 9 and sequence == sorted(sequence)
        assert list(store.iter_records("corrections", after=sequence[3], page_size=2)) == [
            {"delta": Decimal(str(index))} for index in range(4, 9)]
        with pytest.raises((ValueError, FileExistsError)):
            store_type()(tmp_path)
    finally:
        store.close()
