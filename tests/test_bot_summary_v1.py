"""Q-BOT-005/A01 acceptance: bounded aggregates and complete streamed details.

The existing independent retention workload supplies real runner trading inputs;
it is reused here without replacing matching, accounting or archival behavior.
Missing public APIs fail at execution, so this module remains collectable first.
"""
import asyncio
from collections import Counter
from collections.abc import Mapping, Sequence, Set
from decimal import Decimal
from pathlib import Path
import sys

import pytest

import src.bot as bot
from bot_v1_support import VENUES
from test_bot_retention_v1 import execution_totals, run_workload


async def bounded_call(coroutine, *, timeout):
    """Observe completion/cancellation without an unbounded cancellation await."""
    task = asyncio.create_task(coroutine)
    try:
        done, pending = await asyncio.wait({task}, timeout=timeout)
        assert not pending, "summary acceptance operation exceeded its finite fixture deadline"
        return task.result()
    finally:
        if not task.done():
            task.cancel()
            done, pending = await asyncio.wait({task}, timeout=5)
            if pending:
                message = "summary acceptance operation exceeded its cooperative cleanup deadline"
                original_error = sys.exception()
                if original_error is not None:
                    original_error.add_note(message)
                else:
                    raise AssertionError(message)
            else:
                await asyncio.gather(*done, return_exceptions=True)


def return_footprint(summary):
    """Count returned Python container slots/bytes, independently of total RSS."""
    pending, seen, slots, size = [summary], set(), 0, 0
    while pending:
        value = pending.pop()
        slots += 1
        assert slots <= 8192, "summary returned historical collections instead of bounded aggregates"
        identity = id(value)
        if identity in seen:
            continue
        seen.add(identity)
        size += sys.getsizeof(value)
        if isinstance(value, Mapping):
            for key, item in value.items():
                pending.extend((key, item))
        elif isinstance(value, (Sequence, Set)) and not isinstance(value, (str, bytes, bytearray)):
            pending.extend(value)
    assert size <= 1024 * 1024, "returned summary exceeds the explicit finite synthetic size guard"
    return slots, size


def history_references(summary, output_directory, expected_executions):
    history = summary["history"]
    assert isinstance(history, Mapping) and history["scope"] == "full-session"
    details = history["details"]
    assert isinstance(details, list) and 4 <= len(details) <= 64
    counts, execution_references = Counter(), 0
    output = output_directory.resolve()
    for reference in details:
        assert isinstance(reference, Mapping)
        assert {"kind", "path", "records"} <= set(reference)
        kind, relative, records = reference["kind"], reference["path"], reference["records"]
        assert isinstance(kind, str) and kind.strip()
        assert isinstance(relative, str) and relative
        assert not Path(relative).is_absolute()
        artifact = (output / relative).resolve()
        assert artifact.is_relative_to(output) and artifact.is_file()
        assert type(records) is int and records >= 0
        counts[kind] += records
        # This artifact already belongs to the existing execution/reporting
        # contract; other detail filenames, schemas and kind spellings are free.
        if artifact == output / "executions.jsonl":
            execution_references += 1
            assert records == expected_executions
    assert len(counts) >= 4, "detail references must distinguish the four agreed history categories"
    assert execution_references >= 1
    return dict(counts)


def exact_totals(summary, directory, scenario, cycles):
    expected_orders = 2 * len(VENUES) * cycles
    assert summary["submitted"] == summary["full_fills"] == expected_orders
    assert summary["partial_fills"] == summary["zero_fills"] == 0
    assert Decimal(str(summary["fees"])) == 0
    account = summary["accounts"]["PAPER"]
    assert account["open_positions"] == account["open_orders"] == account["pending_reservations"] == 0
    count, signed_notional = execution_totals(directory)
    assert count == expected_orders
    for venue in VENUES:
        expected_cash = Decimal(1000) - Decimal(".02") * scenario.quantity_bought[venue]
        assert Decimal(str(account["cash"][venue])) == expected_cash
        assert Decimal(str(account["cash"][venue])) == Decimal(1000) + signed_notional[venue]
        assert summary["by_venue"][venue]["orders"] == 2 * cycles
    assert summary["by_strategy"]["retention"]["orders"] == expected_orders
    return history_references(summary, directory, count)


@pytest.mark.asyncio
async def test_summary_stays_aggregate_as_archived_trading_grows_and_replay_keeps_complete_details(tmp_path):
    measurements = []
    for cycles in (8, 48):
        directory = tmp_path / f"trading-{cycles}"
        config, scenario, summary = await bounded_call(run_workload(directory, cycles=cycles), timeout=30)
        assert 2 * len(VENUES) * cycles > 4 * config["runtime"]["limits"]["max_hot_history_records"]
        detail_counts = exact_totals(summary, directory, scenario, cycles)
        measurements.append(return_footprint(summary))

    small_slots, small_bytes = measurements[0]
    large_slots, large_bytes = measurements[1]
    # Both workloads exceed the same four-record hot window. Increasing from
    # 32 to 192 completed orders may change scalar values, not append raw
    # per-order/fill/markout/group arrays or ID-indexed historical mappings.
    assert large_slots <= small_slots + 32
    assert large_bytes <= small_bytes + 4096

    replay_directory = tmp_path / "replayed"
    replayed = await bounded_call(bot.replay_bot(directory, replay_directory), timeout=5)
    replay_counts = exact_totals(replayed, replay_directory, scenario, cycles)
    assert replayed["accounts"] == summary["accounts"]
    assert replay_counts == detail_counts
    replay_slots, replay_bytes = return_footprint(replayed)
    assert replay_slots <= large_slots + 32
    assert replay_bytes <= large_bytes + 4096
