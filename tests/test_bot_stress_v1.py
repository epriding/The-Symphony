"""Finite independent checks of the mixed diagnostic workload and capture seam."""
import asyncio
from collections import Counter
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import importlib
import json

import pytest

from src.bot import replay_bot
from src.paper_trading.clock import VirtualClock
from src.paper_trading.models import PaperExecutionConfig
from src.paper_trading.recording import iter_records, read_capture
from src.paper_trading.session import PaperSession
from src.paper_trading.venue_rules import fixture_venue
from src.utilities.classes import OrderBookSnapshotEvent, OrderRecord, OrderStatus, Side, TradingConfig

from bot_v1_support import START


def read_rows(directory, name):
    return [json.loads(line) for line in (directory / name).read_text().splitlines()]


@pytest.mark.asyncio
@pytest.mark.parametrize("bid_quantity", [100, 4], ids=["original-depth", "small-replenished-bid"])
async def test_mixed_two_venue_workload_has_real_partial_cancel_correction_and_replay_evidence(
        tmp_path, monkeypatch, bid_quantity):
    benchmark = importlib.import_module("examples.bot_trading_benchmark")
    if bid_quantity == 4:
        class SmallBidFeed(benchmark._Feed):
            async def stream(self):
                async for envelope in super().stream():
                    event = envelope.event
                    if isinstance(event, OrderBookSnapshotEvent) and event.bids:
                        envelope = replace(envelope, event=replace(event,
                            bids={price: bid_quantity for price in event.bids}))
                    yield envelope

        # Four contracts suffice for either cycle's reduction, provided actual
        # withdrawal/replenishment restores consumed liquidity between cycles.
        # Empty bid books pass through so the genuine conservative engine sees
        # the generator's withdrawals. Every other execution path stays real.
        monkeypatch.setattr(benchmark, "_Feed", SmallBidFeed)
    cycles = 2
    result = await asyncio.wait_for(benchmark.stress_benchmark(tmp_path / "mixed", cycles=cycles), 40)
    capture, replay = tmp_path / "mixed/capture", tmp_path / "mixed/replay"
    manifest, inputs = read_capture(capture)
    records = list(inputs)
    if bid_quantity == 4:
        books = [record["payload"].event for record in records
            if record["kind"] == "public" and isinstance(record["payload"].event, OrderBookSnapshotEvent)]
        assert books and all(not book.bids or set(book.bids.values()) == {4} for book in books)
        assert any(not book.bids and not book.asks for book in books)
    limits = manifest["runtime_config"]["runtime"]["limits"]
    summary, replay_summary = result["summary"], result["replay_summary"]
    assert result["cycles"] == cycles
    assert summary["submitted"] == 8 * cycles
    assert summary["full_fills"] == 4 * cycles
    assert summary["partial_fills"] == summary["zero_fills"] == 2 * cycles
    for key in ("submitted", "full_fills", "partial_fills", "zero_fills", "accounts", "history"):
        assert summary[key] == replay_summary[key]
    rows = read_rows(capture, "executions.jsonl")
    executions = [row for row in rows if row["kind"] == "execution"]
    corrections = [row for row in rows if row["kind"] == "correction"]
    assert len(executions) == 6 * cycles and len(corrections) == 2 * cycles
    assert len({(row["exchange"], row["target_order_id"]) for row in corrections}) == 2 * cycles
    assert all(Decimal(row["notional_delta"]) == Decimal("-.04") and
        Decimal(row["fee_delta"]) == 0 for row in corrections)
    # The cumulative input is recorded twice, while its accounting correction
    # row and the economic effect occur once. Replayed details prove that the
    # private report input was reapplied rather than only copied into a total.
    updates = [record["payload"] for record in records if record["kind"] == "order_update"]
    assert len(updates) == 4 * cycles
    assert set(Counter((row.exchange, row.order_id, row.revision) for row in updates).values()) == {2}
    assert read_rows(replay, "executions.jsonl") == rows
    timings = read_rows(capture, "order_timings.jsonl")
    assert len(timings) == 8 * cycles
    assert len({(row["exchange"], row["order_id"]) for row in timings}) == len(timings)
    partial = [row for row in timings if 0 < Decimal(row["filled_quantity"]) < Decimal(row["size"])]
    canceled = [row for row in timings if row["cancel_requested_at"] is not None]
    assert len(partial) == len(canceled) == 2 * cycles
    assert all(row["time_in_force"] == "IOC" and row["status"] == "CANCELED" for row in partial)
    assert all(row["time_in_force"] == "GTC" and row["status"] == "CANCELED" and
        Decimal(row["filled_quantity"]) == 0 for row in canceled)
    assert all(row["last_report_at"] - row["cancel_requested_at"] >= .14 for row in canceled)
    filled = [row for row in timings if Decimal(row["filled_quantity"]) > 0]
    assert all(row["last_report_at"] - row["last_fill_at"] >= .09 for row in filled)
    groups = read_rows(capture, "groups.jsonl")
    assert len(groups) == cycles and all(not row["failed"] for row in groups)
    assert all(len(row["legs"]) == 2 and {leg["exchange"] for leg in row["legs"]} == set(benchmark.VENUES)
        and all(Decimal(leg["filled_quantity"]) == 2 for leg in row["legs"]) for row in groups)
    assert result["queries"]["executions"] == len(executions)
    assert result["queries"]["corrections"] == len(corrections)
    assert result["queries"]["groups"] == len(groups)
    assert 0 < result["queries"]["max_page_records"] <= limits["max_archive_cache_records"]
    account = summary["accounts"]["PAPER"]
    assert account["open_positions"] == 2 and account["open_orders"] == account["pending_reservations"] == 0
    assert account["realized_pnl"] == pytest.approx(0, abs=1e-9)
    assert result["retained_inventory"] == result["replay_inventory"]
    assert len(result["retained_inventory"]) == 2
    for position in result["retained_inventory"].values():
        assert position["size"] == 1 and position["entry_price"] == pytest.approx(.49)
    # Derive each cash balance from actual fills and once-only correction rows,
    # independently of benchmark counters or the replay's returned summary.
    for venue in benchmark.VENUES:
        expected = Decimal(1000)
        for row in executions:
            if row["exchange"] == venue:
                expected += (1 if row["side"] == "SELL" else -1) * Decimal(row["notional"])
                expected -= Decimal(row["fee"])
                expected += Decimal(row["credit"])
        for row in corrections:
            if row["exchange"] == venue:
                expected += (1 if row["side"] == "SELL" else -1) * Decimal(row["notional_delta"])
                expected -= Decimal(row["fee_delta"])
        assert expected == Decimal("999.51")
        assert account["cash"][venue] == pytest.approx(float(expected))
    batches = [record["payload"] for record in records if record["kind"] == "batch"]
    assert len(batches) == 2 * cycles and sum(batch.batch_id == batches[0].batch_id for batch in batches) == cycles + 1
    attempted = [record["payload"].intent_id for record in records if record["kind"] == "intent"]
    assert attempted.count(batches[0].legs[0].intent_id) == cycles
    assert result["workload_counts"]["duplicate_corrections"] == 2 * cycles
    assert result["workload_counts"]["duplicate_intents"] == result["workload_counts"]["duplicate_batches"] == cycles
    references = {item["kind"]: item for item in summary["history"]["details"]}
    for kind, expected_count in (("executions", 8 * cycles), ("markouts", 12 * cycles),
            ("order_timings", 8 * cycles), ("groups", cycles)):
        assert references[kind]["records"] == len(read_rows(capture, references[kind]["path"])) == expected_count
    high = result["high_water"]
    assert high["active_orders"] <= limits["max_active_orders"]
    assert high["positions"] <= limits["max_positions"]
    assert high["hot_history_records"] <= limits["max_hot_history_records"] + limits["max_active_orders"]
    assert high["archive_cache_records"] <= limits["max_archive_cache_records"]
    assert high["completed_markout_cache_records"] <= limits["max_archive_cache_records"]
    assert high["archive_queued_requests"] <= 8
    execution_limits = manifest["runtime_config"]["execution"]
    assert high["pending_events"] <= execution_limits["max_pending_events"]
    assert high["outbox"] <= execution_limits["max_outbox"]
    assert high["engine_orders"] <= limits["max_active_orders"] + limits["max_hot_history_records"] + execution_limits["max_outbox"]
    assert high["engine_groups"] <= limits["max_active_orders"]
    assert high["book_levels"] <= 4 and high["summary_bytes"] < 1024 * 1024
    assert high["pending_reports"] > 0, "The workload never retained its delayed report dependency"
    memory = result["memory"]
    assert {"startup", "warmup", "trading", "correction", "query", "report", "replay"} <= set(memory["samples"])
    resident_observations = [memory["rss_baseline_bytes"], memory["rss_final_bytes"], memory["kernel_peak_rss_bytes"]]
    for sample in memory["samples"].values():
        assert sample["rss_bytes"] > 0 and sample["python_current_bytes"] > 0
        resident_observations.extend((sample["rss_bytes"], sample["rss_peak_bytes"]))
    assert memory["rss_peak_bytes"] == max(resident_observations)
    assert 0 <= memory["python_peak_delta_bytes"] <= limits["memory_budget_bytes"]
    assert memory["rss_peak_bytes"] - memory["rss_baseline_bytes"] <= limits["memory_budget_bytes"]


def cumulative_session(directory):
    config = TradingConfig()
    config.strategy_portfolios = {"one": "PAPER"}
    config.paper_starting_balances = {"PAPER": {"A": 1000}}
    config.paper_execution = PaperExecutionConfig({"A": fixture_venue("A")})
    return PaperSession(config, VirtualClock(START), capture_directory=directory)


def cumulative_report():
    return OrderRecord("target", "m", "A", "YES", Side.BUY, 2, .5,
        OrderStatus.FILLED, strategy="one", portfolio_id="PAPER", filled_quantity=2,
        average_fill_price=.5, fees_paid=0, cumulative_notional="1.0", intent_id="target-intent")


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_type", ["mapping", "subclass"])
async def test_cumulative_input_whitelist_rejects_before_capture_or_coordinator(tmp_path, monkeypatch, invalid_type):
    session = cumulative_session(tmp_path)
    record = cumulative_report()
    if invalid_type == "mapping":
        invalid = vars(record)
    else:
        class UnregisteredReport(OrderRecord):
            pass
        invalid = UnregisteredReport(**vars(record))
    calls = []

    async def must_not_apply(value):
        calls.append(value)
        pytest.fail("Unwhitelisted input reached the accounting owner")

    monkeypatch.setattr(session.coordinator, "apply_order_update", must_not_apply)
    try:
        before = session.recorder.count
        with pytest.raises(ValueError, match="whitelisted"):
            await session._apply_cumulative_update(invalid)
        assert session.recorder.count == before and not calls
        assert not session.state.portfolios["PAPER"].positions
    finally:
        await session.finish(finalize_capture=False)


@pytest.mark.asyncio
async def test_cumulative_capture_write_failure_prevents_accounting_and_stays_incomplete(tmp_path, monkeypatch):
    session = cumulative_session(tmp_path / "capture")
    actual_write = session.recorder.handle.write

    def unavailable(line):
        raise OSError("injected actual capture handle write failure")

    try:
        monkeypatch.setattr(session.recorder.handle, "write", unavailable)
        with pytest.raises(OSError, match="actual capture handle write"):
            await session._apply_cumulative_update(cumulative_report())
        assert session.recorder.count == 0
        account = session.state.portfolios["PAPER"]
        assert account.balances["A"]["USD"].cash == 1000 and not account.positions
        assert not account.order_history
    finally:
        monkeypatch.setattr(session.recorder.handle, "write", actual_write)
        await session.finish(finalize_capture=False)
    assert json.loads((tmp_path / "capture/manifest.json").read_text())["finalized"] is False
    with pytest.raises(ValueError, match="incomplete"):
        await replay_bot(tmp_path / "capture", tmp_path / "replay")


@pytest.mark.asyncio
async def test_rejected_cumulative_input_keeps_attempted_capture_before_account_error(tmp_path):
    session = cumulative_session(tmp_path / "capture")
    invalid = replace(cumulative_report(), filled_quantity=3)
    try:
        with pytest.raises(ValueError, match="quantities"):
            await session._apply_cumulative_update(invalid)
        session.recorder.handle.flush()
        rows = list(iter_records(tmp_path / "capture/inputs.jsonl"))
        assert len(rows) == 1 and rows[0]["kind"] == "order_update"
        assert rows[0]["payload"] == invalid
        account = session.state.portfolios["PAPER"]
        assert account.balances["A"]["USD"].cash == 1000 and not account.positions
        assert not account.order_history
    finally:
        await session.finish(finalize_capture=False)
    assert json.loads((tmp_path / "capture/manifest.json").read_text())["finalized"] is False


@pytest.mark.asyncio
async def test_diagnostic_cumulative_input_cannot_mutate_live_portfolio_or_enter_capture(tmp_path):
    session = cumulative_session(tmp_path)
    before = deepcopy(session.state.portfolios["LIVE"])
    try:
        with pytest.raises(ValueError, match="paper"):
            await session._apply_cumulative_update(replace(cumulative_report(), portfolio_id="LIVE"))
        assert session.recorder.count == 0
        assert session.state.portfolios["LIVE"] == before
        assert session.state.portfolios["PAPER"].balances["A"]["USD"].cash == 1000
    finally:
        await session.finish(finalize_capture=False)
