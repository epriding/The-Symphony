"""The bounded replay facade enforces its finite legacy compatibility profile."""
from dataclasses import replace
from hashlib import sha256
import json

import pytest

from src.bot import replay_bot
from src.paper_trading.clock import VirtualClock
from src.paper_trading.history import HistoryStore
from src.paper_trading.models import PaperExecutionConfig
from src.paper_trading.session import PaperSession
from src.paper_trading.venue_rules import fixture_venue
from src.utilities.classes import (LiteOrderBookSnapshotEvent, MarketInfo,
    OrderRecord, OrderStatus, Side, TradeEvent, TradingConfig)
from src.utilities.market_events import (MarketStatusEvent, PublicConnectionEvent,
    PublicEventEnvelope, SettlementEvent)

from bot_v1_support import START, valid_config


def metadata(exchange, identifier, **changes):
    market = MarketInfo(identifier, exchange, identifier, {}, True,
        {"YES": True, "NO": True}, tick_size=.01, quantity_step=1,
        min_quantity=1, fee_per_contract=0, resolution_date=START + 3600)
    return replace(market, **changes)


def event(kind, exchange, identifier, *, outcome="YES"):
    if kind == "settlement":
        return SettlementEvent(exchange=exchange, market_id=identifier, timestamp=START,
            settlement_id=f"payout-{exchange}-{identifier}", payouts={"YES": "1", "NO": "0"})
    if kind == "status":
        return MarketStatusEvent(exchange=exchange, market_id=identifier, timestamp=START, status="OPEN")
    if kind == "trade":
        return TradeEvent(identifier, outcome, START, exchange, .5, 1, Side.BUY,
            f"print-{exchange}-{identifier}")
    return LiteOrderBookSnapshotEvent(identifier, outcome, START, exchange, .49, .51)


def private_report(exchange, identifier):
    return OrderRecord(f"order-{exchange}-{identifier}", identifier, exchange, "YES", Side.BUY,
        1, .5, OrderStatus.FILLED, strategy="one", portfolio_id="PAPER",
        filled_quantity=1, average_fill_price=.5, fees_paid=0, cumulative_notional=".5",
        intent_id=f"intent-{exchange}-{identifier}", revision=1)


def scenario(name):
    if name == "repeated-metadata":
        rows = [("market", metadata("A", f"m{i}")) for i in range(7)]
        rows += [("market", metadata("A", "m0", slug=f"refresh-{i}")) for i in range(20)]
        rows += [("event", PublicConnectionEvent(exchange="A", timestamp=START, connected=True))
                 for _ in range(20)]
        return rows + [("market", metadata("A", "m7"))], False
    if name == "cross-venue-eight":
        return [("market", metadata(venue, f"m{i}")) for venue in ("A", "B") for i in range(4)], False
    if name == "cross-venue-ninth":
        return [("market", metadata("A", f"m{i}")) for i in range(8)] + [("market", metadata("B", "m0"))], True
    if name == "mixed-ninth":
        rows = [("market", metadata("A", f"m{i}")) for i in range(4)]
        rows += [("event", event("settlement", "A", f"m{i}")) for i in range(4, 8)]
        return rows + [("event", event("lite", "A", "m8"))], True
    kind, cardinality = name.rsplit("-", 1)
    count = 8 if cardinality == "eight" else 9
    if kind == "metadata":
        return [("market", metadata("A", f"m{i}")) for i in range(count)], count > 8
    if kind == "private":
        return [("private", private_report("A", f"m{i}")) for i in range(count)], count > 8
    return [("event", event(kind, "A", f"m{i}")) for i in range(count)], count > 8


async def write_legacy_capture(directory, rows):
    config = TradingConfig()
    config.strategy_portfolios = {"one": "PAPER"}
    config.paper_starting_balances = {"PAPER": {"A": 1000, "B": 1000}}
    config.paper_execution = PaperExecutionConfig({venue: fixture_venue(venue) for venue in ("A", "B")})
    session = PaperSession(config, VirtualClock(START), capture_directory=directory)
    try:
        for index, (kind, value) in enumerate(rows):
            if kind == "market":
                await session.add_market(value)
            elif kind == "private":
                await session._apply_cumulative_update(value)
            else:
                envelope = PublicEventEnvelope(event=value, received_at=START,
                    available_at=START, ingest_index=index, source_id=f"source-{index}")
                # Exercise both recorded public input paths. The genuine legacy
                # session is intentionally unbounded, so it can create over-cap
                # captures for the new bounded facade to reject.
                if index % 2:
                    await session.apply_public_immediately(envelope)
                else:
                    session.publish(envelope)
                await session.advance(START)
        await session.finish(directory)
    finally:
        if not session.closed:
            await session.finish(finalize_capture=False)
    assert json.loads((directory / "manifest.json").read_text())["schema_version"] == 2
    return session


def retained_market_identities(session):
    identities = {(value.exchange, value.market_id) for value in session.state.markets.values()}
    identities.update(session.engine.books)
    identities.update(session.settled_markets)
    identities.update((value.exchange, value.market_id) for value in session.state.orderbooks.values())
    identities.update((value.exchange, value.market_id) for value in session.state.market_quotes.values())
    for account in session.state.portfolios.values():
        identities.update((value.exchange, value.market_id) for value in account.positions.values())
    return identities


def observe_replay_session(monkeypatch):
    instances = []
    original = PaperSession.__init__

    def observe(session, *args, **kwargs):
        original(session, *args, **kwargs)
        instances.append(session)

    monkeypatch.setattr(PaperSession, "__init__", observe)
    return instances


def assert_replay_closed(session):
    assert session.closed
    assert session.audit_file.closed
    store = session.state.history_store
    assert store._closed and not store._thread.is_alive()


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["metadata-eight", "metadata-ninth", "repeated-metadata",
    "cross-venue-eight", "cross-venue-ninth", "settlement-eight", "settlement-ninth",
    "lite-ninth", "status-ninth", "trade-ninth", "mixed-ninth", "private-eight", "private-ninth"])
async def test_legacy_replay_admits_only_eight_unique_venue_market_identities(tmp_path, monkeypatch, name):
    rows, exceeds = scenario(name)
    capture, output = tmp_path / "capture", tmp_path / "replay"
    legacy = await write_legacy_capture(capture, rows)
    legacy_count = len(retained_market_identities(legacy))
    assert legacy_count == (9 if exceeds else 8), "Legacy fixture was unexpectedly capacity-limited"
    original_inputs = sha256((capture / "inputs.jsonl").read_bytes()).hexdigest()
    instances = observe_replay_session(monkeypatch)
    if exceeds:
        with pytest.raises((BufferError, ValueError), match="market"):
            await replay_bot(capture, output)
    else:
        summary = await replay_bot(capture, output)
        assert summary["runtime"]["clean"] is True
        assert summary["runtime"]["compatibility_profile"] is True
        assert summary["runtime"]["limits"]["max_markets"] == 8
    assert len(instances) == 1
    replay = instances[0]
    admitted = retained_market_identities(replay)
    assert len(admitted) == 8
    if name == "cross-venue-eight":
        assert admitted == {(venue, f"m{i}") for venue in ("A", "B") for i in range(4)}
    elif name == "cross-venue-ninth":
        assert admitted == {("A", f"m{i}") for i in range(8)}
        assert "B:m0" not in replay.state.markets
    if name == "repeated-metadata":
        assert replay.state.markets["A:m0"].slug == "refresh-19"
    assert_replay_closed(replay)
    outcome = json.loads((output / "outcome.json").read_text())
    assert outcome["clean"] is (not exceeds)
    assert outcome["status"]["phase"] == ("failed" if exceeds else "stopped")
    if exceeds:
        assert "market" in outcome["status"]["reason"]
        if (output / "manifest.json").exists():
            assert json.loads((output / "manifest.json").read_text())["finalized"] is False
    assert sha256((capture / "inputs.jsonl").read_bytes()).hexdigest() == original_inputs


@pytest.mark.asyncio
async def test_bounded_replay_rejects_arbitrary_lite_outcomes_before_retaining_quote_keys(tmp_path, monkeypatch):
    rows = [("market", metadata("A", "m"))]
    rows += [("event", event("lite", "A", "m", outcome=outcome)) for outcome in ("YES", "NO")]
    rows += [("event", event("lite", "A", "m", outcome=f"unsupported-{i}")) for i in range(20)]
    capture, output = tmp_path / "capture", tmp_path / "replay"
    legacy = await write_legacy_capture(capture, rows)
    assert len(legacy.state.market_quotes) == 22
    instances = observe_replay_session(monkeypatch)
    with pytest.raises((BufferError, ValueError), match="outcome"):
        await replay_bot(capture, output)
    assert len(instances) == 1
    replay = instances[0]
    assert set(replay.state.market_quotes) == {"A:m:YES", "A:m:NO"}
    assert len(retained_market_identities(replay)) == 1
    assert_replay_closed(replay)
    outcome = json.loads((output / "outcome.json").read_text())
    assert outcome["clean"] is False and outcome["status"]["phase"] == "failed"


@pytest.mark.asyncio
async def test_bounded_private_ninth_market_is_rejected_before_capture_and_account_mutation(tmp_path):
    limits = valid_config()["runtime"]["limits"]
    store = HistoryStore(tmp_path, limits=limits)
    config = TradingConfig()
    config.strategy_portfolios = {"one": "PAPER"}
    config.paper_starting_balances = {"PAPER": {"A": 1000}}
    config.paper_execution = PaperExecutionConfig({"A": fixture_venue("A")})
    session = PaperSession(config, VirtualClock(START), capture_directory=tmp_path,
        history_store=store, limits=limits)
    try:
        for index in range(8):
            await session._apply_cumulative_update(private_report("A", f"m{index}"))
        before_count = session.recorder.count
        with pytest.raises((BufferError, ValueError), match="market"):
            await session._apply_cumulative_update(private_report("A", "m8"))
        assert session.recorder.count == before_count == 8
        account = session.state.portfolios["PAPER"]
        assert account.balances["A"]["USD"].cash == 996
        assert set(account.positions) == {f"A:m{i}:YES" for i in range(8)}
        assert "A:order-A-m8" not in account.order_history
    finally:
        await session.finish(finalize_capture=False)
        await store.aclose()
    assert json.loads((tmp_path / "manifest.json").read_text())["finalized"] is False
