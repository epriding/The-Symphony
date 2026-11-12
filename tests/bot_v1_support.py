"""Offline input fixtures for the confirmed bot-v1 public interface.

This module supplies market data and strategies' configuration, never a runner,
matching engine, archive or account-ledger substitute. Values are deliberately
synthetic test limits and are not deployment recommendations.
"""
from copy import deepcopy
import asyncio
import sys

from src.paper_trading.clock import RealClock
from src.utilities.classes import MarketInfo, OrderBookSnapshotEvent
from src.utilities.market_events import PublicEventEnvelope, ReceiptSequencer


START = 1_800_000_000.0
US = "POLYMARKET_US"
KALSHI = "KALSHI"
VENUES = (US, KALSHI)


def strategy_config(name="one", *, factory=None, required_markets=None,
                    enabled=True, settings=None):
    return {
        "name": name,
        "factory": factory or name,
        "enabled": enabled,
        "required_markets": ([f"{venue}:m" for venue in VENUES]
                             if required_markets is None else list(required_markets)),
        "settings": {} if settings is None else deepcopy(settings),
    }


def valid_config(*, strategies=None, source="synthetic", observe_only=False):
    entries = [strategy_config()] if strategies is None else deepcopy(strategies)
    venue_rules = {
        venue: {
            "fees": {"version": "synthetic-zero-fee", "model": "flat",
                     "taker": "0", "maker": "0", "balance_precision": ".01",
                     "effective_from": 0},
            "max_book_age": 5.0,
            "outbound": {"seconds": .01},
            "acknowledgement": {"seconds": .01},
            "execution_report": {"seconds": .01},
            "cancel": {"seconds": .01},
            "request_rate": 1000,
            "request_capacity": 1000,
            "rate_policy": "reject",
            "netted_outcomes": True,
        } for venue in VENUES
    }
    return {
        "strategy_portfolios": {entry["name"]: "PAPER" for entry in entries},
        "starting_balances": {"PAPER": {venue: 1000.0 for venue in VENUES}},
        "risk": {"PAPER": {"diversification": {"other": 1.0},
                           "max_position_exposure": 10.0,
                           "max_total_exposure": 100.0,
                           "max_quote_age_seconds": 5.0}},
        "execution": {"seed": 7, "session_id": "bot-v1-synthetic",
                      "assumptions": "Synthetic offline acceptance fixture; not calibrated",
                      "processing_delay": {"seconds": 0},
                      "max_pending_events": 1024, "max_outbox": 128,
                      "markout_horizons": [.01, .05], "venues": venue_rules},
        "feeds": {venue: {"markets": ["m"], "read_interval": .01,
                           "market_rules": {"m": {"tick_size": ".01",
                                                      "quantity_step": "1",
                                                      "min_quantity": "1"}}}
                  for venue in VENUES},
        "strategies": entries,
        "runtime": {
            "source": source,
            "observe_only": observe_only,
            "startup_timeout_seconds": .5,
            "strategy_timeout_seconds": .2,
            "risk_interval_seconds": .02,
            "shutdown_grace_seconds": .25,
            "limits": {"max_strategies": 8, "max_markets": 8,
                       "max_active_orders": 64, "max_positions": 32,
                       "max_intents_per_callback": 8,
                       "max_hot_history_records": 16,
                       "max_archive_cache_records": 8,
                       "max_event_bytes": 262144,
                       "memory_budget_bytes": 64 * 1024 * 1024},
        },
    }


def market(exchange=US, market_id="m"):
    return MarketInfo(market_id, exchange, market_id, {}, True,
                      {"YES": True, "NO": True}, tick_size=.01,
                      quantity_step=1, min_quantity=1, fee_per_contract=0,
                      resolution_date=START + 10_000_000)


def book_event(exchange=US, *, market_id="m", bids=None, asks=None,
               timestamp=START):
    return OrderBookSnapshotEvent(
        market_id, "YES", timestamp, exchange,
        {.49: 1000} if bids is None else dict(bids),
        {.50: 1000} if asks is None else dict(asks),
    )


class ControlledFeed:
    """Existing public-feed protocol with test-controlled asynchronous inputs.

    The generator remains open until ``end``. Its ``finally`` records resource
    closure whether a runner closes, cancels or exhausts the stream. It contains
    no network transport or private-account capability.
    """
    _END = object()

    def __init__(self, exchange=US, *, markets=None, clock=None):
        self.exchange = exchange
        self.markets = ["m"] if markets is None else list(markets)
        self.clock = clock or RealClock()
        self.sequencer = ReceiptSequencer()
        self.inputs = asyncio.Queue(maxsize=2048)
        self.started = asyncio.Event()
        self.closed = asyncio.Event()
        self.emitted = 0
        self.metadata_reads = 0
        self.close_count = 0

    async def get_market(self, identifier):
        self.metadata_reads += 1
        return market(self.exchange, identifier)

    async def push(self, event):
        await self.inputs.put(event)

    async def push_book(self, *, market_id="m", bids=None, asks=None):
        await self.push(book_event(self.exchange, market_id=market_id,
                                   bids=bids, asks=asks, timestamp=self.clock()))

    async def end(self):
        await self.inputs.put(self._END)

    async def stream(self):
        self.started.set()
        try:
            while True:
                event = await self.inputs.get()
                try:
                    if event is self._END:
                        return
                    if isinstance(event, BaseException):
                        raise event
                    self.emitted += 1
                    receipt = self.sequencer.receipt()
                    now = self.clock()
                    # Real transport receipt order is retained, while test time
                    # supplies deterministic epoch/availability when injected.
                    receipt["received_at"] = now
                    yield PublicEventEnvelope(
                        event=event, available_at=now, **receipt,
                        source_id=f"{self.exchange}-synthetic-{self.emitted}",
                        connection_epoch=0,
                    )
                finally:
                    self.inputs.task_done()
        finally:
            self.close_count += 1
            self.closed.set()

    async def settlements(self):
        if False:
            yield None


async def eventually(predicate, *, timeout=2.0, task=None):
    """Yield until a visible condition holds, with a finite test-harness bound."""
    async def wait():
        while not predicate():
            if task is not None and task.done():
                # Preserve an early runner failure rather than hiding it behind
                # a later timeout waiting for a state it can no longer reach.
                await task
                raise AssertionError('Runner exited before the expected condition')
            await asyncio.sleep(.001)
    await asyncio.wait_for(wait(), timeout)


async def stop_and_join(runner, task, *, timeout=2.0):
    """Exercise the public stop request while bounding cleanup in the test."""
    runner.request_stop()
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout)
    finally:
        if not task.done():
            task.cancel()
            done, pending = await asyncio.wait({task}, timeout=timeout)
            if pending:
                message = "runner did not finish cooperative cancellation before cleanup deadline"
                original_error = sys.exception()
                if original_error is not None:
                    original_error.add_note(message)
                else:
                    raise AssertionError(message)
            else:
                await asyncio.gather(*done, return_exceptions=True)
