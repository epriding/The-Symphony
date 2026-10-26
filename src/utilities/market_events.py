"""Neutral public-data provenance shared by adapters, capture, and execution."""
from dataclasses import dataclass
from typing import Any
import math
import time


@dataclass(frozen=True, kw_only=True)
class PublicConnectionEvent:
    exchange: str
    timestamp: float
    connected: bool
    market_id: str = ""
    reason: str = "connection_change"


@dataclass(frozen=True, kw_only=True)
class MarketStatusEvent:
    exchange: str
    market_id: str
    timestamp: float
    status: str


@dataclass(frozen=True, kw_only=True)
class SettlementEvent:
    exchange: str
    market_id: str
    timestamp: float
    settlement_id: str
    payouts: dict[str, str]


@dataclass(frozen=True, kw_only=True)
class PublicEventEnvelope:
    event: Any
    received_at: float
    available_at: float
    ingest_index: int
    source_id: str = ""
    source_sequence: int | None = None
    connection_epoch: int = 0
    timestamp_quality: str = "venue"
    depth_coverage: str = "full"
    eligible_trade: bool = True
    market_state: str = "OPEN"
    received_monotonic_ns: int = 0

    def __post_init__(self):
        if (not all(math.isfinite(x) for x in (self.received_at, self.available_at))
                or self.available_at < self.received_at or self.ingest_index < 0):
            raise ValueError("Invalid public event timing")


class ReceiptSequencer:
    """One collector-wide counter, called before asynchronous normalization."""
    def __init__(self):
        self.index = 0

    def receipt(self):
        self.index += 1
        return dict(received_at=time.time(), received_monotonic_ns=time.monotonic_ns(),
                    ingest_index=self.index)
