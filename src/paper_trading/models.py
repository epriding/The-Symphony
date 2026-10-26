"""Explicit execution assumptions. Default fixtures are never calibrated profiles."""
from dataclasses import dataclass, field
from decimal import Decimal
import math


def dec(value):
    if isinstance(value, bool):
        raise ValueError("Boolean is not a numeric amount")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Nonfinite amount")
    return result


@dataclass(frozen=True)
class Delay:
    seconds: float
    jitter: float = 0.0

    def __post_init__(self):
        if any(isinstance(v, bool) or not math.isfinite(v) or v < 0
               for v in (self.seconds, self.jitter)) or self.jitter > self.seconds:
            raise ValueError("Delay requires finite nonnegative base >= jitter")

    def sample(self, rng):
        return self.seconds if not self.jitter else rng.uniform(
            self.seconds - self.jitter, self.seconds + self.jitter)


@dataclass(frozen=True)
class FeeSchedule:
    version: str
    model: str
    taker: str
    maker: str = "0"
    maker_rebate: str = "0"
    balance_precision: str = "0.01"
    effective_from: float = 0.0
    effective_until: float | None = None

    def __post_init__(self):
        if self.model not in {"polymarket_us", "kalshi", "flat"} or not self.version:
            raise ValueError("Unknown fee model or missing version")
        if any(dec(x) < 0 for x in (self.taker, self.maker, self.maker_rebate)):
            raise ValueError("Fee coefficients must be nonnegative")
        if dec(self.balance_precision) <= 0:
            raise ValueError("Invalid account precision")
        if not math.isfinite(self.effective_from) or (self.effective_until is not None and (
                not math.isfinite(self.effective_until) or self.effective_until <= self.effective_from)):
            raise ValueError("Invalid fee effective dates")


@dataclass(frozen=True)
class VenueRules:
    exchange: str
    fees: FeeSchedule
    max_book_age: float
    outbound: Delay
    acknowledgement: Delay
    execution_report: Delay
    cancel: Delay
    request_rate: float
    request_capacity: float
    rate_policy: str = "reject"
    submit_cost: float = 1.0
    cancel_cost: float = 1.0
    netted_outcomes: bool = True
    post_only: bool = True
    cancel_on_disconnect: bool = False
    supported_tifs: tuple[str, ...] = ("FOK", "IOC", "GTC", "GTD")
    market_fee_overrides: dict[str, FeeSchedule] = field(default_factory=dict)

    def __post_init__(self):
        values = (self.max_book_age, self.request_rate, self.request_capacity,
                  self.submit_cost, self.cancel_cost)
        if not self.exchange or any(isinstance(v, bool) or not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError("Venue age and request budgets must be explicit positive values")
        if self.rate_policy not in {"queue", "reject"}:
            raise ValueError("Rate policy must be queue or reject")
        if max(self.submit_cost, self.cancel_cost) > self.request_capacity:
            raise ValueError("Request cost exceeds capacity")

    def fee_schedule(self, market_id, timestamp):
        fee = self.market_fee_overrides.get(market_id, self.fees)
        if timestamp < fee.effective_from or (fee.effective_until is not None and timestamp >= fee.effective_until):
            raise ValueError("fee_schedule_outside_effective_period")
        return fee


@dataclass(frozen=True)
class PaperExecutionConfig:
    venues: dict[str, VenueRules]
    seed: int = 0
    session_id: str = "paper"
    processing_delay: Delay = field(default_factory=lambda: Delay(0))
    max_pending_events: int = 100_000
    max_outbox: int = 10_000
    assumptions: str = "hypothetical; not calibrated"
    participation_alert: float = 0.1
    markout_horizons: tuple[float, ...] = (0.1, 1.0, 5.0)
    lose_acknowledgements: bool = False

    def __post_init__(self):
        if not self.venues or any(k != v.exchange for k, v in self.venues.items()):
            raise ValueError("Explicit venue profiles are required")
        if not self.session_id or type(self.seed) is not int:
            raise ValueError("Invalid session ID or seed")
        if any(type(v) is not int or v < 1 for v in (self.max_pending_events, self.max_outbox)):
            raise ValueError("Buffers must be bounded positive integers")
        if not 0 < self.participation_alert <= 1 or any(
                not math.isfinite(v) or v <= 0 for v in self.markout_horizons):
            raise ValueError("Invalid reporting thresholds")


@dataclass(frozen=True)
class Execution:
    execution_id: str
    order_id: str
    exchange: str
    market_id: str
    outcome: str
    side: str
    quantity: str
    price: str
    notional: str
    fee: str
    credit: str
    role: str
    timestamp: float
    book_revision: str
    fee_version: str
    decision_quote: float | None = None
    arrival_quote: float | None = None
    decision_mid: float | None = None
    fee_details: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class CashCredit:
    credit_id: str
    portfolio_id: str
    exchange: str
    amount: str
    strategy: str
    order_id: str
    timestamp: float
