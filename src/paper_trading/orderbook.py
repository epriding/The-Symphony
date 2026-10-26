"""One YES-denominated liquidity pool with private, shared paper consumption."""
from dataclasses import dataclass, field
from decimal import Decimal
from .models import dec
from ..utilities.trading_rules import valid_price


def canonical_order(outcome, side, price):
    buy = side == "BUY"
    if outcome == "NO":
        return ("BUY" if not buy else "SELL"), Decimal(1) - dec(price)
    if outcome != "YES":
        raise ValueError("Unknown outcome")
    return side, dec(price)


@dataclass
class BookPool:
    observed: dict = field(default_factory=lambda: {"BUY": {}, "SELL": {}})
    available: dict = field(default_factory=lambda: {"BUY": {}, "SELL": {}})
    received_at: float = 0.0
    revision: str = ""
    epoch: int = 0
    valid: bool = False
    state: str = "OPEN"
    reason: str = "depth_unavailable"
    # Last observed revision/receipt at which each price changed.
    changed_at: dict = field(default_factory=dict)
    quarantined: set = field(default_factory=set)

    def snapshot(self, event, envelope, market):
        """Apply depth and report queue resets; duplicates/locked books return None."""
        if envelope.source_id and envelope.source_id == self.revision and envelope.connection_epoch == self.epoch:
            return None
        if self.revision and envelope.connection_epoch == self.epoch and envelope.received_at < self.received_at:
            self.invalidate("out_of_order_depth")
            raise ValueError("Out-of-order public book")
        bids = {dec(price): dec(quantity) for price, quantity in event.bids.items() if dec(quantity) != 0}
        asks = {dec(price): dec(quantity) for price, quantity in event.asks.items() if dec(quantity) != 0}
        if event.outcome == "NO":
            bids, asks = (
                {1 - price: quantity for price, quantity in asks.items()},
                {1 - price: quantity for price, quantity in bids.items()},
            )
        step = dec(market.quantity_step)
        dec(market.tick_size)  # Reject nonfinite tick metadata even for empty books.
        for levels in (bids, asks):
            if any(not valid_price(market, price) or quantity < 0 or quantity % step
                   for price, quantity in levels.items()):
                self.invalidate("invalid_depth")
                raise ValueError("Invalid price/quantity in executable book")
        if bids and asks and max(bids) >= min(asks):
            self.invalidate("locked_or_crossed_depth")
            return
        replacement = {"BUY": bids, "SELL": asks}
        new_epoch = envelope.connection_epoch != self.epoch
        recovering = not self.valid
        for side in ("BUY", "SELL"):
            previous_levels = self.observed[side]
            available = self.available[side]
            updated = {}
            for price, quantity in replacement[side].items():
                previous = previous_levels.get(price, Decimal(0))
                replenished = available.get(price, Decimal(0)) + quantity - previous
                updated[price] = max(Decimal(0), min(quantity, replenished))
                if new_epoch or not previous_levels:
                    updated[price] = quantity
                if quantity != previous:
                    self.changed_at[(side, price)] = envelope.received_at
            self.available[side] = updated
        self.observed = replacement
        self.received_at, self.revision = envelope.received_at, envelope.source_id or str(envelope.ingest_index)
        self.epoch = envelope.connection_epoch
        self.valid = envelope.depth_coverage == "full"
        self.state = envelope.market_state
        self.reason = "" if self.valid else "incomplete_depth"
        self.quarantined.clear()  # authoritative post-print snapshot resolves uncertainty
        return recovering or new_epoch

    def invalidate(self, reason):
        self.valid, self.reason = False, reason

    def executable(self, now, max_age):
        """Return the blocking reason, or an empty string when depth is usable."""
        if not self.valid:
            return self.reason or "depth_unavailable"
        if self.state not in {"OPEN", "MARKET_STATE_OPEN", "active", "open"}:
            return "market_unavailable"
        if not 0 <= now - self.received_at <= max_age:
            return "stale_depth"
        return ""

    def levels(self, side, bound):
        opposite = "SELL" if side == "BUY" else "BUY"
        levels = self.available[opposite]
        for price in sorted(levels, reverse=side == "SELL"):
            if (side == "BUY" and price > bound) or (side == "SELL" and price < bound):
                break
            if (opposite, price) not in self.quarantined:
                yield price, levels[price]

    def quote(self, side, outcome="YES"):
        canonical_side, _ = canonical_order(outcome, side, ".5")
        opposite = "SELL" if canonical_side == "BUY" else "BUY"
        prices = [price for price, quantity in self.observed[opposite].items() if quantity > 0]
        if not prices:
            return None
        price = min(prices) if canonical_side == "BUY" else max(prices)
        return float(price if outcome == "YES" else 1 - price)
