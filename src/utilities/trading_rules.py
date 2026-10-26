"""Venue price ladders expressed in the underlying YES instrument."""
from decimal import Decimal


def valid_price(market, price, outcome="YES"):
    value = Decimal(str(price))
    if not value.is_finite() or not 0 < value < 1:
        return False
    if market.price_ranges:
        canonical = value if outcome == "YES" else 1 - value
        for price_range in market.price_ranges:
            start = Decimal(price_range["start"])
            if start <= canonical <= Decimal(price_range["end"]):
                step = Decimal(price_range["step"])
                if (canonical - start) % step == 0:
                    return True
        return False
    return value % Decimal(str(market.tick_size)) == 0
