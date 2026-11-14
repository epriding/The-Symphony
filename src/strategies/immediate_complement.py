"""Immediate complementary purchases using explicit paper execution assumptions."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from fractions import Fraction
from math import lcm

from ..paper_trading.fees import FeeModel
from ..paper_trading.models import FeeSchedule, dec


@dataclass(frozen=True)
class LegQuote:
    exchange: str
    market_id: str
    outcome: str
    asks: tuple[tuple[Decimal, Decimal], ...]
    quantity_step: Decimal
    min_quantity: Decimal
    cash: Decimal
    timestamp: float
    fees: FeeSchedule
    terms_key: tuple = ()


@dataclass(frozen=True)
class PairCandidate:
    legs: tuple[LegQuote, LegQuote]
    quantity: Decimal
    prices: tuple[Decimal, Decimal]
    fee_reserves: tuple[Decimal, Decimal]
    quoted_cost: Decimal
    surplus: Decimal


def _common_step(legs):
    steps = [Fraction(leg.quantity_step) for leg in legs]
    denominator = lcm(*(step.denominator for step in steps))
    units = lcm(*(step.numerator * (denominator // step.denominator) for step in steps))
    return Decimal(units) / Decimal(denominator)


def _depth_cost(leg, quantity):
    remaining, cost, bound = quantity, Decimal(0), Decimal(0)
    for price, size in sorted(leg.asks):
        fill = min(remaining, size)
        if fill:
            cost += fill * price
            bound = price
            remaining -= fill
        if not remaining:
            return cost, bound
    return None


def evaluate_pair(legs, *, now, max_spend, min_profit, max_quantity,
                  max_age, max_skew, max_candidates):
    """Return a bounded, fee-reserved equal-quantity candidate or a skip reason.

    Surplus assumes matching normal settlement, not guaranteed cross-venue finality.
    Orders are capped at the worst consumed price, so every permitted arrival fill
    remains within the evaluated cost. The coordinator rechecks the final size.
    """
    if (len(legs) != 2 or type(max_candidates) is not int or not 0 < max_candidates <= 10000
            or any(dec(value) <= 0 for value in (max_spend, min_profit, max_quantity, max_age))
            or dec(max_skew) < 0):
        raise ValueError('Invalid pair evaluation bounds')
    if (not legs[0].terms_key or legs[0].terms_key != legs[1].terms_key
            or {leg.outcome for leg in legs} != {'YES', 'NO'}):
        return None, 'incompatible_terms'
    if any(not 0 <= dec(now) - dec(leg.timestamp) <= dec(max_age) for leg in legs):
        return None, 'stale_depth'
    if abs(dec(legs[0].timestamp) - dec(legs[1].timestamp)) > dec(max_skew):
        return None, 'quote_skew'
    for leg in legs:
        if not leg.asks:
            return None, 'missing_depth'
        if (dec(leg.quantity_step) <= 0 or dec(leg.min_quantity) <= 0 or dec(leg.cash) < 0
                or leg.outcome not in ('YES', 'NO')
                or any(not 0 < dec(price) < 1 or dec(size) <= 0 for price, size in leg.asks)):
            return None, 'invalid_depth'
    step = _common_step(legs)
    lower = int((max(leg.min_quantity for leg in legs) / step).to_integral_value(rounding=ROUND_CEILING))
    upper = min(int(dec(max_quantity) / step), lower + max_candidates - 1)
    best, affordable = None, False
    for units in range(max(1, lower), upper + 1):
        quantity = step * units
        costs = [_depth_cost(leg, quantity) for leg in legs]
        if any(value is None for value in costs):
            break
        prices = tuple(value[1] for value in costs)
        fees = tuple(FeeModel(leg.fees).reserve(quantity, leg.quantity_step, 'BUY', bound)
                     for leg, bound in zip(legs, prices))
        debits = [quantity * price + fee for price, fee in zip(prices, fees)]
        if sum(debits) > dec(max_spend) or any(debit > leg.cash for debit, leg in zip(debits, legs)):
            continue
        affordable = True
        surplus = quantity - sum(debits)
        if surplus < quantity * dec(min_profit):
            continue
        candidate = PairCandidate(tuple(legs), quantity, prices, fees,
                                  sum(value[0] for value in costs), surplus)
        if best is None or candidate.surplus > best.surplus:
            best = candidate
    return (best, 'eligible') if best else (None, 'no_net_edge' if affordable else 'insufficient_funds_or_depth')
