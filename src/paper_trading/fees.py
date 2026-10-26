"""Exact execution fees and conservative order reserves, with per-order state.

US: https://docs.polymarket.us/fees (2026-10-19).
Kalshi: https://docs.kalshi.com/getting_started/fee_rounding.
Coefficients/effective dates are supplied by the account/market profile.
"""
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN
from .models import dec


def round_unit(value, unit, rounding):
    return (value / unit).to_integral_value(rounding=rounding) * unit


@dataclass
class FeeAccumulator:
    exact_taker: Decimal = Decimal(0)
    charged_taker: Decimal = Decimal(0)
    rounding_overpayment: Decimal = Decimal(0)
    last_breakdown: dict[str, str] = field(default_factory=dict)


class FeeModel:
    def __init__(self, schedule):
        self.schedule = schedule

    def charge(self, quantity, price, side, role, accumulator):
        """Return debit and separate credit; mutate only the supplied accumulator."""
        quantity = dec(quantity)
        price = dec(price)
        schedule = self.schedule
        coefficient = dec(schedule.taker if role == "taker" else schedule.maker)
        exact = coefficient * quantity * (price * (1 - price) if schedule.model != "flat" else 1)
        credit = Decimal(0)
        alignment = Decimal(0)
        refund = Decimal(0)
        cap_reduction = Decimal(0)
        if schedule.model == "polymarket_us":
            cent = Decimal(".01")
            fee = round_unit(exact, cent, ROUND_HALF_EVEN)
            model_fee = fee
            if role == "taker":
                accumulator.exact_taker += exact
                cap = round_unit(accumulator.exact_taker, cent, ROUND_HALF_EVEN)
                fee = min(fee, max(Decimal(0), cap - accumulator.charged_taker))
                cap_reduction = model_fee - fee
                accumulator.charged_taker += fee
            elif dec(schedule.maker_rebate):
                rebate = dec(schedule.maker_rebate) * quantity * price * (1 - price)
                credit = round_unit(rebate, cent, ROUND_HALF_EVEN)
        elif schedule.model == "kalshi":
            fee = round_unit(exact, Decimal(".000001"), ROUND_CEILING)
            model_fee = fee
            revenue = quantity * price * (-1 if side == "BUY" else 1)
            precision = dec(schedule.balance_precision)
            aligned = round_unit(revenue - fee, precision, ROUND_FLOOR)
            rounding_fee = revenue - fee - aligned
            alignment = rounding_fee
            accumulator.rounding_overpayment += rounding_fee
            refund = min(round_unit(accumulator.rounding_overpayment, precision, ROUND_FLOOR),
                         round_unit(fee + rounding_fee, precision, ROUND_FLOOR))
            accumulator.rounding_overpayment -= refund
            # Kalshi's immediate rounding refund is part of its nonnegative net fee.
            fee += rounding_fee - refund
        else:
            fee = exact
            model_fee = exact
        accumulator.last_breakdown = {
            "exact_formula_fee": str(exact),
            "model_fee": str(model_fee),
            "balance_rounding_adjustment": str(alignment),
            "immediate_rounding_refund": str(refund),
            "taker_cap_reduction": str(cap_reduction),
            "charged_fee": str(fee),
            "maker_credit": str(credit),
        }
        return fee, credit

    def reserve(self, quantity, quantity_step, side, bound):
        """Bound all allowed prices, per-fill rounding, and account alignment.

        Fills are allocated in at least quantity_step increments. US taker
        rounding has an order-wide cap. Kalshi's rounding accumulator bounds
        unrefunded alignment by one account unit plus per-fill fee rounding.
        """
        quantity = dec(quantity)
        step = dec(quantity_step)
        bound = dec(bound)
        if quantity <= 0:
            return Decimal(0)
        coefficient = max(dec(self.schedule.taker), dec(self.schedule.maker))
        if self.schedule.model == "flat":
            return quantity * coefficient
        lower, upper = (Decimal(0), bound) if side == "BUY" else (bound, Decimal(1))
        maximizing_price = min(upper, max(lower, Decimal(".5")))
        exact = quantity * coefficient * maximizing_price * (1 - maximizing_price)
        if self.schedule.model == "polymarket_us":
            # Round up rather than half even to bound the cumulative cap.
            reserve = round_unit(exact, Decimal(".01"), ROUND_CEILING)
            if dec(self.schedule.maker):
                # Maker fees lack the order-wide taker cap. Bound half-cent
                # rounding for every possible minimum-quantity fragment.
                max_fragments = (quantity / step).to_integral_value(rounding=ROUND_CEILING)
                reserve += max_fragments * Decimal(".005")
            return reserve
        max_fragments = (quantity / step).to_integral_value(rounding=ROUND_CEILING)
        return exact + max_fragments * Decimal(".000001") + dec(self.schedule.balance_precision)


class FeeReserveProvider:
    def __init__(self, config, clock):
        self.config = config
        self.clock = clock

    def __call__(self, intent, market, quantity, bound):
        rules = self.config.venues[intent.exchange]
        schedule = rules.fee_schedule(market.market_id, self.clock())
        return float(FeeModel(schedule).reserve(quantity, market.quantity_step, intent.side.value, bound))
