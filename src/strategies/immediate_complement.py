"""Immediate complementary purchases using explicit paper execution assumptions."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from fractions import Fraction
from math import lcm
from hashlib import sha256

from ..paper_trading.fees import FeeModel
from ..paper_trading.models import FeeSchedule, dec
from ..utilities.classes import Side, TimeInForce


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


class ImmediateComplementStrategy:
    """One immediate pair attempt per window; reduce observed excess on failure."""
    needs_account_ticks = True

    def __init__(self, context):
        self.context = context
        self.active = None
        self.last_window = None
        self.last_reason = 'awaiting_pair'
        self.last_candidate = None
        self.options = dict(max_pair_spend='5', max_quantity='10',
            min_net_profit_per_pair='.02', max_book_age=3, max_quote_skew=1,
            entry_timeout=1, recovery_timeout=5, recovery_attempts=3,
            entry_cutoff=30, max_candidates=1000)
        self.options.update(context.settings)
        for name in ('max_pair_spend', 'max_quantity', 'min_net_profit_per_pair',
                     'max_book_age', 'entry_timeout', 'recovery_timeout', 'entry_cutoff'):
            if dec(self.options[name]) <= 0:
                raise ValueError(f'Invalid complement setting: {name}')
        if dec(self.options['max_quote_skew']) < 0:
            raise ValueError('Invalid max_quote_skew')
        for name in ('recovery_attempts', 'max_candidates'):
            if type(self.options[name]) is not int or not 0 < self.options[name] <= 10000:
                raise ValueError(f'Invalid complement setting: {name}')

    async def on_snapshot(self, snapshot):
        if self.active is not None:
            return self._recover(snapshot)
        markets = self.context.settings.get('markets', {})
        window = tuple(sorted(markets.items()))
        if len(window) != 2 or window == self.last_window or self.context.execution is None:
            return None
        choices = []
        for outcomes in (('YES', 'NO'), ('NO', 'YES')):
            quotes = []
            for (venue, market_id), outcome in zip(window, outcomes):
                market = snapshot.markets.get(f'{venue}:{market_id}')
                book = snapshot.orderbooks.get(f'{venue}:{market_id}:{outcome}')
                terms = tuple(self.context.settings.get('terms', {}).get(venue, ()))
                if (market is None or book is None or not market.active or len(terms) < 4
                        or self.context.now() >= float(terms[3]) - float(self.options['entry_cutoff'])):
                    break
                rules = self.context.execution.venues[venue]
                account = snapshot.portfolios[self.context.portfolio_id]
                cash = dec(account.balances[venue]['USD'].buying_power)
                quotes.append(LegQuote(venue, market_id, outcome,
                    tuple((dec(p), dec(q)) for p, q in book.asks.items()),
                    dec(market.quantity_step), dec(market.min_quantity), cash,
                    book.timestamp, rules.fee_schedule(market_id, self.context.now()), terms))
            if len(quotes) != 2:
                continue
            candidate, self.last_reason = evaluate_pair(quotes, now=self.context.now(),
                max_spend=dec(self.options['max_pair_spend']),
                min_profit=dec(self.options['min_net_profit_per_pair']),
                max_quantity=dec(self.options['max_quantity']),
                max_age=min(float(self.options['max_book_age']),
                            *(self.context.execution.venues[q.exchange].max_book_age for q in quotes)),
                max_skew=self.options['max_quote_skew'], max_candidates=self.options['max_candidates'])
            if candidate:
                choices.append(candidate)
        if not choices:
            return None
        candidate = max(choices, key=lambda c: c.surplus)
        now = self.context.now()
        deadline = now + float(self.options['entry_timeout'])
        legs = [self.context.new_intent(exchange=q.exchange, market_id=q.market_id,
            outcome=q.outcome, side=Side.BUY, price=float(price), tif=TimeInForce.FOK,
            expires_at=deadline, resolution_date=float(q.terms_key[3]), reason='immediate_complement')
            for q, price in zip(candidate.legs, candidate.prices)]
        batch = self.context.new_batch(legs=legs, quantity_ratios=[1, 1],
            capital_ceiling=float(dec(self.options['max_pair_spend'])),
            max_quantity=float(candidate.quantity),
            min_net_profit_per_pair=float(dec(self.options['min_net_profit_per_pair'])),
            expires_at=deadline, execution_deadline=deadline,
            execution_policy='paper_independent_legs',
            market_equivalence=sha256(repr(candidate.legs[0].terms_key).encode()).hexdigest())
        self.active = dict(batch_id=batch.batch_id, legs=[(q.exchange, q.market_id, q.outcome)
                           for q in candidate.legs], attempts=0,
                           recovery_end=deadline+float(self.options['recovery_timeout']))
        self.last_window = window
        self.last_reason = 'pair_proposed'
        self.last_candidate = dict(batch_id=batch.batch_id,quantity=str(candidate.quantity),
            projected_surplus=str(candidate.surplus),prices=[str(p) for p in candidate.prices],
            fee_reserves=[str(f) for f in candidate.fee_reserves],
            window_start=candidate.legs[0].terms_key[2],window_end=candidate.legs[0].terms_key[3])
        return [batch]

    def _recover(self, snapshot):
        account = snapshot.portfolios[self.context.portfolio_id]
        pending = [o for o in account.pending_orders if o.strategy == self.context.strategy_name]
        if pending:
            self.last_reason = 'awaiting_execution_reports'
            return None
        positions = account.strategy_positions.get(self.context.strategy_name, {})
        keys = [':'.join(leg) for leg in self.active['legs']]
        sizes = [dec(positions[key].size) if key in positions else Decimal(0) for key in keys]
        pair = account.complement_pairs.get(self.active['batch_id'])
        matched = dec(min(pair['fills'])) if pair else min(sizes)
        excesses = [max(Decimal(0), size-matched) for size in sizes]
        if not any(excesses):
            self.last_reason = 'holding_pair' if matched else 'attempt_complete'
            self.active = None
            return None
        self.last_reason = 'unresolved_excess'
        if (self.context.now() > self.active['recovery_end']
                or self.active['attempts'] >= self.options['recovery_attempts']):
            return None
        index = 0 if excesses[0] > excesses[1] else 1
        venue, market_id, outcome = self.active['legs'][index]
        book = snapshot.orderbooks.get(keys[index])
        market = snapshot.markets.get(f'{venue}:{market_id}')
        if (not book or not book.bids or not market or not market.active
                or not 0 <= self.context.now() - book.timestamp <= float(self.options['max_book_age'])):
            return None
        self.active['attempts'] += 1
        return [self.context.new_intent(exchange=venue, market_id=market_id, outcome=outcome,
            side=Side.SELL, price=max(book.bids), tif=TimeInForce.IOC, reduce_only=True,
            close_fraction=float(excesses[index]/sizes[index]), reason='complement_recovery')]

    async def on_shutdown(self, snapshot):
        return self._recover(snapshot) if self.active is not None else None
