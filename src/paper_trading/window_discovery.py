"""Bounded official-series discovery for normally matching Bitcoin 15m windows.

Uses typed US assetPriceTerms and Kalshi KXBTC15M open/close measurement
boundaries plus the sampled contract calculation. Unknown terms fail closed.
No settlement payout is inferred here.
"""
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from ..api.interfaces.public_market_data import market_metadata
from .models import dec


@dataclass(frozen=True)
class WindowMarket:
    market: object
    subscription: str
    terms: tuple
    event_ticker: str = ''
    fee_coefficient: str | None = None


def _utc(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.utcoffset() is None or stamp.utcoffset().total_seconds() != 0:
        raise ValueError('Window boundaries must be UTC')
    return stamp.timestamp()


def normalize_window(venue, raw):
    if venue == 'POLYMARKET_US':
        terms = raw['assetPriceTerms']
        if (terms['marketType'] != 'ASSET_PRICE_MARKET_TYPE_UP_DOWN'
                or terms['asset']['symbol'].lower() != 'btc'
                or terms['asset']['assetClass'] != 'ASSET_CLASS_CRYPTO'
                or terms['indexSymbol'] != 'BRTI' or terms['horizon'] != '15m'
                or terms['priceDecimals'] != 2 or terms['priceToBeat']['currency'] != 'USD'):
            raise ValueError('Unknown US asset terms')
        start, end = _utc(terms['windowStart']), _utc(terms['windowEnd'])
        threshold = dec(terms['priceToBeat']['value']) if terms.get('priceToBeat') else None
        text = raw['description'].lower()
        if not all(token in text for token in ('cf benchmarks', 'brti', '60', 'simple average',
                    '2 decimal places', 'greater than or equal to')):
            raise ValueError('Unknown US calculation/outcome mapping')
        if raw.get('outcomes') not in ('["Yes","No"]', '["Yes", "No"]', ['Yes','No']):
            raise ValueError('Unknown US outcomes')
        subscription = raw['slug']
        tick = raw['orderPriceMinTickSize']
    elif venue == 'KALSHI':
        if (not raw['ticker'].startswith('KXBTC15M-')
                or not raw['event_ticker'].startswith('KXBTC15M-')
                or raw['strike_type'] != 'greater_or_equal'
                or int(raw.get('custom_strike',{}).get('round_digits',2)) != 2):
            raise ValueError('Unknown Kalshi series/strike')
        start, end = _utc(raw['open_time']), _utc(raw['close_time'])
        threshold = dec(raw['floor_strike']) if raw.get('floor_strike') is not None else None
        text = (raw['rules_primary']+' '+raw['rules_secondary']).lower()
        if not all(token in text for token in ('cf benchmarks', 'brti', 'sixty seconds',
                                              '60 rti prices', '2 decimal places', 'at least')):
            raise ValueError('Unknown Kalshi calculation/outcome mapping')
        subscription = raw['ticker']
        tick = min(dec(row['step']) for row in raw['price_ranges'])
    else:
        raise ValueError('Only US production and Kalshi are supported')
    if (end-start != 900 or start % 900 or (threshold is not None
            and (threshold <= 0 or threshold != threshold.quantize(Decimal('.01'))))):
        raise ValueError('Invalid 15m window/threshold')
    market = market_metadata(venue, raw, dict(tick_size=tick, quantity_step=1,
        min_quantity=max(1,float(raw.get('minimumTradeQty',1))), category='crypto'))
    return WindowMarket(market, subscription,
        ('BTC','BRTI',start,end,str(threshold) if threshold is not None else 'pending','2','gte','mean60'),
        raw.get('event_ticker',''), str(raw['feeCoefficient']) if 'feeCoefficient' in raw else None)


def match_windows(windows, *, now):
    grouped, reasons = defaultdict(lambda: defaultdict(list)), Counter()
    for window in windows:
        if window.terms[3] <= now or window.terms[2] > (int(now)//900+1)*900:
            continue
        grouped[window.terms[:4]+window.terms[5:]][window.market.exchange].append(window)
    pairs = []
    for terms, venues in sorted(grouped.items()):
        if set(venues) != {'KALSHI','POLYMARKET_US'} or any(len(v) != 1 for v in venues.values()):
            reasons['missing_or_ambiguous_counterpart'] += 1
            continue
        pair = tuple(venues[venue][0] for venue in sorted(venues))
        thresholds = {w.terms[4] for w in pair}
        if len(thresholds-{'pending'}) > 1 or ('pending' in thresholds and pair[0].terms[2] <= now):
            reasons['opening_threshold_unavailable_or_different'] += 1
            continue
        pairs.append(pair)
    return pairs, dict(reasons)


async def discover_windows(feeds, *, now, settings):
    windows, reasons = [], Counter()
    page_size, max_pages = settings.get('page_size',100), settings.get('max_pages',3)
    if type(page_size) is not int or not 1 <= page_size <= 200 or type(max_pages) is not int or not 1 <= max_pages <= 10:
        raise ValueError('Invalid discovery bounds')
    for feed in feeds:
        cursor = None
        for page in range(max_pages):
            rows, next_cursor = await feed.discover_windows(limit=page_size, cursor=cursor,
                series_id=settings.get('polymarket_series_id',375), now=now)
            if len(rows) > page_size:
                raise ValueError('Discovery response exceeds result bound')
            for raw in rows:
                try:
                    windows.append(normalize_window(feed.exchange,raw))
                except (ValueError, KeyError, TypeError, ArithmeticError):
                    reasons['unknown_or_pending_terms'] += 1
            if not next_cursor:
                break
            if next_cursor == cursor:
                reasons['repeated_cursor'] += 1
                break
            cursor = next_cursor
        else:
            reasons['page_limit'] += 1
    pairs, unmatched = match_windows(windows,now=now)
    reasons.update(unmatched)
    return pairs,dict(reasons)
