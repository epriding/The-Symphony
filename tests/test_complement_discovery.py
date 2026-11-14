import json
from copy import deepcopy
from pathlib import Path
import pytest
from src.paper_trading.window_discovery import normalize_window, match_windows, discover_windows

ROOT = Path(__file__).resolve().parents[1]/'docs/validation/bitcoin-15m-2026-11-14'


def raw(venue):
    name = 'kalshi-market' if venue == 'KALSHI' else 'poly-market'
    return json.loads((ROOT/(name+'.json')).read_text())['body']['market']


def test_actual_saved_pair_uses_measurement_boundaries_and_exact_threshold():
    k = normalize_window('KALSHI',raw('KALSHI'))
    p = normalize_window('POLYMARKET_US',raw('POLYMARKET_US'))
    assert k.terms == p.terms
    assert k.terms[3]-k.terms[2] == 900
    pairs, reasons = match_windows([k,p],now=k.terms[2]+10)
    assert len(pairs) == 1 and not reasons
    assert p.terms[3] != p.market.resolution_date  # administrative expiry differs


@pytest.mark.parametrize('change',['threshold','duration','asset','reference','tie','calculation'])
def test_missing_or_different_terms_never_admit_similar_titles(change):
    k = normalize_window('KALSHI',raw('KALSHI'))
    r = raw('POLYMARKET_US')
    terms = r['assetPriceTerms']
    if change == 'threshold': terms['priceToBeat']['value'] = '68209.63'
    if change == 'duration': terms['windowEnd'] = '2026-11-14T02:30:00Z'
    if change == 'asset': terms['asset']['symbol'] = 'eth'
    if change == 'reference': terms['indexSymbol'] = 'OTHER'
    if change == 'tie': r['description'] = r['description'].replace('greater than or equal to','greater than')
    if change == 'calculation': r['description'] = 'Bitcoin Up Down'
    try: p = normalize_window('POLYMARKET_US',r)
    except ValueError: return
    assert not match_windows([k,p],now=k.terms[2]+10)[0]


def test_ambiguous_counterpart_rejected_and_future_prepared_separately():
    k = normalize_window('KALSHI',raw('KALSHI')); p = normalize_window('POLYMARKET_US',raw('POLYMARKET_US'))
    assert not match_windows([k,p,p],now=k.terms[2])[0]
    assert len(match_windows([k,p],now=k.terms[2]-100)[0]) == 1
    assert not match_windows([k,p],now=k.terms[3])[0]


@pytest.mark.asyncio
async def test_discovery_follows_bounded_pages_and_skips_invalid_rows():
    class Feed:
        def __init__(self,venue): self.exchange=venue; self.calls=[]
        async def discover_windows(self, **kwargs):
            self.calls.append(kwargs)
            return [raw(self.exchange)], None
    feeds = [Feed(v) for v in ('KALSHI','POLYMARKET_US')]
    k = normalize_window('KALSHI',raw('KALSHI'))
    pairs,reasons = await discover_windows(feeds,now=k.terms[2],settings={})
    assert len(pairs) == 1 and all(len(f.calls)==1 for f in feeds)


@pytest.mark.asyncio
async def test_public_adapters_use_official_series_and_bounded_pagination():
    from unittest.mock import AsyncMock
    from types import SimpleNamespace
    from src.api.interfaces.kalshi import KalshiMarketData
    from src.api.interfaces.polymarket_us_public import PolymarketUSMarketData
    kapi = SimpleNamespace(get=AsyncMock(return_value={'markets':[raw('KALSHI')],'cursor':'next'}))
    k = KalshiMarketData([],api=kapi)
    rows,cursor = await k.discover_windows(limit=5)
    assert cursor == 'next' and len(rows)==1
    kapi.get.assert_awaited_once_with('/markets',{'series_ticker':'KXBTC15M','limit':5})
    p = PolymarketUSMarketData([],max_event_bytes=262144)
    try:
        request = AsyncMock(return_value={'events':[{'markets':[raw('POLYMARKET_US')]}]})
        p.api.client._request = request
        rows,cursor = await p.discover_windows(limit=5)
        assert len(rows)==1 and cursor is None
        assert request.call_args.kwargs['query']['seriesId']==375
    finally:
        await p.aclose()


@pytest.mark.asyncio
async def test_subscription_update_is_bounded_and_requests_resnapshot():
    import asyncio
    from src.api.interfaces.polymarket_us_public import PolymarketUSMarketData
    from src.api.interfaces.kalshi import KalshiMarketData
    for feed in (PolymarketUSMarketData([],max_event_bytes=262144,max_markets=2),
                 KalshiMarketData([],max_event_bytes=262144,max_markets=2)):
        try:
            feed._resubscribe = asyncio.Event()
            feed.set_markets(['a','b'])
            assert feed.markets == ['a','b'] and feed._resubscribe.is_set()
            with pytest.raises(ValueError): feed.set_markets(['a','b','c'])
            assert feed.markets == ['a','b']
            feed.set_markets(['b'])
            assert feed.markets == ['b']
        finally:
            if hasattr(feed,'aclose'): await feed.aclose()
