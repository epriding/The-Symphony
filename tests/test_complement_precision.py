from decimal import Decimal as D
import json
import pytest
from src.paper_trading.orderbook import BookPool
from src.paper_trading.models import FeeSchedule
from src.strategies.immediate_complement import LegQuote,evaluate_pair
from src.utilities.classes import OrderBookSnapshotEvent
from src.utilities.market_events import PublicEventEnvelope
from test_complement_discovery import normalize_window,raw,ROOT


@pytest.mark.parametrize('venue',['KALSHI','POLYMARKET_US'])
def test_actual_saved_fractional_depth_is_executable_under_discovered_precision(venue):
    market=normalize_window(venue,raw(venue)).market
    assert market.quantity_step==.01 and market.min_quantity==.01
    name='kalshi-book' if venue=='KALSHI' else 'poly-book'
    data=json.loads((ROOT/(name+'.json')).read_text())['body']
    if venue=='KALSHI':
        bids={float(p):float(q) for p,q in data['orderbook_fp']['yes_dollars']}
        asks={float(D(1)-D(p)):float(q) for p,q in data['orderbook_fp']['no_dollars']}
    else:
        bids={float(r['px']['value']):float(r['qty']) for r in data['marketData']['bids']}
        asks={float(r['px']['value']):float(r['qty']) for r in data['marketData']['offers']}
    event=OrderBookSnapshotEvent(market.market_id,'YES',0,venue,bids,asks)
    envelope=PublicEventEnvelope(event=event,received_at=0,available_at=0,ingest_index=1,source_id='actual-shape')
    pool=BookPool();pool.snapshot(event,envelope,market)
    assert pool.valid and pool.observed['BUY'] and pool.observed['SELL']


def test_whole_candidate_grid_does_not_underreserve_native_fractional_fill_fees():
    common=dict(quantity_step=D('.01'),min_quantity=D('.01'),cash=D(25),timestamp=0,
                terms_key=('same',),selection_step=D(1))
    a=LegQuote('A','m','YES',((D('.4'),D('99.99')),),
               fees=FeeSchedule('k','kalshi','.07'),**common)
    b=LegQuote('B','m','NO',((D('.5'),D('99.99')),),
               fees=FeeSchedule('u','polymarket_us','.095'),**common)
    candidate,reason=evaluate_pair((a,b),now=0,max_spend=D(3),min_profit=D('.02'),
        max_quantity=D('2.7'),max_age=3,max_skew=1,max_candidates=1000)
    assert reason=='eligible' and candidate.quantity==2
    # 200 possible native 0.01-contract fragments, rather than two whole fragments.
    assert candidate.fee_reserves[0]==D('.07')*2*D('.4')*D('.6')+D(200)*D('.000001')+D('.01')
