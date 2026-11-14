from decimal import Decimal as D
from dataclasses import replace

import pytest

from src.paper_trading.models import FeeSchedule
from src.strategies.immediate_complement import LegQuote, evaluate_pair


def leg(venue, price, *, fee='0', quantity_step='1', cash='25'):
    return LegQuote(venue, 'm', 'YES' if venue == 'KALSHI' else 'NO',
                    ((D(price), D('100')),), D(quantity_step), D(quantity_step),
                    D(cash), 100.0, FeeSchedule('test', 'flat', fee), ('BTC', 'BRTI', 0, 900, '100'))


def evaluate(a=None, b=None, **changes):
    values = dict(now=100, max_spend=D('5'), min_profit=D('.02'),
                  max_quantity=D('10'), max_age=3, max_skew=1, max_candidates=100)
    values.update(changes)
    return evaluate_pair((a or leg('KALSHI', '.4'), b or leg('POLYMARKET_US', '.5')), **values)


def test_pair_uses_net_profit_cash_and_maximum_spend():
    candidate, reason = evaluate()
    assert reason == 'eligible'
    assert candidate.quantity == D('5')
    assert candidate.quoted_cost == D('4.5')
    assert candidate.surplus == D('.5')
    assert candidate.prices == (D('.4'), D('.5'))


def test_positive_gross_edge_erased_by_fees_is_rejected():
    candidate, reason = evaluate(leg('KALSHI', '.4', fee='.06'),
                                 leg('POLYMARKET_US', '.5', fee='.06'))
    assert candidate is None and reason == 'no_net_edge'


@pytest.mark.parametrize('change,reason', [
    ({'timestamp': 96.9}, 'stale_depth'),
    ({'timestamp': 100.1}, 'stale_depth'),
    ({'timestamp': 98.5}, 'quote_skew'),
    ({'asks': ()}, 'missing_depth'),
    ({'cash': D('.1')}, 'insufficient_funds_or_depth'),
])
def test_unusable_books_do_not_produce_a_pair(change, reason):
    candidate, actual = evaluate(a=replace(leg('KALSHI', '.4'), **change))
    assert candidate is None and actual == reason


def test_common_quantity_grid_and_depth_are_respected():
    a = replace(leg('KALSHI', '.4', quantity_step='.3'), asks=((D('.4'), D('.9')),))
    b = leg('POLYMARKET_US', '.5', quantity_step='.2')
    candidate, _ = evaluate(a, b)
    assert candidate.quantity == D('.6')


def test_depth_walk_uses_worst_price_cap_and_reserve():
    a = replace(leg('KALSHI', '.4'), asks=((D('.4'), D('1')), (D('.6'), D('99'))))
    candidate, _ = evaluate(a)
    assert candidate.quantity == D('1')  # Larger quantities have no edge at their caps.
    assert candidate.quoted_cost == D('.9')


def test_fee_rounding_is_not_scaled_linearly():
    a = replace(leg('KALSHI', '.4'), fees=FeeSchedule('us', 'polymarket_us', '.095'))
    candidate, _ = evaluate(a, max_quantity=D('1'))
    assert candidate.fee_reserves[0] == D('.03')
    assert candidate.surplus == D('.07')


def test_invalid_or_unbounded_settings_are_rejected():
    with pytest.raises(ValueError):
        evaluate(max_candidates=0)


def test_mismatched_or_unverified_terms_are_rejected():
    assert evaluate(a=replace(leg('KALSHI', '.4'), terms_key=()))[1] == 'incompatible_terms'
    assert evaluate(a=replace(leg('KALSHI', '.4'), terms_key=('different-window',)))[1] == 'incompatible_terms'
