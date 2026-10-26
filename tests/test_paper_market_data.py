"""Adapter contracts and public-only startup, without network requests."""
import asyncio
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest

from src.api.interfaces.public_market_data import (
    PolymarketUSNormalizer, KalshiNormalizer, market_metadata, kalshi_settlement,
)
from src.api.interfaces.polymarket_us_public import PolymarketUSMarketData
from src.api.interfaces.kalshi import KalshiMarketData
from src.paper_trading.clock import RealClock
from src.paper_trading.live import run_live
from src.paper_trading.models import PaperExecutionConfig
from src.paper_trading.session import PaperSession
from src.paper_trading.venue_rules import fixture_venue
from src.utilities.classes import MarketInfo, Side, TradingConfig
from src.utilities.market_events import PublicEventEnvelope, PublicConnectionEvent

PAYLOADS = json.loads((Path(__file__).parent/'fixtures/paper_trading/public_payloads.json').read_text())


def receipt(index=1):
    return dict(received_at=1800000000+index*.01, received_monotonic_ns=index,
                ingest_index=index)


def test_us_book_and_trade_have_one_canonical_yes_denomination():
    normalizer = PolymarketUSNormalizer({'demo-us': 'us-id'})
    book = normalizer.normalize(PAYLOADS['us_book'], receipt())[0]
    assert book.event.outcome == 'YES'
    assert book.event.bids == {Decimal('.49'): Decimal(100)}
    assert book.event.asks == {Decimal('.50'): Decimal(40)}
    trade = normalizer.normalize(PAYLOADS['us_trade'], receipt(2))[0]
    assert trade.event.outcome == 'YES' and trade.event.side == Side.SELL
    assert trade.event.price == Decimal('.49') and trade.event.quantity == 105
    assert trade.source_id == 'synthetic-us-trade'
    missing = deepcopy(PAYLOADS['us_book'])
    missing['marketData'].pop('transactTime')
    assert normalizer.normalize(missing, receipt())[0].timestamp_quality == 'receipt_only'


def test_kalshi_delta_complements_and_trade_aggressor():
    normalizer = KalshiNormalizer()
    one = normalizer.normalize(PAYLOADS['kalshi_book'], receipt())[0]
    two = normalizer.normalize(PAYLOADS['kalshi_delta'], receipt(2))[0]
    assert one.event.asks == {Decimal('.5'): Decimal(40)}
    assert two.event.asks == {Decimal('.5'): Decimal(40), Decimal('.51'): Decimal(20)}
    trade = normalizer.normalize(PAYLOADS['kalshi_trade'], receipt(3))[0]
    assert trade.event.outcome == 'YES' and trade.event.side == Side.SELL
    blocked = deepcopy(PAYLOADS['kalshi_trade'])
    blocked['seq'] = 2
    blocked['msg']['is_block_trade'] = True
    assert not normalizer.normalize(blocked, receipt(4))[0].eligible_trade
    gap = deepcopy(PAYLOADS['kalshi_delta'])
    gap['seq'] = 7
    with pytest.raises(ValueError, match='sequence gap'):
        normalizer.normalize(gap, receipt(5))
    normalizer.reset()
    with pytest.raises(ValueError, match='bootstrap'):
        normalizer.normalize(PAYLOADS['kalshi_delta'], receipt(6))


def test_kalshi_sequence_includes_other_global_lifecycle_messages():
    normalizer = KalshiNormalizer()
    normalizer.normalize({'type': 'event_fee_update', 'sid': 30, 'seq': 3, 'msg': {}}, receipt())
    result = normalizer.normalize({'type': 'market_lifecycle_v2', 'sid': 30, 'seq': 4,
        'msg': {'market_ticker': 'DEMO-K', 'event_type': 'closed'}}, receipt(2))
    assert result[0].event.status == 'closed'


def test_metadata_requires_explicit_rules_and_supports_fractional_ladders():
    raw = {'ticker': 'DEMO-K', 'status': 'active', 'price_ranges': [
        {'start': '0', 'end': '1', 'step': '.0001'}]}
    with pytest.raises(ValueError, match='Explicit'):
        market_metadata('KALSHI', raw)
    result = market_metadata('KALSHI', raw, {'quantity_step': '.01', 'min_quantity': '.01'})
    assert result.quantity_step == .01 and result.tick_size == .0001
    assert result.price_ranges == raw['price_ranges']


def test_kalshi_settlement_needs_final_payout():
    raw = {'ticker': 'DEMO-K', 'status': 'closed', 'result': 'yes'}
    assert kalshi_settlement(raw, receipt()) is None
    raw.update(status='settled', settlement_value_dollars='.5')
    result = kalshi_settlement(raw, receipt())
    assert result.event.payouts == {'YES': '0.5', 'NO': '0.5'}
    raw['is_provisional'] = True
    assert kalshi_settlement(raw, receipt()) is None


@pytest.mark.asyncio
async def test_us_stream_never_opens_private_account_channel(monkeypatch):
    handlers = {}
    class Socket:
        def on(self, name, callback):
            handlers[name] = callback
        async def connect(self):
            pass
        async def subscribe(self, request_id, subscription, slugs):
            if subscription == 'SUBSCRIPTION_TYPE_MARKET_DATA':
                handlers['market_data'](PAYLOADS['us_book'])
            else:
                handlers['trade'](PAYLOADS['us_trade'])
        async def close(self):
            pass
    class SDK:
        def __init__(self, **kwargs):
            self.ws = SimpleNamespace(markets=lambda: Socket())
    monkeypatch.setattr('src.api.interfaces.polymarket_us_public.PolymarketUS', SDK)
    feed = PolymarketUSMarketData(['demo-us'])
    await feed.api.client.close()
    feed.api = SimpleNamespace(client=SimpleNamespace(close=AsyncMock()))
    feed.identities['demo-us'] = 'us-id'
    stream = feed.stream()
    try:
        assert (await anext(stream)).event.connected
        book = await anext(stream)
        trade = await anext(stream)
        assert book.ingest_index < trade.ingest_index
        assert book.event.market_id == 'us-id' and trade.event.outcome == 'YES'
    finally:
        await stream.aclose()
    feed.api.client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_kalshi_read_only_rest_book_preserves_contract_units():
    api = SimpleNamespace(get=AsyncMock(return_value={'orderbook_fp': {
        'yes_dollars': [['.49', '2.50']], 'no_dollars': [['.50', '3.75']]}}))
    feed = KalshiMarketData(['DEMO-K'], api=api)
    book = await feed.get_orderbook('DEMO-K', 'NO')
    assert book.asks == {Decimal('.51'): Decimal('2.50')}
    assert book.bids == {Decimal('.50'): Decimal('3.75')}
    assert not hasattr(feed, 'place_order') and not hasattr(feed, 'get_balance')


@pytest.mark.asyncio
async def test_live_collector_overflow_invalidates_execution_and_finalizes(tmp_path):
    config = TradingConfig()
    config.paper_starting_balances = {'PAPER': {'A': 1000}}
    config.paper_execution = PaperExecutionConfig({'A': fixture_venue('A')}, max_pending_events=2)
    session = PaperSession(config, RealClock(), tmp_path/'capture')
    class Feed:
        exchange, markets = 'A', ['m']
        async def get_market(self, identifier):
            return MarketInfo('m', 'A', 'm', {}, True, {'YES': True, 'NO': True},
                              tick_size=.01, quantity_step=1, min_quantity=1)
        async def stream(self):
            for _ in range(50):
                received = self.sequencer.receipt()
                yield PublicEventEnvelope(event=PublicConnectionEvent(exchange='A',
                    timestamp=received['received_at'], connected=False),
                    available_at=received['received_at'], **received)
        async def settlements(self):
            if False:
                yield None
    with pytest.raises(BufferError, match='overflow'):
        await run_live(session, [Feed()], duration=.1, output_directory=tmp_path/'output')
    assert session.closed
    assert json.loads((tmp_path/'capture/manifest.json').read_text())['finalized']
    assert not session.engine.executions


def test_fee_override_change_blocks_execution_across_reconnect():
    normalizer = KalshiNormalizer()
    normalizer.event_members['EVENT'] = {'DEMO-K'}
    normalizer.states['OTHER-K'] = 'HALTED'
    normalizer.books['OTHER-K'] = {'yes': {}, 'no': {}}
    normalizer.sequences[30] = 4
    result = normalizer.normalize({'type':'event_fee_update', 'sid':30, 'seq':5,
        'msg':{'event_ticker':'EVENT', 'fee_multiplier_override':2}}, receipt())
    assert result[0].event.status == 'FEE_RULES_CHANGED'
    states = normalizer.states
    normalizer.reset()
    assert normalizer.states is states
    assert normalizer.states == {'OTHER-K': 'HALTED', 'DEMO-K': 'FEE_RULES_CHANGED'}
    assert not normalizer.books and not normalizer.sequences
    book = normalizer.normalize(PAYLOADS['kalshi_book'], receipt(2))[0]
    assert book.market_state == 'FEE_RULES_CHANGED'


@pytest.mark.asyncio
async def test_fee_freeze_survives_lifecycle_and_metadata_refresh():
    api = SimpleNamespace(get=AsyncMock(return_value={'market': {
        'ticker':'DEMO-K', 'event_ticker':'EVENT', 'status':'active', 'tickSize':'.01'}}))
    feed = KalshiMarketData(['DEMO-K'], api=api,
        overrides={'DEMO-K': {'quantity_step':1, 'min_quantity':1}})
    await feed.get_market('DEMO-K')
    normalizer = feed.normalizer
    normalizer.normalize({'type':'event_fee_update', 'sid':30, 'seq':1,
        'msg':{'event_ticker':'EVENT'}}, receipt())
    for sequence, event_type in enumerate(('deactivated', 'activated'), start=2):
        event = normalizer.normalize({'type':'market_lifecycle_v2', 'sid':30, 'seq':sequence,
            'msg':{'market_ticker':'DEMO-K', 'event_type':event_type}}, receipt(sequence))[0]
        assert event.event.status == 'FEE_RULES_CHANGED'
    await feed.get_market('DEMO-K')
    assert normalizer.states['DEMO-K'] == 'FEE_RULES_CHANGED'
    assert normalizer.normalize(PAYLOADS['kalshi_book'], receipt(4))[0].market_state == 'FEE_RULES_CHANGED'

    closed = normalizer.normalize({'type':'market_lifecycle_v2', 'sid':30, 'seq':4,
        'msg':{'market_ticker':'DEMO-K', 'event_type':'closed'}}, receipt(5))[0]
    assert closed.event.status == 'closed'  # Closure must still expire working orders.
    await feed.get_market('DEMO-K')
    assert normalizer.states['DEMO-K'] == 'closed'
    assert 'DEMO-K' in normalizer.frozen_fees


@pytest.mark.asyncio
async def test_a26_streaming_replay_uses_bounded_event_and_audit_buffers(tmp_path):
    from examples.paper_trading_benchmark import benchmark
    result = await benchmark(tmp_path/'workload', events=300)
    assert result['audit_window_events'] <= 128
    assert result['peak_pending_events'] <= 128
    assert result['events_per_second'] > 0
    assert result['peak_traced_bytes'] > 0
    assert result['processing_wall_time']['sample_count'] <= 128
    assert result['processing_wall_time']['p99_seconds'] >= result['processing_wall_time']['p50_seconds']
    assert len((tmp_path/'workload/replay/audit.jsonl').read_text().splitlines()) >= 300


@pytest.mark.asyncio
async def test_live_bootstrap_failure_finalizes_capture_without_strategy_calls(tmp_path):
    config = TradingConfig()
    config.paper_execution = PaperExecutionConfig({'A': fixture_venue('A')})
    session = PaperSession(config, RealClock(), tmp_path/'capture')
    strategy = SimpleNamespace(on_snapshot=AsyncMock())
    session.strategy = strategy
    feed = SimpleNamespace(exchange='A', markets=['m'],
                           get_market=AsyncMock(side_effect=ValueError('missing market rules')))
    with pytest.raises(ValueError, match='missing market rules'):
        await run_live(session, [feed], duration=.1, output_directory=tmp_path/'output')
    assert session.closed and not session.engine.executions
    assert json.loads((tmp_path/'capture/manifest.json').read_text())['finalized']
    strategy.on_snapshot.assert_not_awaited()


@pytest.mark.parametrize('kind', ['rsa', 'ed25519'])
def test_kalshi_public_websocket_authentication_signature(tmp_path, kind):
    import base64
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa, padding, ed25519
    from src.api.kalshi.public_api import KalshiPublicAPI

    key = (rsa.generate_private_key(public_exponent=65537, key_size=2048)
           if kind == 'rsa' else ed25519.Ed25519PrivateKey.generate())
    path = tmp_path/'test-key.pem'
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                      serialization.NoEncryption()))
    headers = KalshiPublicAPI('synthetic-key', path).websocket_headers()
    message = (headers['KALSHI-ACCESS-TIMESTAMP']+'GET/trade-api/ws/v2').encode()
    signature = base64.b64decode(headers['KALSHI-ACCESS-SIGNATURE'])
    public = key.public_key()
    if kind == 'rsa':
        public.verify(signature, message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                      salt_length=hashes.SHA256().digest_size), hashes.SHA256())
    else:
        public.verify(signature, message)
    assert headers['KALSHI-ACCESS-KEY'] == 'synthetic-key'
