"""Synthetic demonstration, never a public-market trading algorithm."""
import asyncio
from ..paper_trading.clock import VirtualClock
from ..utilities.classes import MarketInfo, OrderBookSnapshotEvent, Side
from ..utilities.market_events import PublicEventEnvelope

START = 1800000000.0
VENUES = ('POLYMARKET_US', 'KALSHI')


class SmokeStrategy:
    def __init__(self, context):
        self.context, self.sent = context, False

    async def on_snapshot(self, snapshot):
        if self.sent:
            return None
        self.sent = True
        intents = []
        for venue in VENUES:
            common = dict(market_id='synthetic', exchange=venue, outcome='YES')
            intents.append(self.context.new_intent(**common, side=Side.BUY, price=.5,
                estimated_probability=1, resolution_date=self.context.now() + 3600))
        return intents


class _SyntheticFeed:
    def __init__(self, exchange, clock):
        self.exchange, self.clock, self.markets = exchange, clock, ['synthetic']

    async def get_market(self, identifier):
        return MarketInfo(identifier, self.exchange, 'Synthetic smoke fixture', {}, True,
            {'YES': True, 'NO': True}, tick_size=.01, quantity_step=1, min_quantity=1,
            fee_per_contract=0, resolution_date=START + 3600)

    async def stream(self):
        for index in range(12):
            await asyncio.sleep(.003)
            # This fixture represents repeated observations of one unchanged
            # book. Keep its time and revision fixed: worker/OS scheduling must
            # not select a different arrival book or execution timestamp.
            timestamp = self.clock()
            event = OrderBookSnapshotEvent('synthetic', 'YES', timestamp, self.exchange,
                                           {.48: 1000}, {.5: 1000})
            receipt = self.sequencer.receipt()
            receipt['received_at'], receipt['received_monotonic_ns'] = timestamp, index
            yield PublicEventEnvelope(event=event, available_at=timestamp, **receipt,
                source_id=f'{self.exchange}-smoke-static')

    async def settlements(self):
        if False:
            yield None


def smoke_fixture():
    """Finite uncalibrated numbers for an entirely offline journey."""
    from ..paper_trading.runtime_config import COMPATIBILITY_LIMITS
    venues = {venue: dict(
        fees=dict(version='synthetic-zero-fee', model='flat', taker='0', maker='0',
                  balance_precision='.01', effective_from=0), max_book_age=5,
        outbound={'seconds': 0}, acknowledgement={'seconds': 0},
        execution_report={'seconds': 0}, cancel={'seconds': 0},
        request_rate=1000, request_capacity=1000, rate_policy='reject', netted_outcomes=True)
        for venue in VENUES}
    config = dict(strategy_portfolios={'smoke': 'PAPER'},
        starting_balances={'PAPER': {venue: 1000 for venue in VENUES}},
        risk={'PAPER': dict(diversification={'other': 1}, max_position_exposure=.5,
            max_total_exposure=10, stop_loss_pct=100, max_quote_age_seconds=5)},
        execution=dict(seed=7, session_id='synthetic-smoke-v1', assumptions=
            'Synthetic offline smoke demonstration; limits and execution are unmeasured, not calibrated',
            processing_delay={'seconds': 0}, max_pending_events=1024, max_outbox=128,
            markout_horizons=[.01, .05], venues=venues),
        feeds={venue: dict(markets=['synthetic'], read_interval=.01,
            market_rules={'synthetic': dict(tick_size='.01', quantity_step='1', min_quantity='1')})
            for venue in VENUES},
        strategies=[dict(name='smoke', factory='smoke-v1', enabled=True,
            required_markets=[f'{venue}:synthetic' for venue in VENUES], settings={})],
        runtime=dict(source='synthetic', observe_only=False, startup_timeout_seconds=1,
            strategy_timeout_seconds=.5, risk_interval_seconds=.02, shutdown_grace_seconds=1,
            limits=dict(COMPATIBILITY_LIMITS)))
    clock = VirtualClock(START)
    return config, [_SyntheticFeed(venue, clock) for venue in VENUES], clock
