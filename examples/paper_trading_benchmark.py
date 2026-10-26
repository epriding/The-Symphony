"""Synthetic two-venue market-data replay workload; no live/HFT accuracy claim."""
import argparse
import asyncio
import json
from pathlib import Path
import time
import tracemalloc

from src.paper_trading.clock import VirtualClock
from src.paper_trading.models import PaperExecutionConfig
from src.paper_trading.replay import replay
from src.paper_trading.session import PaperSession
from src.paper_trading.venue_rules import fixture_venue
from src.utilities.classes import MarketInfo, OrderBookSnapshotEvent, TradingConfig
from src.utilities.market_events import PublicEventEnvelope


async def benchmark(directory, events=2000):
    root = Path(directory)
    config = TradingConfig()
    venues = ('POLYMARKET_US', 'KALSHI')
    config.paper_starting_balances = {'PAPER': {venue:1000 for venue in venues}}
    config.paper_execution = PaperExecutionConfig({venue:fixture_venue(venue) for venue in venues},
        max_pending_events=128, assumptions='Synthetic book-only streaming workload; no orders or network')
    start = 1_800_000_000.0
    session = PaperSession(config, VirtualClock(start), root/'capture')
    for venue in venues:
        await session.add_market(MarketInfo('benchmark', venue, 'benchmark', {}, True,
            {'YES': True, 'NO': True}, tick_size=.01, quantity_step=1, min_quantity=1))
    for index in range(events):
        at = start + index*.001
        venue = venues[index%2]
        event = OrderBookSnapshotEvent('benchmark', 'YES', at, venue, {.49:100+index%19}, {.51:100+index%23})
        session.publish(PublicEventEnvelope(event=event, received_at=at, available_at=at,
            ingest_index=index+1, source_id=f'benchmark-{index}'))
        await session.advance(at)
    await session.finish(root/'capture')
    tracemalloc.start()
    began = time.perf_counter()
    restored, summary = await replay(root/'capture', root/'replay')
    elapsed = time.perf_counter()-began
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    result = dict(workload='synthetic two-venue full-book events; no orders', events=events,
        elapsed_seconds=elapsed, events_per_second=events/elapsed,
        peak_traced_bytes=peak, audit_window_events=len(restored.engine.audit),
        peak_pending_events=summary['peak_pending_events'],
        processing_wall_time=summary['processing_wall_time'])
    (root/'benchmark.json').write_text(json.dumps(result, indent=2)+'\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--events', type=int, default=2000)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(benchmark(args.output, args.events)), indent=2))
