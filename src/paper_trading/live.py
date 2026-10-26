"""Bounded public-feed collection and paper execution. No live order client."""
import asyncio
import time

from ..utilities.market_events import ReceiptSequencer, PublicConnectionEvent, PublicEventEnvelope


async def run_live(session, feeds, *, duration, output_directory=None, metadata_interval=30):
    if duration <= 0 or metadata_interval <= 0:
        raise ValueError("Positive capture duration and metadata interval required")
    queue = asyncio.Queue(maxsize=session.engine.config.max_pending_events)
    sequencer = ReceiptSequencer()
    def put(kind, value):
        try:
            queue.put_nowait((kind, value))
        except asyncio.QueueFull as error:
            raise BufferError("public_collector_overflow; execution data invalidated") from error

    async def read_feed(feed):
        async for envelope in feed.stream():
            put("public", envelope)

    async def refresh(feed):
        while True:
            await asyncio.sleep(metadata_interval)
            for identifier in feed.markets:
                put("market", await feed.get_market(identifier))
            async for envelope in feed.settlements():
                put("public", envelope)

    tasks = []
    try:
        for feed in feeds:
            feed.sequencer = sequencer
            for identifier in feed.markets:
                await session.add_market(await feed.get_market(identifier))
        tasks = [asyncio.create_task(callback(feed)) for feed in feeds for callback in (read_feed, refresh)]
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            for task in tasks:
                if task.done():
                    if task.exception():
                        raise task.exception()
                    raise RuntimeError("Public data producer stopped unexpectedly")
            next_timer = session.engine.scheduler.next_time
            timeout = min(.05, deadline-time.monotonic())
            if next_timer is not None:
                timeout = min(timeout, max(.001, next_timer-session.clock()))
            try:
                kind, payload = await asyncio.wait_for(queue.get(), timeout=max(.001, timeout))
                for task in tasks:
                    if task.done() and task.exception():
                        raise task.exception()
                if kind == "public":
                    session.publish(payload)
                else:
                    await session.add_market(payload)
                queue.task_done()
            except asyncio.TimeoutError:
                pass
            await session.advance(session.clock())
    except BaseException:
        # Fail closed before final report delivery. No queued input may be used
        # to manufacture matching after an overflow or failed collector.
        for feed in feeds:
            receipt = sequencer.receipt()
            event = PublicConnectionEvent(exchange=feed.exchange, timestamp=receipt["received_at"],
                                           connected=False, reason="collector_stopped")
            envelope = PublicEventEnvelope(event=event, available_at=receipt["received_at"], **receipt)
            await session.apply_public_immediately(envelope)
        raise
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await session.finish(output_directory)
    return session.reporter.summary(session.state)
