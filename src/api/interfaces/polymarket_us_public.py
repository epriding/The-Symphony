"""US public-feed composition, independent of live account reconciliation."""
import asyncio
from dataclasses import replace
import time

from polymarket_us import PolymarketUS
from websockets.exceptions import WebSocketException

from .base_interfaces import MarketDataClient
from .public_market_data import PolymarketUSNormalizer, market_metadata, amount
from ..polymarket.polyus_rest_api import PolymarketAPI
from ...utilities.classes import OrderBook
from ...utilities.market_events import (PublicEventEnvelope, PublicConnectionEvent,
                                        ReceiptSequencer, SettlementEvent)


class PolymarketUSMarketData(MarketDataClient):
    exchange = "POLYMARKET_US"

    def __init__(self, markets, key_id=None, secret_key=None, *, overrides=None,
                 sequencer=None, queue_limit=10000, read_interval=1.0):
        if queue_limit < 1 or read_interval <= 0:
            raise ValueError("Positive public queue/read limits required")
        self.markets = list(markets)
        self.key_id = key_id
        self.secret_key = secret_key
        self.api = PolymarketAPI(key_id, secret_key)
        self.overrides = overrides or {}
        self.identities = {}
        self.sequencer = sequencer or ReceiptSequencer()
        self.normalizer = PolymarketUSNormalizer(self.identities)
        self.queue_limit = queue_limit
        self.read_interval = read_interval
        self.read_lock = asyncio.Lock()
        self.last_read = 0.0
        self.epoch = 0

    async def _read(self, method, **kwargs):
        async with self.read_lock:
            delay = self.read_interval - (time.monotonic() - self.last_read)
            if delay > 0:
                await asyncio.sleep(delay)
            self.last_read = time.monotonic()
        return await method(**kwargs)

    async def get_market(self, slug):
        raw = await self._read(self.api.get_market_by_slug, slug=slug)
        raw = raw.get("market", raw)
        market = market_metadata(self.exchange, raw, self.overrides.get(slug))
        self.identities[slug] = market.market_id
        return market

    async def get_markets(self, active=True, limit=200, offset=None):
        response = await self._read(self.api.get_markets, active=active, limit=limit, offset=offset or 0)
        result = []
        for raw in response["markets"]:
            market = market_metadata(self.exchange, raw, self.overrides.get(raw["slug"]))
            self.identities[market.slug] = market.market_id
            result.append(market)
        return result

    async def get_orderbook(self, market_id, outcome):
        slug = next(slug for slug, identity in self.identities.items() if identity == market_id)
        raw = await self._read(self.api.get_orderbook, slug=slug, outcome="YES")
        bids = {amount(row["px"]): amount(row["qty"]) for row in raw["bids"]}
        asks = {amount(row["px"]): amount(row["qty"]) for row in raw["offers"]}
        if outcome == "NO":
            bids, asks = (
                {1 - price: quantity for price, quantity in asks.items()},
                {1 - price: quantity for price, quantity in bids.items()},
            )
        elif outcome != "YES":
            raise ValueError("Unknown outcome")
        return OrderBook(market_id, self.exchange, outcome, bids, asks, time.time())

    async def settlements(self):
        for slug in self.markets:
            raw = await self._read(self.api.get_market_by_slug, slug=slug)
            raw = raw.get("market", raw)
            if not raw.get("closed"):
                continue
            response = await self._read(self.api.get_settlement, slug=slug)
            if response.get("settlement") is None:
                continue
            payout = amount(response["settlement"])
            if not 0 <= payout <= 1:
                raise ValueError("US settlement requires a verified USD payout in [0,1]")
            receipt = self.sequencer.receipt()
            event = SettlementEvent(exchange=self.exchange, market_id=self.identities[slug],
                timestamp=receipt["received_at"], settlement_id=f"us:{slug}:{payout}",
                payouts={"YES": str(payout), "NO": str(1-payout)})
            yield PublicEventEnvelope(event=event, available_at=receipt["received_at"], **receipt,
                                      source_id=event.settlement_id, timestamp_quality="receipt_only")

    def connection(self, connected, reason):
        receipt = self.sequencer.receipt()
        return PublicEventEnvelope(event=PublicConnectionEvent(exchange=self.exchange,
            timestamp=receipt["received_at"], connected=connected, reason=reason),
            available_at=receipt["received_at"], **receipt, connection_epoch=self.epoch,
            timestamp_quality="receipt_only")

    async def stream(self):
        for slug in self.markets:
            if slug not in self.identities:
                await self.get_market(slug)
        attempts = 0
        sdk = PolymarketUS(key_id=self.key_id, secret_key=self.secret_key)
        try:
            while True:
                self.epoch += 1
                raw_queue = asyncio.Queue(maxsize=self.queue_limit)
                recovery = asyncio.Event()
                websocket = sdk.ws.markets()
                def callback(message):
                    receipt = self.sequencer.receipt()
                    try:
                        raw_queue.put_nowait((message, receipt))
                    except asyncio.QueueFull:
                        recovery.set()
                event_fields = (
                    ("market_data", "marketData"),
                    ("market_data_lite", "marketDataLite"),
                    ("trade", "trade"),
                )
                for name, field in event_fields:
                    # Bind each payload field now, before the loop advances.
                    def on_data(message, field=field):
                        callback(message if field in message else {field: message})
                    websocket.on(name, on_data)
                websocket.on("close", lambda *args: recovery.set())
                websocket.on("error", lambda *args: recovery.set())
                try:
                    await websocket.connect()
                    for slug in self.markets:
                        for subscription in ("SUBSCRIPTION_TYPE_MARKET_DATA", "SUBSCRIPTION_TYPE_TRADE"):
                            await websocket.subscribe(f"{slug}:{subscription}", subscription, [slug])
                    attempts = 0
                    yield self.connection(True, "awaiting_resnapshot")
                    while not recovery.is_set():
                        try:
                            message, receipt = await asyncio.wait_for(raw_queue.get(), timeout=.25)
                        except asyncio.TimeoutError:
                            continue
                        for envelope in self.normalizer.normalize(message, receipt, self.epoch):
                            yield replace(envelope, available_at=max(time.time(), envelope.received_at))
                    yield self.connection(False, "public_queue_overflow_or_connection_loss")
                except (OSError, ValueError, KeyError, ArithmeticError, asyncio.TimeoutError, WebSocketException) as error:
                    yield self.connection(False, f"public_feed_recovery:{type(error).__name__}")
                finally:
                    await websocket.close()
                attempts += 1
                await asyncio.sleep(min(2**min(attempts, 5), 30))
        finally:
            await self.api.client.close()
