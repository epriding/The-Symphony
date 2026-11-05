"""Kalshi public market-data adapter. It has no order-placement methods."""
import asyncio
from dataclasses import replace
import json
import time

import aiohttp

from .base_interfaces import MarketDataClient
from .public_market_data import KalshiNormalizer, market_metadata, kalshi_settlement, amount
from ..kalshi.public_api import KalshiPublicAPI
from ...utilities.classes import OrderBook
from ...utilities.market_events import PublicConnectionEvent, PublicEventEnvelope, ReceiptSequencer


class KalshiMarketData(MarketDataClient):
    exchange = "KALSHI"

    def __init__(self, markets, *, api=None, overrides=None, sequencer=None,
                 ws_url="wss://external-api-ws.kalshi.com/trade-api/ws/v2",
                 max_event_bytes=None, max_markets=None, max_book_levels=None):
        self.markets = list(markets)
        if max_markets is not None and len(self.markets) > max_markets:
            raise ValueError('Public market capacity exceeded')
        self.max_event_bytes = max_event_bytes
        if max_event_bytes is not None:
            from .bounded_public import positive_limit
            positive_limit(max_event_bytes)
        self.api = api or KalshiPublicAPI()
        self.overrides = overrides or {}
        self.sequencer = sequencer or ReceiptSequencer()
        self.ws_url = ws_url
        self.normalizer = KalshiNormalizer(allowed_markets=self.markets if max_event_bytes else None,
            max_book_levels=max_book_levels,
            max_sequences=(len(self.markets) + 4) if max_event_bytes else None)
        self.epoch = 0

    async def get_market(self, ticker):
        if self.max_event_bytes is not None and ticker not in self.markets:
            raise ValueError('Market is outside the fixed public subscriptions')
        response = await self.api.get(f"/markets/{ticker}")
        event_ticker = response["market"].get("event_ticker")
        market = None
        if self.max_event_bytes is not None:
            # Fee updates are routed through this association. A fixed market
            # must not accumulate a new retained event key on each refresh, or
            # silently lose the association needed to freeze changed fee rules.
            if (not isinstance(event_ticker, str) or not event_ticker
                    or any(event != event_ticker for event, members in
                           self.normalizer.event_members.items() if ticker in members)):
                raise ValueError('Kalshi metadata event association is missing or changed')
            market = market_metadata(self.exchange, response["market"], self.overrides.get(ticker))
        if event_ticker:
            self.normalizer.event_members.setdefault(event_ticker, set()).add(ticker)
        if ticker not in self.normalizer.frozen_fees:
            self.normalizer.states[ticker] = response["market"].get("status", "unknown")
        return market if market is not None else market_metadata(
            self.exchange, response["market"], self.overrides.get(ticker))

    async def get_markets(self, active=True, limit=200, offset=None):
        if self.max_event_bytes is not None:
            return [await self.get_market(ticker) for ticker in self.markets]
        params = {"limit": limit}
        if active:
            params["status"] = "open"
        if offset:
            params["cursor"] = offset
        response = await self.api.get("/markets", params)
        return [market_metadata(self.exchange, raw, self.overrides.get(raw["ticker"]))
                for raw in response["markets"]]

    async def get_orderbook(self, market_id, outcome):
        response = await self.api.get(f"/markets/{market_id}/orderbook")
        raw = response["orderbook_fp"]
        bids = {amount(price): amount(quantity) for price, quantity in raw["yes_dollars"]}
        asks = {1 - amount(price): amount(quantity) for price, quantity in raw["no_dollars"]}
        if outcome == "NO":
            bids, asks = (
                {1 - price: quantity for price, quantity in asks.items()},
                {1 - price: quantity for price, quantity in bids.items()},
            )
        elif outcome != "YES":
            raise ValueError("Unknown outcome")
        return OrderBook(market_id, self.exchange, outcome, bids, asks, time.time())

    async def settlements(self):
        for ticker in self.markets:
            raw = (await self.api.get(f"/markets/{ticker}"))["market"]
            result = kalshi_settlement(raw, self.sequencer.receipt())
            if result:
                yield result

    def connection(self, connected, reason):
        receipt = self.sequencer.receipt()
        return PublicEventEnvelope(event=PublicConnectionEvent(exchange=self.exchange,
            timestamp=receipt["received_at"], connected=connected, reason=reason),
            available_at=receipt["received_at"], **receipt, connection_epoch=self.epoch,
            timestamp_quality="receipt_only")

    async def stream(self):
        attempts = 0
        while True:
            self.epoch += 1
            self.normalizer.reset()
            try:
                headers = self.api.websocket_headers()
                async with aiohttp.ClientSession() as client:
                    async with client.ws_connect(self.ws_url, headers=headers, heartbeat=20,
                                                  compress=0,
                                                  max_msg_size=self.max_event_bytes or 8*1024*1024) as websocket:
                        await websocket.send_json({"id": 1, "cmd": "subscribe", "params": {
                            "channels": ["orderbook_delta", "trade"], "market_tickers": self.markets}})
                        await websocket.send_json({"id": 2, "cmd": "subscribe", "params": {
                            "channels": ["market_lifecycle_v2"]}})
                        attempts = 0
                        yield self.connection(True, "awaiting_resnapshot")
                        async for frame in websocket:
                            if frame.type != aiohttp.WSMsgType.TEXT:
                                if self.max_event_bytes and frame.type == aiohttp.WSMsgType.ERROR:
                                    failure = websocket.exception()
                                    if getattr(failure, 'code', None) == 1009:
                                        from .bounded_public import PublicCapacityError
                                        raise PublicCapacityError('Public websocket message byte capacity exceeded')
                                if frame.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                                    break
                                continue
                            receipt = self.sequencer.receipt()
                            if self.max_event_bytes and len(frame.data.encode('utf-8')) > self.max_event_bytes:
                                from .bounded_public import PublicCapacityError
                                raise PublicCapacityError('Public websocket message byte capacity exceeded')
                            message = json.loads(frame.data)
                            # Lifecycle subscriptions are global; maintain their
                            # sequence even when ignoring unrelated market updates.
                            for envelope in self.normalizer.normalize(message, receipt, self.epoch):
                                if getattr(envelope.event, "market_id", "") in self.markets:
                                    yield replace(envelope, available_at=max(time.time(), envelope.received_at))
                yield self.connection(False, "connection_closed")
            except (aiohttp.ClientError, OSError, ValueError, KeyError, ArithmeticError) as error:
                # Do not log raw authenticated payloads/headers or private keys.
                yield self.connection(False, f"public_feed_recovery:{type(error).__name__}")
            attempts += 1
            await asyncio.sleep(min(2**min(attempts, 5), 30))
