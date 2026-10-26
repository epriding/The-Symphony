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
                 ws_url="wss://external-api-ws.kalshi.com/trade-api/ws/v2"):
        self.markets = list(markets)
        self.api = api or KalshiPublicAPI()
        self.overrides = overrides or {}
        self.sequencer = sequencer or ReceiptSequencer()
        self.ws_url = ws_url
        self.normalizer = KalshiNormalizer()
        self.epoch = 0

    async def get_market(self, ticker):
        response = await self.api.get(f"/markets/{ticker}")
        event_ticker = response["market"].get("event_ticker")
        if event_ticker:
            self.normalizer.event_members.setdefault(event_ticker, set()).add(ticker)
        if ticker not in self.normalizer.frozen_fees:
            self.normalizer.states[ticker] = response["market"].get("status", "unknown")
        return market_metadata(self.exchange, response["market"], self.overrides.get(ticker))

    async def get_markets(self, active=True, limit=200, offset=None):
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
                                                  max_msg_size=8*1024*1024) as websocket:
                        await websocket.send_json({"id": 1, "cmd": "subscribe", "params": {
                            "channels": ["orderbook_delta", "trade"], "market_tickers": self.markets}})
                        await websocket.send_json({"id": 2, "cmd": "subscribe", "params": {
                            "channels": ["market_lifecycle_v2"]}})
                        attempts = 0
                        yield self.connection(True, "awaiting_resnapshot")
                        async for frame in websocket:
                            if frame.type != aiohttp.WSMsgType.TEXT:
                                if frame.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}:
                                    break
                                continue
                            receipt = self.sequencer.receipt()
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
