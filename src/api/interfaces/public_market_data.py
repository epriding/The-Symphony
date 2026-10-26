"""Pure public-feed normalization; no execution or account dependencies.

Schemas: docs.polymarket.us/api-reference/websocket/markets and
docs.kalshi.com/websockets/{orderbook-updates,public-trades}.
"""
from datetime import datetime
from decimal import Decimal
import math

from ...utilities.classes import (MarketInfo, OrderBookSnapshotEvent,
                                  LiteOrderBookSnapshotEvent, TradeEvent, Side)
from ...utilities.market_events import PublicEventEnvelope, MarketStatusEvent, SettlementEvent


def amount(value):
    if isinstance(value, dict):
        value = value["value"]
    if isinstance(value, bool) or value is None:
        raise ValueError("Missing/invalid numeric amount")
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("Nonfinite public amount")
    return number


def timestamp(value, fallback):
    if value in (None, ""):
        return fallback, "receipt_only"
    if isinstance(value, bool):
        raise ValueError("Invalid timestamp")
    try:
        result = float(value)
    except (ValueError, TypeError):
        result = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    if not math.isfinite(result) or result < 0:
        raise ValueError("Invalid timestamp")
    return result, "venue"


def market_metadata(exchange, raw, overrides=None):
    settings = overrides or {}
    is_us = exchange == "POLYMARKET_US"
    market_id = str(raw["id"] if is_us else raw["ticker"])
    if is_us:
        active = bool(raw.get("active")) and not raw.get("closed")
    else:
        active = raw.get("status") in {"open", "active"}
    ranges = settings.get("price_ranges", raw.get("price_ranges", []))
    tick = settings.get("tick_size", raw.get("tickSize"))
    if tick is None and ranges:
        tick = min(amount(row["step"]) for row in ranges)
    quantity_step = settings.get("quantity_step")
    minimum = settings.get("min_quantity", raw.get("minimumTradeQty", quantity_step))
    if tick is None or quantity_step is None or minimum is None:
        raise ValueError(f"Explicit tick_size/quantity_step/min_quantity required for {exchange}:{market_id}")
    resolution = settings.get(
        "resolution_date",
        raw.get("expected_expiration_time") or raw.get("expiration_time") or raw.get("endDate"),
    )
    resolution = timestamp(resolution, 0)[0] if resolution is not None else None
    market = MarketInfo(
        market_id=market_id,
        exchange=exchange,
        slug=raw.get("slug", market_id),
        token_ids={},
        active=active,
        tradable={"YES": active, "NO": active},
        category=settings.get("category", raw.get("category", "other")),
        resolution_date=resolution,
        tick_size=float(amount(tick)),
        quantity_step=float(amount(quantity_step)),
        min_quantity=float(amount(minimum)),
        min_notional=float(amount(settings.get("min_notional", 0))),
        price_ranges=ranges,
    )
    if raw.get("market_type", "binary") != "binary" or amount(raw.get("notional_value_dollars", "1")) != 1:
        raise ValueError("Only fully funded binary $1 contracts are supported")
    return market


class PolymarketUSNormalizer:
    exchange = "POLYMARKET_US"

    def __init__(self, identities):
        self.identities = identities

    def normalize(self, message, receipt, epoch=0):
        public_type = next((key for key in ("marketData", "marketDataLite", "trade") if key in message), None)
        if public_type is None:
            return []
        payload = message[public_type]
        slug = payload.get("marketSlug")
        market_id = str(payload.get("marketId") or self.identities.get(slug, ""))
        if not market_id:
            raise ValueError("Unknown public market identity; preload subscriptions")
        stamp, quality = timestamp(payload.get("transactTime") or payload.get("tradeTime"), receipt["received_at"])
        source_id = ""
        eligible = True
        if public_type == "marketData":
            event = OrderBookSnapshotEvent(
                market_id=market_id, outcome="YES", timestamp=stamp, exchange=self.exchange,
                bids=self._book_levels(payload["bids"]), asks=self._book_levels(payload["offers"]),
            )
            source_id = f"us-book:{epoch}:{receipt['ingest_index']}"
        elif public_type == "marketDataLite":
            event = LiteOrderBookSnapshotEvent(
                market_id=market_id, outcome="YES", timestamp=stamp, exchange=self.exchange,
                best_bid=float(amount(payload["bestBid"])) if payload.get("bestBid") is not None else None,
                best_ask=float(amount(payload["bestAsk"])) if payload.get("bestAsk") is not None else None,
            )
        else:
            event = self._trade_event(payload, market_id, stamp)
            source_id = payload.get("tradeId", "")
            eligible = event.side is not None and not payload.get("isBlockTrade", False)
        return [PublicEventEnvelope(event=event, available_at=receipt["received_at"],
            **receipt, source_id=source_id, connection_epoch=epoch, timestamp_quality=quality,
            eligible_trade=eligible, market_state=payload.get("state", "OPEN"))]

    @staticmethod
    def _book_levels(rows):
        levels = {}
        for row in rows:
            price = amount(row["px"])
            quantity = amount(row["qty"])
            if price in levels or quantity < 0:
                raise ValueError("Duplicate or negative depth level")
            levels[price] = quantity
        return levels

    def _trade_event(self, payload, market_id, stamp):
        # Public price and aggressor side use the YES instrument, independent
        # of the maker's economic YES/NO intent.
        taker = payload.get("taker", {})
        side = {"ORDER_SIDE_BUY": Side.BUY, "ORDER_SIDE_SELL": Side.SELL}.get(taker.get("side"))
        event = TradeEvent(
            market_id=market_id, outcome="YES", timestamp=stamp, exchange=self.exchange,
            price=amount(payload["price"]), quantity=amount(payload["quantity"]),
            side=side, trade_id=payload.get("tradeId"),
        )
        if event.quantity <= 0 or not 0 < event.price < 1:
            raise ValueError("Invalid public trade amount")
        return event


class KalshiNormalizer:
    exchange = "KALSHI"

    def __init__(self):
        self.books = {}
        self.sequences = {}
        self.states = {}
        self.event_members = {}
        self.frozen_fees = set()

    def reset(self):
        self.books.clear()
        self.sequences.clear()
        for market in self.frozen_fees:
            self.states[market] = "FEE_RULES_CHANGED"

    def normalize(self, message, receipt, epoch=0):
        kind = message.get("type")
        payload = message.get("msg", {})
        subscription_id = message.get("sid")
        sequence = message.get("seq")
        self._check_sequence(subscription_id, sequence)
        if kind == "event_fee_update":
            return self._fee_update(payload, receipt, epoch)
        if kind not in {"orderbook_snapshot", "orderbook_delta", "trade", "market_lifecycle_v2"}:
            if kind == "error":
                raise ValueError("Kalshi public subscription error")
            return []
        market_id = payload.get("market_ticker")
        if not market_id:
            raise ValueError("Missing Kalshi ticker")
        raw_time = payload.get("ts_ms", message.get("sending_ts_ms"))
        if raw_time is not None:
            raw_time = float(raw_time) / 1000
        else:
            raw_time = payload.get("ts")
        stamp, quality = timestamp(raw_time, receipt["received_at"])
        source_id = ""
        eligible = True
        if kind in {"orderbook_snapshot", "orderbook_delta"}:
            self._update_book(kind, payload, market_id)
            levels = self.books[market_id]
            event = OrderBookSnapshotEvent(
                market_id=market_id, outcome="YES", timestamp=stamp, exchange=self.exchange,
                bids=dict(levels["yes"]),
                asks={1 - price: quantity for price, quantity in levels["no"].items()},
            )
            source_id = f"kalshi-book:{epoch}:{subscription_id}:{sequence}"
        elif kind == "trade":
            event = self._trade_event(payload, market_id, stamp)
            source_id = payload.get("trade_id", "")
            eligible = event.side is not None and not payload.get("is_block_trade", False)
        else:
            # Resolution notification alone is not proof cash has settled.
            status = payload.get("status") or payload.get("event_type", "unknown")
            status = {"activated": "OPEN", "deactivated": "HALTED"}.get(status, status)
            terminal_states = {"closed", "settled", "EXPIRED", "MARKET_STATE_EXPIRED", "TERMINATED"}
            if market_id in self.frozen_fees and status not in terminal_states:
                status = "FEE_RULES_CHANGED"
            self.states[market_id] = status
            event = MarketStatusEvent(exchange=self.exchange, market_id=market_id, timestamp=stamp, status=status)
        return [PublicEventEnvelope(event=event, available_at=receipt["received_at"],
            **receipt, source_id=source_id, source_sequence=sequence, connection_epoch=epoch,
            timestamp_quality=quality, eligible_trade=eligible,
            market_state=("FEE_RULES_CHANGED" if market_id in self.frozen_fees
                          else self.states.get(market_id, "OPEN")))]

    def _check_sequence(self, subscription_id, sequence):
        # Global subscriptions must advance even for messages we ignore.
        if sequence is None:
            return
        previous = self.sequences.get(subscription_id)
        if previous is not None and sequence != previous + 1:
            raise ValueError("Kalshi subscription sequence gap; reconnect/resnapshot required")
        self.sequences[subscription_id] = sequence

    def _fee_update(self, payload, receipt, epoch):
        events = []
        market_ids = self.event_members.get(payload.get("event_ticker"), set())
        for market_id in sorted(market_ids):
            self.frozen_fees.add(market_id)
            self.states[market_id] = "FEE_RULES_CHANGED"
            event = MarketStatusEvent(
                exchange=self.exchange, market_id=market_id,
                timestamp=receipt["received_at"], status="FEE_RULES_CHANGED",
            )
            events.append(PublicEventEnvelope(
                event=event, available_at=receipt["received_at"], **receipt,
                connection_epoch=epoch, timestamp_quality="receipt_only",
            ))
        return events

    def _update_book(self, kind, payload, market_id):
        if kind == "orderbook_snapshot":
            self.books[market_id] = {
                "yes": {amount(price): amount(quantity) for price, quantity in payload["yes_dollars_fp"]},
                "no": {amount(price): amount(quantity) for price, quantity in payload["no_dollars_fp"]},
            }
            return
        if market_id not in self.books:
            raise ValueError("Delta without a bootstrap snapshot")
        price = amount(payload["price_dollars"])
        levels = self.books[market_id][payload["side"]]
        quantity = levels.get(price, Decimal(0)) + amount(payload["delta_fp"])
        if quantity < 0:
            raise ValueError("Negative depth after delta")
        if quantity:
            levels[price] = quantity
        else:
            levels.pop(price, None)

    def _trade_event(self, payload, market_id, stamp):
        book_side = payload.get("taker_book_side")
        if book_side is None:
            book_side = {"yes": "bid", "no": "ask"}.get(payload.get("taker_side"))
        side = {"bid": Side.BUY, "ask": Side.SELL}.get(book_side)
        event = TradeEvent(
            market_id=market_id, outcome="YES", timestamp=stamp, exchange=self.exchange,
            price=amount(payload["yes_price_dollars"]), quantity=amount(payload["count_fp"]),
            side=side, trade_id=payload.get("trade_id", "") or None,
        )
        if event.quantity <= 0 or not 0 < event.price < 1:
            raise ValueError("Invalid public trade amount")
        return event


def kalshi_settlement(raw, receipt):
    if raw.get("status") != "settled" or raw.get("is_provisional"):
        return None
    value = raw.get("settlement_value_dollars")
    if value is None and raw.get("settlement_bounds_type", "default") == "default":
        value = {"yes": "1", "no": "0"}.get(raw.get("result"))
    if value is None:
        return None
    payout = amount(value)
    if not 0 <= payout <= 1:
        raise ValueError("Invalid Kalshi payout")
    stamp, quality = timestamp(raw.get("settlement_ts"), receipt["received_at"])
    event = SettlementEvent(exchange="KALSHI", market_id=raw["ticker"], timestamp=stamp,
        settlement_id=f"kalshi:{raw['ticker']}:{raw.get('settlement_ts', stamp)}",
        payouts={"YES": str(payout), "NO": str(1-payout)})
    return PublicEventEnvelope(event=event, available_at=receipt["received_at"], **receipt,
                               timestamp_quality=quality, source_id=event.settlement_id)
