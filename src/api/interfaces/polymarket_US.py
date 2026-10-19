from decimal import Decimal
from typing import Optional, Tuple
from .base_interfaces import WebsocketClient, RestClient
from ..polymarket.polyUS_api import PolymarketAPI
from ..polymarket.polyus_websocket import PolyUSWebsocket
from typing import List, Dict
import asyncio
from datetime import datetime
from ...utilities.classes import (
    OrderIntent, OrderRecord, Position, MarketInfo, OrderBookSnapshotEvent,
    LiteOrderBookSnapshotEvent, TradeEvent, OrderSnapshotEvent,
    OrderUpdateEvent, PositionSnapshotEvent, PositionUpdateEvent, BalanceSnapshotEvent,
    BalanceUpdateEvent, RfqEvent, OrderBook, Side, OrderStatus, TimeInForce,
)
from ...utilities.logger import setup_logger
import time
import math


logger = setup_logger(__name__)


def _order_field(order, name, default=None):
    """Read fields from the SDK's dict-like and model-like order responses."""
    if isinstance(order, dict):
        return order.get(name, default)
    return getattr(order, name, default)


def _numeric_amount(value):
    """Parse a number or API Amount object into a finite float."""
    if isinstance(value, dict):
        if "value" not in value:
            raise ValueError("amount object is missing 'value'")
        value = value["value"]
    elif hasattr(value, "value"):
        value = value.value
    if isinstance(value, bool) or value is None or value == "":
        raise ValueError("amount must be a number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid amount {value!r}") from exc
    if not math.isfinite(number):
        raise ValueError("amount must be finite")
    return number


def _order_size_and_filled(order):
    """Return original order quantity and cumulative fills when available."""
    quantity = _order_field(order, "quantity")
    cumulative = _order_field(order, "cumQuantity")
    leaves = _order_field(order, "leavesQuantity")

    if quantity is not None:
        size = _numeric_amount(quantity)
    elif cumulative is not None and leaves is not None:
        # The API defines cumQuantity as filled and leavesQuantity as still open.
        size = _numeric_amount(cumulative) + _numeric_amount(leaves)
    else:
        raise ValueError("order has no quantity fields")

    state = _order_field(order, "state")
    terminal_without_fill = {
        "ORDER_STATE_CANCELED", "ORDER_STATE_REJECTED", "ORDER_STATE_EXPIRED",
        "ORDER_STATE_REPLACED", "CANCELED", "REJECTED", "EXPIRED",
    }
    if cumulative is not None:
        filled = _numeric_amount(cumulative)
    elif state in {"ORDER_STATE_FILLED", "FILLED"}:
        filled = size
    elif leaves is not None and state not in terminal_without_fill:
        filled = max(0.0, size - _numeric_amount(leaves))
    else:
        filled = None
    return size, filled


class PolyUsClient(RestClient):
    def __init__(
        self,
        key_id: str,
        secret_key: str,
        market_id_by_slug: Optional[Dict[str, str]] = None,
    ):
        self.polymarket_api: PolymarketAPI = PolymarketAPI(key_id=key_id, secret_key=secret_key)
        self._slug_cache: Dict[str, str] = {}
        self._market_id_by_slug = market_id_by_slug if market_id_by_slug is not None else {}
        self._INTENT_MAP = {
            "ORDER_INTENT_BUY_LONG":   ("YES", Side.BUY),
            "ORDER_INTENT_SELL_LONG":  ("YES", Side.SELL),
            "ORDER_INTENT_BUY_SHORT":  ("NO",  Side.BUY),
            "ORDER_INTENT_SELL_SHORT": ("NO",  Side.SELL),
        }
        self._REVERSE_INTENT_MAP = {
            ("YES", Side.BUY):  "ORDER_INTENT_BUY_LONG",
            ("YES", Side.SELL): "ORDER_INTENT_SELL_LONG",
            ("NO",  Side.BUY):  "ORDER_INTENT_BUY_SHORT",
            ("NO",  Side.SELL): "ORDER_INTENT_SELL_SHORT",
        }
        self._STATUS_MAP = {
            "ORDER_STATE_PENDING_NEW": OrderStatus.NEW,
            "ORDER_STATE_PENDING_REPLACE": OrderStatus.NEW,
            "ORDER_STATE_PENDING_CANCEL": OrderStatus.NEW,
            "ORDER_STATE_PENDING_RISK": OrderStatus.NEW,
            "ORDER_STATE_NEW": OrderStatus.NEW,
            "ORDER_STATE_PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "ORDER_STATE_FILLED": OrderStatus.FILLED,
            "ORDER_STATE_CANCELED": OrderStatus.CANCELED,
            "ORDER_STATE_REJECTED": OrderStatus.REJECTED,
            "ORDER_STATE_EXPIRED": OrderStatus.EXPIRED,
        }
        self._TIF_MAP = {
            "TIME_IN_FORCE_GOOD_TILL_CANCEL": TimeInForce.GTC,
            "TIME_IN_FORCE_GOOD_TILL_DATE": TimeInForce.GTD,
            "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL": TimeInForce.IOC,
            "TIME_IN_FORCE_FILL_OR_KILL": TimeInForce.FOK,
        }

    async def get_markets(self, active: bool = True, limit: int = 100, offset: int = 0) -> List[MarketInfo]:
        response = await self.polymarket_api.get_markets(active=active, limit=limit, offset=offset)

        market_details = response['markets']

        markets = []

        for market in market_details:
            self._market_id_by_slug[market.slug] = market.id
            self._slug_cache[market.id] = market.slug
            yes_side, no_side = market.marketSides
            market_info = MarketInfo(
                market_id=market.id,
                exchange="POLYMARKET_US",
                slug=market.slug,
                token_ids={
                    yes_side.description.upper(): yes_side.id,
                    no_side.description.upper(): no_side.id
                },
                active=market.active,
                tradable={
                    yes_side.description.upper(): yes_side.tradable,
                    no_side.description.upper(): no_side.tradable
                },
                volume=market.volume,
                prices={
                    yes_side.description.upper(): yes_side.price,
                    no_side.description.upper(): no_side.price
                },
            )
            markets.append(market_info)

        return markets

    async def _get_slug(self, market_id: str) -> str:
        slug = self._slug_cache.get(market_id, None)

        if slug is not None:
            return slug

        slug = next(
            (slug for slug, cached_market_id in self._market_id_by_slug.items()
             if cached_market_id == market_id),
            None,
        )
        if slug is not None:
            self._slug_cache[market_id] = slug
            return slug

        market = await self.polymarket_api.get_market(market_id=market_id)
        self._slug_cache[market_id] = market.slug
        self._market_id_by_slug[market.slug] = market.id

        return market.slug

    async def _get_market_id(self, slug: str) -> str:
        market_id = self._market_id_by_slug.get(slug)
        if market_id is not None:
            return market_id

        market = await self.polymarket_api.get_market_by_slug(slug=slug)
        market_id = market.id
        self._market_id_by_slug[slug] = market_id
        self._slug_cache[market_id] = slug  # populate the reverse cache too
        return market_id

    def _to_orderbook(self, raw: Dict, market_id: str, outcome: str) -> OrderBook:
        """Converts a raw Polymarket US book response into the canonical OrderBook."""
        bids = {
            _numeric_amount(level["px"]): _numeric_amount(level["qty"])
            for level in raw.get("bids", [])
        }
        asks = {
            _numeric_amount(level["px"]): _numeric_amount(level["qty"])
            for level in raw.get("offers", [])
        }

        return OrderBook(
            market_id=market_id,
            exchange="POLYMARKET_US",
            outcome=outcome,
            bids=bids,
            asks=asks,
            timestamp=time.time(),
        )

    async def _to_position(self, raw_positions: Dict, strategy: str = "") -> List[Position]:
        """Converts client.portfolio.positions() response into canonical Positions.
        Outcome derived from sign of netPositionDecimal: positive = YES, negative = NO.
        """
        positions = []

        for slug, raw in raw_positions.get("positions", {}).items():
            net_position = getattr(raw, "netPositionDecimal", None)
            if net_position is None:
                net_position = getattr(raw, "netPosition")
            net = Decimal(str(net_position))
            if net == 0:
                continue

            outcome = "YES" if net > 0 else "NO"
            size = abs(net)

            qty_bought = Decimal(raw.qtyBoughtDecimal)
            cost = Decimal(raw.cost) / Decimal(1_000_000)
            entry_price = (cost / qty_bought) if qty_bought else Decimal(0)

            market_id = await self._get_market_id(slug)

            positions.append(Position(
                market_id=market_id,
                exchange="POLYMARKET_US",
                outcome=outcome,
                size=float(size),
                entry_price=float(entry_price),
                side=Side.BUY,
                strategy=strategy,
            ))

        return positions

    async def get_orderbook(self, market_id: str, outcome: str) -> OrderBook:
        slug = await self._get_slug(market_id=market_id)
        raw_orderbook = await self.polymarket_api.get_orderbook(slug=slug, outcome=outcome)
        orderbook = self._to_orderbook(raw=raw_orderbook, market_id=market_id, outcome=outcome)

        return orderbook

    async def get_positions(self) -> List[Position]:
        raw_positions = await self.polymarket_api.get_positions()
        positions = await self._to_position(raw_positions=raw_positions)

        return positions

    def _get_order_state(self, state: str) -> OrderStatus:
        status = self._STATUS_MAP.get(state)

        if status is None:
            #log error for unknown status
            return OrderStatus.UNKNOWN

        return status



    def _get_tif(self, tif: str) -> TimeInForce:
        try:
            time_in_force = self._TIF_MAP[tif]

            return time_in_force

        except KeyError:
            #Log error
            return TimeInForce.FOK  #returns default so entire order gets filled

    async def get_open_orders(self) -> List[OrderRecord]:
        raw = await self.polymarket_api.get_orders()
        orders = []

        for order in raw.get("orders", []):
            outcome, side = self._INTENT_MAP[_order_field(order, "intent")]
            size, filled_quantity = _order_size_and_filled(order)
            price = _order_field(order, "price")
            orders.append(OrderRecord(
                order_id=_order_field(order, "id"),
                market_id=await self._get_market_id(slug=_order_field(order, "marketSlug")),
                exchange="POLYMARKET_US",
                outcome=outcome,
                side=side,
                size=size,  # total/original order quantity
                price=_numeric_amount(price),
                status=self._get_order_state(state=_order_field(order, "state")),
                strategy="",                        # exchange doesn't know strategies but state.reconcile carries it over
                tif=self._get_tif(tif=_order_field(order, "tif")),
                filled_quantity=filled_quantity,
            ))

        return orders

    async def get_order(self, order_id: str) -> Optional[OrderRecord]:
        raw_response = await self.polymarket_api.get_order(order_id)
        if raw_response is None:
            return None
        raw = _order_field(raw_response, "order", raw_response)
        intent = _order_field(raw, "intent")
        if intent not in self._INTENT_MAP:
            raise ValueError(f"order {order_id} has missing or unknown intent")
        slug = _order_field(raw, "marketSlug")
        market_id = await self._resolve_order_market_id(raw, slug)
        size, filled_quantity = _order_size_and_filled(raw)
        outcome, side = self._INTENT_MAP[intent]
        return OrderRecord(
            order_id=_order_field(raw, "id", order_id),
            market_id=market_id,
            exchange="POLYMARKET_US",
            outcome=outcome,
            side=side,
            size=size,
            price=_numeric_amount(_order_field(raw, "price")),
            status=self._get_order_state(_order_field(raw, "state")),
            strategy="",
            tif=self._get_tif(_order_field(raw, "tif")),
            filled_quantity=filled_quantity,
        )

    async def _resolve_order_market_id(self, order, slug: Optional[str]) -> str:
        market_id = _order_field(order, "marketId")
        if market_id:
            return market_id
        if not slug:
            raise ValueError("order is missing marketSlug and marketId")
        return await self._get_market_id(slug=slug)

    async def place_order(self, order: OrderIntent) -> OrderRecord:
        response = await self.polymarket_api.create_order(
            slug=await self._get_slug(order.market_id),
            intent=self._REVERSE_INTENT_MAP[(order.outcome, order.side)],
            order_type=order.order_type,
            price=order.price,
            quantity=order.size,
            tif=order.tif,
            slippage=order.slippage,
            currency="USD" #add a strategy config change in future maybe
        )

        if 'error' in response:
            #log an error and return none
            return None

        executions = _order_field(response, "executions")
        payload = _order_field(executions, "order")
        outcome, side = self._INTENT_MAP[_order_field(payload, "intent")]
        size, filled_quantity = _order_size_and_filled(payload)
        record = OrderRecord(
            order_id=_order_field(payload, "id"),
            market_id=await self._get_market_id(slug=_order_field(payload, "marketSlug")),
            exchange="POLYMARKET_US",
            outcome=outcome,
            side=side,
            size=size,
            price=_numeric_amount(_order_field(payload, "price")),
            status=self._get_order_state(state=_order_field(payload, "state")),
            strategy=order.strategy,
            tif=self._get_tif(tif=_order_field(payload, "tif")),
            filled_quantity=filled_quantity,
        )

        return record

    async def cancel_order(self, order_id: str, market_id: str) -> None:
        slug = await self._get_slug(market_id=market_id)
        response = await self.polymarket_api.cancel_order(order_id=order_id, slug=slug)

        return

    async def get_balance(self) -> Dict[str, float]:
        response = await self.polymarket_api.get_balance()
        return {"USD": float(response.buyingPower)}


class PolyUsWebsocketClient(WebsocketClient):
    def __init__(
        self,
        key_id: Optional[str],
        secret_key: Optional[str],
        market_id_by_slug: Optional[Dict[str, str]] = None,
    ):
        self._market_id_by_slug = market_id_by_slug if market_id_by_slug is not None else {}
        self._INTENT_MAP = {
            "ORDER_INTENT_BUY_LONG": ("YES", Side.BUY),
            "ORDER_INTENT_SELL_LONG": ("YES", Side.SELL),
            "ORDER_INTENT_BUY_SHORT": ("NO", Side.BUY),
            "ORDER_INTENT_SELL_SHORT": ("NO", Side.SELL),
        }
        self.polymarket_api = PolymarketAPI(key_id=key_id, secret_key=secret_key)
        self.websocket = PolyUSWebsocket(
            key_id=key_id,
            secret_key=secret_key,
            handlers={
                "market_data": self._handle_market_data,
                "market_data_lite": self._handle_market_data_lite,
                "trade": self._handle_trade,
                "order_snapshot": self._handle_order_snapshot,
                "order_update": self._handle_order_update,
                "position_snapshot": self._handle_position_snapshot,
                "position_update": self._handle_position_update,
                "account_balance_snapshot": self._handle_account_balance_snapshot,
                "account_balance_update": self._handle_account_balance_update,
                "rfq_event": self._handle_rfq_event,
            },
        )
        self.queue: asyncio.Queue = asyncio.Queue()
        self._order_snapshot_orders: List[OrderRecord] = []
        self._order_snapshot_skipped = False

    @staticmethod
    def _timestamp(value) -> float:
        if value is None or value == "":
            return time.time()
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            result = float(value)
            if math.isfinite(result):
                return result
            raise ValueError(f"invalid timestamp {value!r}")
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except (AttributeError, TypeError, ValueError):
            try:
                result = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid timestamp {value!r}") from exc
        if not math.isfinite(result):
            raise ValueError(f"invalid timestamp {value!r}")
        return result

    async def _resolve_market_id(
        self, slug: Optional[str], event_name: str, market_id: Optional[str] = None
    ) -> Optional[str]:
        """Resolve/cache a slug via REST when the shared market cache misses."""
        if isinstance(market_id, str) and market_id.strip():
            return market_id
        if not isinstance(slug, str) or not slug.strip():
            logger.warning("Dropping Polymarket US %s event: missing market slug/ID", event_name)
            return None
        market_id = self._market_id_by_slug.get(slug)
        if market_id:
            return market_id
        try:
            market = await self.polymarket_api.get_market_by_slug(slug=slug)
            resolved_id = getattr(market, "id", None)
            resolved_slug = getattr(market, "slug", None)
            if not isinstance(resolved_id, str) or not resolved_id.strip():
                raise ValueError("market lookup returned no ID")
            self._market_id_by_slug[slug] = resolved_id
            if isinstance(resolved_slug, str) and resolved_slug:
                self._market_id_by_slug[resolved_slug] = resolved_id
            return resolved_id
        except Exception as exc:
            logger.warning(
                "Dropping Polymarket US %s event: could not resolve slug %r: %s",
                event_name, slug, exc,
            )
            return None

    async def _handle_market_data(self, message: Dict) -> None:
        """Convert a Polymarket US full-book message into YES and NO snapshots."""
        payload = message.get("marketData") if isinstance(message, dict) else None
        if not isinstance(payload, dict) or not isinstance(payload.get("bids"), list) or not isinstance(payload.get("offers"), list):
            logger.warning("Dropping malformed Polymarket US market data message")
            return
        slug = payload.get("marketSlug")
        market_id = await self._resolve_market_id(slug, "market data", payload.get("marketId"))
        if not market_id:
            return

        try:
            def parse_levels(levels):
                result = {}
                for level in levels:
                    if not isinstance(level, dict) or "px" not in level or "qty" not in level:
                        raise ValueError("book level requires px and qty")
                    price, quantity = _numeric_amount(level["px"]), _numeric_amount(level["qty"])
                    if quantity < 0:
                        raise ValueError("book quantity cannot be negative")
                    result[price] = quantity
                return result
            yes_bids = parse_levels(payload["bids"])
            yes_asks = parse_levels(payload["offers"])
            timestamp = self._timestamp(payload.get("transactTime"))

            self.queue.put_nowait(OrderBookSnapshotEvent(
                exchange="POLYMARKET_US", timestamp=timestamp,
                market_id=market_id, outcome="YES",
                bids=yes_bids, asks=yes_asks,
            ))
            # The US API publishes a YES-denominated book; derive the complementary NO book.
            no_bids = {round(1.0 - price, 6): size for price, size in yes_asks.items()}
            no_asks = {round(1.0 - price, 6): size for price, size in yes_bids.items()}
            self.queue.put_nowait(OrderBookSnapshotEvent(
                exchange="POLYMARKET_US", timestamp=timestamp,
                market_id=market_id, outcome="NO",
                bids=no_bids, asks=no_asks,
            ))
        except (AttributeError, TypeError, ValueError) as exc:
            logger.warning("Dropping malformed Polymarket US market data message: %s", exc)

    async def _market_event_fields(self, payload: Dict, event_name: str, outcome: Optional[str] = None) -> Optional[Dict]:
        if not isinstance(payload, dict):
            logger.warning("Dropping malformed Polymarket US %s message", event_name)
            return None
        slug = payload.get("marketSlug")
        market_id = await self._resolve_market_id(slug, event_name, payload.get("marketId"))
        if market_id is None:
            return None
        try:
            timestamp = self._timestamp(payload.get("transactTime") or payload.get("tradeTime"))
        except (TypeError, ValueError) as exc:
            logger.warning("Dropping malformed Polymarket US %s timestamp: %s", event_name, exc)
            return None
        return {
            "market_id": market_id,
            "outcome": outcome or "YES",
            "timestamp": timestamp,
            "exchange": "POLYMARKET_US",
        }

    @staticmethod
    def _find_market_slug(value) -> Optional[str]:
        """Find marketSlug in nested SDK event payloads such as order executions."""
        if isinstance(value, dict):
            slug = value.get("marketSlug")
            if slug:
                return slug
            for nested_value in value.values():
                slug = PolyUsWebsocketClient._find_market_slug(nested_value)
                if slug:
                    return slug
        elif isinstance(value, list):
            for nested_value in value:
                slug = PolyUsWebsocketClient._find_market_slug(nested_value)
                if slug:
                    return slug
        return None

    async def _handle_market_data_lite(self, message: Dict) -> None:
        payload = message.get("marketDataLite") if isinstance(message, dict) else None
        if not isinstance(payload, dict) or not any(k in payload for k in ("bestBid", "bestAsk", "currentPx", "lastTradePx")):
            logger.warning("Dropping malformed Polymarket US lite market data message")
            return
        event_fields = await self._market_event_fields(payload, "lite market data")
        if event_fields is None:
            return
        try:
            self.queue.put_nowait(LiteOrderBookSnapshotEvent(
                **event_fields,
                best_bid=_numeric_amount(payload["bestBid"]) if payload.get("bestBid") is not None else None,
                best_ask=_numeric_amount(payload["bestAsk"]) if payload.get("bestAsk") is not None else None,
                last_price=_numeric_amount(payload["currentPx"] if payload.get("currentPx") is not None else payload["lastTradePx"])
                if payload.get("currentPx") is not None or payload.get("lastTradePx") is not None else None,
            ))
        except (AttributeError, TypeError, ValueError) as exc:
            logger.warning("Dropping malformed Polymarket US lite market data message: %s", exc)

    async def _handle_trade(self, message: Dict) -> None:
        payload = message.get("trade") if isinstance(message, dict) else None
        if not isinstance(payload, dict) or not isinstance(payload.get("maker"), dict) or not isinstance(payload.get("taker"), dict):
            logger.warning("Dropping malformed Polymarket US trade message")
            return
        intent = payload["maker"].get("intent")
        taker_side = payload["taker"].get("side")
        if intent not in self._INTENT_MAP or taker_side not in ("ORDER_SIDE_BUY", "ORDER_SIDE_SELL"):
            logger.warning("Dropping malformed Polymarket US trade message: invalid intent/side")
            return
        outcome = self._INTENT_MAP[intent][0]
        event_fields = await self._market_event_fields(payload, "trade", outcome)
        if event_fields is None:
            return
        try:
            self.queue.put_nowait(TradeEvent(
                **event_fields,
                trade_id=payload.get("tradeId"),
                price=_numeric_amount(payload["price"]),
                quantity=_numeric_amount(payload["quantity"]),
                side=Side.BUY if taker_side == "ORDER_SIDE_BUY" else Side.SELL,
            ))
        except (AttributeError, TypeError, ValueError) as exc:
            logger.warning("Dropping malformed Polymarket US trade message: %s", exc)

    async def _to_order_record(self, raw: Dict) -> Optional[OrderRecord]:
        if not isinstance(raw, dict):
            raise ValueError("order must be an object")
        market_id = await self._resolve_market_id(raw.get("marketSlug"), "order", raw.get("marketId"))
        if market_id is None:
            return None
        intent = raw.get("intent")
        if intent not in self._INTENT_MAP:
            raise ValueError("order has missing or unknown intent")
        if not isinstance(raw.get("id"), str) or not raw["id"]:
            raise ValueError("order has no ID")
        try:
            size, filled_quantity = _order_size_and_filled(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("order has missing quantity fields") from exc
        if "price" not in raw or not raw.get("state") or not raw.get("tif"):
            raise ValueError("order is missing price, quantity, state, or time-in-force")
        outcome, side = self._INTENT_MAP[intent]
        tif = self._websocket_tif(raw["tif"])
        if tif is None:
            raise ValueError(f"order has unknown time-in-force {raw['tif']!r}")
        return OrderRecord(
            order_id=raw["id"], market_id=market_id,
            exchange="POLYMARKET_US", outcome=outcome, side=side,
            size=size, price=_numeric_amount(raw["price"]),
            status=self._websocket_order_status(raw["state"]),
            tif=tif,
            filled_quantity=filled_quantity,
        )

    @staticmethod
    def _websocket_order_status(state: str) -> OrderStatus:
        return {
            "ORDER_STATE_PENDING_NEW": OrderStatus.NEW,
            "ORDER_STATE_NEW": OrderStatus.NEW,
            "ORDER_STATE_PENDING_REPLACE": OrderStatus.NEW,
            "ORDER_STATE_PENDING_CANCEL": OrderStatus.NEW,
            "ORDER_STATE_PENDING_RISK": OrderStatus.NEW,
            "ORDER_STATE_PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "ORDER_STATE_FILLED": OrderStatus.FILLED,
            "ORDER_STATE_CANCELED": OrderStatus.CANCELED,
            "ORDER_STATE_REPLACED": OrderStatus.CANCELED,
            "ORDER_STATE_REJECTED": OrderStatus.REJECTED,
            "ORDER_STATE_EXPIRED": OrderStatus.EXPIRED,
        }.get(state, OrderStatus.UNKNOWN)

    @staticmethod
    def _websocket_tif(tif: str) -> TimeInForce:
        return {
            "TIME_IN_FORCE_GOOD_TILL_CANCEL": TimeInForce.GTC,
            "TIME_IN_FORCE_GOOD_TILL_DATE": TimeInForce.GTD,
            "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL": TimeInForce.IOC,
            "TIME_IN_FORCE_FILL_OR_KILL": TimeInForce.FOK,
        }.get(tif)

    async def _handle_order_snapshot(self, message: Dict) -> None:
        payload = message.get("orderSubscriptionSnapshot") if isinstance(message, dict) else None
        if not isinstance(payload, dict) or not isinstance(payload.get("orders"), list) or not isinstance(payload.get("eof"), bool):
            logger.warning("Dropping malformed Polymarket US order snapshot")
            return
        for raw_order in payload.get("orders", []):
            try:
                order = await self._to_order_record(raw_order)
                if order is not None:
                    self._order_snapshot_orders.append(order)
                else:
                    self._order_snapshot_skipped = True
            except (AttributeError, TypeError, ValueError) as exc:
                self._order_snapshot_skipped = True
                logger.warning("Skipping malformed Polymarket US order in snapshot: %s", exc)
        if payload["eof"]:
            self.queue.put_nowait(OrderSnapshotEvent(
                exchange="POLYMARKET_US", timestamp=time.time(),
                orders=list(self._order_snapshot_orders),
                complete=not self._order_snapshot_skipped,
            ))
            self._order_snapshot_orders.clear()
            self._order_snapshot_skipped = False

    async def _handle_order_update(self, message: Dict) -> None:
        payload = message.get("orderSubscriptionUpdate") if isinstance(message, dict) else None
        execution = payload.get("execution") if isinstance(payload, dict) else None
        raw_order = execution.get("order") if isinstance(execution, dict) else None
        if not isinstance(raw_order, dict):
            logger.warning("Dropping malformed Polymarket US order update: missing order")
            return
        try:
            order = await self._to_order_record(raw_order)
            if order is None:
                return
            fill_quantity = fill_price = None
            if execution.get("lastShares") is not None:
                if execution.get("lastPx") is None:
                    raise ValueError("execution has lastShares but no lastPx")
                fill_quantity = _numeric_amount(execution["lastShares"])
                fill_price = _numeric_amount(execution["lastPx"])
            self.queue.put_nowait(OrderUpdateEvent(
                exchange="POLYMARKET_US", timestamp=time.time(), order=order,
                fill_quantity=fill_quantity, fill_price=fill_price,
                execution_id=execution.get("executionId") or execution.get("tradeId"),
            ))
        except (AttributeError, TypeError, ValueError) as exc:
            logger.warning("Dropping malformed Polymarket US order update: %s", exc)

    async def _handle_position_snapshot(self, message: Dict) -> None:
        payload = message.get("positionSubscriptionSnapshot") if isinstance(message, dict) else None
        if not isinstance(payload, dict) or not isinstance(payload.get("positions"), (list, dict)):
            logger.warning("Dropping malformed Polymarket US position snapshot")
            return
        raw_positions = payload["positions"]
        if isinstance(raw_positions, dict):
            raw_positions = [
                {"marketSlug": slug, **position} if isinstance(position, dict) else position
                for slug, position in raw_positions.items()
            ]
        positions = []
        for raw in raw_positions:
            try:
                if not isinstance(raw, dict):
                    raise ValueError("position must be an object")
                slug = self._find_market_slug(raw)
                market_id = await self._resolve_market_id(slug, "position snapshot", raw.get("marketId"))
                if market_id is None:
                    return
                position = self._position_from_payload(raw, market_id)
                if position is not None:
                    positions.append(position)
            except (AttributeError, TypeError, ValueError) as exc:
                logger.warning("Dropping malformed Polymarket US position snapshot: %s", exc)
                return
        self.queue.put_nowait(PositionSnapshotEvent(
            exchange="POLYMARKET_US", timestamp=time.time(), positions=positions,
        ))

    async def _handle_position_update(self, message: Dict) -> None:
        payload = message.get("positionSubscription", message.get("positionSubscriptionUpdate")) if isinstance(message, dict) else None
        if not isinstance(payload, dict):
            logger.warning("Dropping malformed Polymarket US position update")
            return
        slug = self._find_market_slug(payload)
        market_id = await self._resolve_market_id(slug, "position update", payload.get("marketId"))
        if market_id is None:
            return
        try:
            position = self._position_from_payload(payload, market_id)
            self.queue.put_nowait(PositionUpdateEvent(
                exchange="POLYMARKET_US", timestamp=self._timestamp(payload.get("updateTime")),
                market_id=market_id, position=position, removed=position is None,
            ))
        except (AttributeError, TypeError, ValueError) as exc:
            logger.warning("Dropping malformed Polymarket US position update: %s", exc)

    def _position_from_payload(self, payload: Dict, market_id: Optional[str] = None) -> Optional[Position]:
        after = payload.get("afterPosition", payload)
        if not isinstance(after, dict):
            raise ValueError("position afterPosition must be an object")
        if market_id is None:
            raise ValueError("position has no resolved market ID")
        net_value = after.get("netPositionDecimal", after.get("netPosition"))
        if net_value is None:
            raise ValueError("position has no net position")
        net = _numeric_amount(net_value)
        if net == 0:
            return None
        outcome = "YES" if net > 0 else "NO"
        quantity_bought = _numeric_amount(after.get("qtyBoughtDecimal", abs(net)))
        cost_value = after.get("cost")
        if cost_value is None:
            raise ValueError("position has no cost")
        cost = _numeric_amount(cost_value)
        if quantity_bought <= 0 or cost < 0:
            raise ValueError("position quantity must be positive and cost cannot be negative")
        return Position(
            market_id=market_id, exchange="POLYMARKET_US", outcome=outcome,
            size=abs(net), entry_price=cost / quantity_bought if quantity_bought else 0.0,
            side=Side.BUY, strategy="",
        )

    async def _handle_account_balance_snapshot(self, message: Dict) -> None:
        payload = message.get("accountBalancesSnapshot", message.get("accountBalanceSubscriptionSnapshot")) if isinstance(message, dict) else None
        if not isinstance(payload, dict) or not isinstance(payload.get("balances"), list):
            logger.warning("Dropping malformed Polymarket US balance snapshot")
            return
        balances = {}
        for item in payload["balances"]:
            try:
                if not isinstance(item, dict) or not isinstance(item.get("currency"), str) or not item["currency"]:
                    raise ValueError("balance requires currency")
                amount = item.get("buyingPower", item.get("currentBalance"))
                if amount is None:
                    raise ValueError("balance requires buyingPower/currentBalance")
                balances[item["currency"]] = _numeric_amount(amount)
            except (AttributeError, TypeError, ValueError) as exc:
                logger.warning("Skipping malformed Polymarket US balance snapshot item: %s", exc)
        self.queue.put_nowait(BalanceSnapshotEvent(
            exchange="POLYMARKET_US", timestamp=time.time(), balances=balances
        ))

    async def _handle_account_balance_update(self, message: Dict) -> None:
        payload = message.get("accountBalancesUpdate", message.get("accountBalanceSubscriptionUpdate")) if isinstance(message, dict) else None
        change = payload.get("balanceChange") if isinstance(payload, dict) else None
        after = change.get("afterBalance") if isinstance(change, dict) else None
        if not isinstance(after, dict) or not isinstance(after.get("currency"), str) or not after["currency"]:
            logger.warning("Dropping malformed Polymarket US balance update")
            return
        try:
            amount = after.get("buyingPower", after.get("currentBalance", after.get("balance")))
            if amount is None:
                raise ValueError("balance has no amount")
            self.queue.put_nowait(BalanceUpdateEvent(
                exchange="POLYMARKET_US", timestamp=self._timestamp(change.get("updateTime")),
                currency=after["currency"], balance=_numeric_amount(amount),
            ))
        except (AttributeError, TypeError, ValueError) as exc:
            logger.warning("Dropping malformed Polymarket US balance update: %s", exc)

    async def _handle_rfq_event(self, message: Dict) -> None:
        envelope = message.get("rfqEvent") if isinstance(message, dict) else None
        if not isinstance(envelope, dict) or len(envelope) != 1:
            logger.warning("Dropping malformed Polymarket US RFQ message")
            return
        event_type, payload = next(iter(envelope.items()))
        if not isinstance(payload, dict):
            logger.warning("Dropping malformed Polymarket US RFQ message: payload is not an object")
            return
        raw_trade = payload.get("trade") if event_type == "rfqTrade" else None
        if event_type == "rfqTrade" and not isinstance(raw_trade, dict):
            logger.warning("Dropping malformed Polymarket US RFQ trade: missing trade")
            return
        slug = self._find_market_slug(payload)
        if slug is None:
            rfq = payload.get("rfq")
            if isinstance(rfq, dict):
                slug = rfq.get("symbol")
        # RFQ trade messages identify the instrument by symbol rather than marketSlug.
        if slug is None and isinstance(raw_trade, dict):
            slug = raw_trade.get("symbol")
        market_id = await self._resolve_market_id(slug, "RFQ") if slug else None
        trade = None
        if raw_trade is not None:
            if market_id is None:
                return
            try:
                aggressor_side = raw_trade.get("aggressorSide")
                if aggressor_side not in ("SIDE_BUY", "SIDE_SELL"):
                    raise ValueError("RFQ trade has invalid aggressorSide")
                trade = TradeEvent(
                    exchange="POLYMARKET_US", timestamp=self._timestamp(raw_trade.get("executedTime")),
                    market_id=market_id, outcome="UNKNOWN",
                    trade_id=raw_trade.get("tradeId"), price=_numeric_amount(raw_trade.get("price")),
                    quantity=_numeric_amount(raw_trade.get("qtyDecimal")),
                    side=Side.BUY if aggressor_side == "SIDE_BUY" else Side.SELL,
                )
            except (AttributeError, TypeError, ValueError) as exc:
                logger.warning("Dropping malformed Polymarket US RFQ trade: %s", exc)
                return
        try:
            timestamp = self._timestamp(payload.get("executedTime") or payload.get("updateTime"))
        except (TypeError, ValueError) as exc:
            logger.warning("Dropping malformed Polymarket US RFQ timestamp: %s", exc)
            return
        self.queue.put_nowait(RfqEvent(
            exchange="POLYMARKET_US", timestamp=timestamp,
            event_type=event_type, request_id=message.get("requestId"),
            market_id=market_id, trade=trade,
        ))

    async def start(self) -> None:
        await self.websocket.start()

    async def subscribe(self, market_ids: List[str]) -> None:
        for market_id in market_ids:
            cached_slug = next(
                (slug for slug, cached_id in self._market_id_by_slug.items() if cached_id == market_id),
                None,
            )
            if cached_slug is None:
                market = await self.polymarket_api.get_market(market_id)
                slug = market.slug
                market_id = market.id
            else:
                slug = cached_slug
            self._market_id_by_slug[slug] = market_id
            await self.websocket.add_market_subscription(
                {
                    "SUBSCRIPTION_TYPE_MARKET_DATA",
                    "SUBSCRIPTION_TYPE_MARKET_DATA_LITE",
                    "SUBSCRIPTION_TYPE_TRADE",
                },
                slug,
            )

    async def run(self) -> None:
        """Connect, listen, normalize events, push MarketEvent onto queue"""
        if not any(self.websocket._connected.values()):
            await self.start()
        await self.websocket.run()


def create_poly_us_clients(
    key_id: str,
    secret_key: str,
) -> Tuple[PolyUsClient, PolyUsWebsocketClient]:
    """Build REST and websocket clients with one shared slug-to-market-ID map."""
    market_id_by_slug: Dict[str, str] = {}
    return (
        PolyUsClient(key_id, secret_key, market_id_by_slug),
        PolyUsWebsocketClient(key_id, secret_key, market_id_by_slug),
    )
