"""Apply normalized websocket events to the bot's shared trading state."""

from __future__ import annotations

import asyncio
import inspect
from collections import OrderedDict, deque
from dataclasses import replace
from typing import Awaitable, Callable, Deque, Optional, Set, Tuple

from ..api.interfaces.base_interfaces import RestClient, WebsocketClient
from ..market_decisions.order_coordinator import OrderCoordinator
from .classes import (
    BalanceSnapshotEvent,
    BalanceUpdateEvent,
    LiteOrderBookSnapshotEvent,
    OrderBook,
    OrderBookSnapshotEvent,
    OrderSnapshotEvent,
    OrderStatus,
    OrderUpdateEvent,
    PositionSnapshotEvent,
    PositionUpdateEvent,
    RfqEvent,
    TradeEvent,
)
from .logger import setup_logger
from .trading_state import TradingState


logger = setup_logger(__name__)
EventHandler = Callable[[object], Optional[Awaitable[None]]]


class WebsocketEventIngestor:
    """Consume a websocket client's typed queue and apply events to TradingState."""

    _TERMINAL_ORDER_STATUSES = {
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    }
    _MAX_REMEMBERED_EXECUTIONS = 10_000
    _MAX_REMEMBERED_ORDERS = 10_000

    def __init__(
        self,
        websocket_client: WebsocketClient,
        state: TradingState,
        coordinator: OrderCoordinator,
        event_handler: Optional[EventHandler] = None,
    ) -> None:
        self.websocket_client = websocket_client
        self.state = state
        self.coordinator = coordinator
        self.event_handler = event_handler
        self._filled_by_order: OrderedDict[str, float] = OrderedDict()
        self._seen_execution_ids: Set[Tuple[str, str]] = set()
        self._execution_id_order: Deque[Tuple[str, str]] = deque()
        self._order_snapshot_ready = False
        self._pending_order_updates = []

    async def run(self) -> None:
        """Process events until cancelled; a bad event does not stop the queue."""
        while True:
            event = await self.websocket_client.queue.get()
            try:
                await self.handle_event(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Failed to ingest websocket event %r", event)
            finally:
                self.websocket_client.queue.task_done()

    async def handle_event(self, event: object) -> None:
        if isinstance(event, OrderBookSnapshotEvent):
            await self._apply_orderbook_snapshot(event)
        elif isinstance(event, LiteOrderBookSnapshotEvent):
            await self.state.update_market_quote(event)
        elif isinstance(event, TradeEvent):
            if event.outcome.upper() in {"YES", "NO"}:
                await self.state.update_market_price(
                    event.market_id, event.exchange, event.outcome.upper(),
                    event.price, event.timestamp,
                )
            await self._forward(event)
        elif isinstance(event, OrderSnapshotEvent):
            await self._apply_order_snapshot(event)
        elif isinstance(event, OrderUpdateEvent):
            await self._apply_order_update(event)
        elif isinstance(event, PositionSnapshotEvent):
            await self.state.apply_live_position_snapshot(event.positions, event.exchange)
        elif isinstance(event, PositionUpdateEvent):
            if event.removed or event.position is None:
                await self.state.remove_live_position(event.market_id, event.exchange)
            else:
                await self.state.update_live_position(event.position)
        elif isinstance(event, BalanceSnapshotEvent):
            await self.state.replace_balances(event.balances)
        elif isinstance(event, BalanceUpdateEvent):
            await self.state.update_balance(event.currency, event.balance)
        elif isinstance(event, RfqEvent):
            await self._forward(event)
        else:
            logger.warning("Ignoring unsupported websocket event type %s", type(event).__name__)

    async def _apply_orderbook_snapshot(self, event: OrderBookSnapshotEvent) -> None:
        key = f"{event.exchange}:{event.market_id}:{event.outcome}"
        current = self.state.orderbooks.get(key)
        if current is not None and current.timestamp > event.timestamp:
            return
        orderbook = OrderBook(
            market_id=event.market_id,
            exchange=event.exchange,
            outcome=event.outcome,
            bids=dict(event.bids),
            asks=dict(event.asks),
            timestamp=event.timestamp,
        )
        await self.state.update_orderbook(orderbook)

    async def _apply_order_snapshot(self, event: OrderSnapshotEvent) -> None:
        if not event.complete:
            logger.warning("Ignoring incomplete %s order snapshot", event.exchange)
            return
        for order in event.orders:
            key = f"{order.exchange}:{order.order_id}"
            if order.filled_quantity is not None:
                self._remember_cumulative(
                    key, max(self._filled_by_order.get(key, 0.0), order.filled_quantity)
                )
        await self.state.apply_order_snapshot(event.exchange, event.orders, event.complete)
        self._order_snapshot_ready = True
        pending, self._pending_order_updates = self._pending_order_updates, []
        for pending_update in pending:
            await self._apply_order_update(pending_update)

    async def _apply_order_update(self, event: OrderUpdateEvent) -> None:
        if not self._order_snapshot_ready:
            self._pending_order_updates.append(event)
            return
        order = event.order
        key = f"{order.exchange}:{order.order_id}"
        existing = self.state.open_orders.get(key)
        if existing is not None:
            order.strategy = existing.strategy

        cumulative = order.filled_quantity
        known_cumulative = max(
            self._filled_by_order.get(key, 0.0),
            existing.filled_quantity if existing and existing.filled_quantity is not None else 0.0,
        )

        fill_change = 0.0
        if cumulative is not None:
            fill_change = max(0.0, cumulative - known_cumulative)
            self._remember_cumulative(key, max(known_cumulative, cumulative))
        elif event.fill_quantity is not None:
            execution_key = (key, event.execution_id) if event.execution_id else None
            if execution_key is None or execution_key not in self._seen_execution_ids:
                fill_change = event.fill_quantity
                if execution_key is not None:
                    self._remember_execution(execution_key)
            self._remember_cumulative(key, known_cumulative + fill_change)
            order.filled_quantity = self._filled_by_order[key]

        if fill_change > 0:
            fill = replace(
                order,
                size=fill_change,
                price=event.fill_price if event.fill_price is not None else order.price,
                status=OrderStatus.FILLED,
            )
            await self.coordinator.apply_fill(fill)

        if order.status in self._TERMINAL_ORDER_STATUSES:
            await self.state.remove_open_order(order.order_id, order.exchange)
        else:
            await self.state.update_open_order(order)

    def _remember_execution(self, execution_key: Tuple[str, str]) -> None:
        self._seen_execution_ids.add(execution_key)
        self._execution_id_order.append(execution_key)
        while len(self._execution_id_order) > self._MAX_REMEMBERED_EXECUTIONS:
            expired = self._execution_id_order.popleft()
            self._seen_execution_ids.discard(expired)

    def _remember_cumulative(self, order_key: str, quantity: float) -> None:
        self._filled_by_order[order_key] = quantity
        self._filled_by_order.move_to_end(order_key)
        while len(self._filled_by_order) > self._MAX_REMEMBERED_ORDERS:
            self._filled_by_order.popitem(last=False)

    async def _forward(self, event: object) -> None:
        if self.event_handler is None:
            return
        result = self.event_handler(event)
        if inspect.isawaitable(result):
            await result


async def run_websocket_ingestion(
    websocket_client: WebsocketClient,
    rest_client: RestClient,
    state: TradingState,
    coordinator: OrderCoordinator,
    exchange: str,
    event_handler: Optional[EventHandler] = None,
) -> None:
    """Bootstrap live account state, then run websocket ingestion and streaming."""
    try:
        positions = await rest_client.get_positions()
        await state.apply_live_position_snapshot(positions, exchange)
    except Exception:
        logger.exception("Could not load initial positions before websocket startup")

    try:
        balances = await rest_client.get_balance()
        await state.replace_balances(balances)
    except Exception:
        logger.exception("Could not load initial balances before websocket startup")

    ingestor = WebsocketEventIngestor(
        websocket_client=websocket_client,
        state=state,
        coordinator=coordinator,
        event_handler=event_handler,
    )
    ingest_task = asyncio.create_task(ingestor.run(), name="websocket-event-ingestor")
    try:
        await websocket_client.run()
    finally:
        ingest_task.cancel()
        await asyncio.gather(ingest_task, return_exceptions=True)
