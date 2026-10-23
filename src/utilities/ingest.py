"""Apply normalized websocket events to the bot's shared trading state."""

from __future__ import annotations

import asyncio
import inspect
from typing import Awaitable, Callable, Optional

from ..api.interfaces.base_interfaces import RestClient, WebsocketClient
from ..market_decisions.order_coordinator import OrderCoordinator
from .classes import (
    AccountConnectionEvent,
    BalanceSnapshotEvent,
    BalanceUpdateEvent,
    LiteOrderBookSnapshotEvent,
    OrderBook,
    OrderBookSnapshotEvent,
    OrderSnapshotEvent,
    OrderUpdateEvent,
    PositionSnapshotEvent,
    PositionUpdateEvent,
    PrivateEvent,
    RfqEvent,
    TradeEvent,
)
from .logger import setup_logger
from .trading_state import TradingState


logger = setup_logger(__name__)
EventHandler = Callable[[object], Optional[Awaitable[None]]]


class WebsocketEventIngestor:
    """Consume a websocket client's typed queue and apply events to TradingState."""

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

    async def run(self) -> None:
        """Process events until cancelled; a bad event does not stop the queue."""
        while True:
            event = await self.websocket_client.queue.get()
            try:
                await self.handle_event(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                if isinstance(event, PrivateEvent):
                    async with self.state.lock:
                        self.state.portfolios["LIVE"].dirty_exchanges.add(event.exchange)
                logger.exception("Failed to ingest websocket event %r", event)
            finally:
                self.websocket_client.queue.task_done()

    async def handle_event(self, event: object) -> None:
        if isinstance(event, AccountConnectionEvent):
            async with self.state.lock:
                portfolio = self.state.portfolios["LIVE"]
                self.state._invalidate_exchange(event.exchange)
                if event.connected:
                    portfolio.disconnected_exchanges.discard(event.exchange)
                else:
                    portfolio.disconnected_exchanges.add(event.exchange)
            if event.connected:
                # Connectivity is restored even if this refresh fails. The dirty
                # flag keeps trading blocked until a later reconciliation succeeds.
                await self.coordinator.reconcile(event.exchange)
        elif isinstance(event, OrderBookSnapshotEvent):
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
            await self.state.replace_balances(event.balances, event.exchange, event.timestamp)
        elif isinstance(event, BalanceUpdateEvent):
            await self.state.update_balance(event.currency, event.balance, event.exchange)
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
        await self.coordinator.apply_order_snapshot(event.orders, event.exchange, event.complete)

    async def _apply_order_update(self, event: OrderUpdateEvent) -> None:
        await self.coordinator.apply_order_update(
            event.order, fill_quantity=event.fill_quantity,
            fill_price=event.fill_price, execution_id=event.execution_id,
        )

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
    # A failed bootstrap propagates: do not start trading from partial state.
    await coordinator.reconcile(exchange)

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
