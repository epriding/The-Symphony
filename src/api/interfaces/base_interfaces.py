from abc import ABC, abstractmethod
from typing import List, Any, Dict
import asyncio
from ...utilities.classes import OrderIntent, OrderRecord, Position, MarketInfo, OrderBook

class RestClient(ABC):
    """Contract that OrderCoordinator depends on. Real client and
    PaperTradingEngine both implement this so the backend is swappable."""

    @abstractmethod
    async def get_markets(self, active: bool = True, limit: int = 200, offset: Any = None) -> List[MarketInfo]:
        pass

    @abstractmethod
    async def get_orderbook(self, market_id: str, outcome: str) -> OrderBook:
        pass

    @abstractmethod
    async def get_positions(self) -> List[Position]:
        pass

    @abstractmethod
    async def get_open_orders(self) -> List[OrderRecord]:
        pass

    @abstractmethod
    async def get_order(self, order_id: str) -> OrderRecord | None:
        """Fetch an individual order, including its final cumulative fill state."""
        pass

    @abstractmethod
    async def place_order(self, order: OrderIntent) -> OrderRecord:
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, market_id: str) -> None:
        pass

    @abstractmethod
    async def get_balance(self) -> Dict[str, float]:
        pass



class WebsocketClient(ABC):
    """Contract for exchange-specific market data ingestion.
    Each exchange (Polymarket, Kalshi, etc.) implements this.
    Ingestion only — never makes trading decisions."""

    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def subscribe(self, market_ids: List[str]) -> None:
        pass

    @abstractmethod
    async def run(self) -> None:
        """Connect, listen, normalize exchange messages, and queue typed events."""
        pass
