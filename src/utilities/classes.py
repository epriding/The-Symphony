from dataclasses import dataclass, field
from typing import Optional, Dict, List
from enum import Enum

@dataclass(frozen=True)
class MarketEvent:
    """Common fields every event carries."""
    market_id: str
    outcome: str
    timestamp: float
    exchange: str

@dataclass(frozen=True)
class PrivateEvent:
    """Common fields every private event carries."""
    exchange: str
    timestamp: float

@dataclass(frozen=True)
class OrderSnapshotEvent(PrivateEvent):
    """Snapshot of normalized open orders."""
    orders: List["OrderRecord"] = field(default_factory=list)
    complete: bool = True

@dataclass(frozen=True)
class OrderUpdateEvent(PrivateEvent):
    """Normalized order state and optional fill information."""
    order: "OrderRecord"
    fill_quantity: Optional[float] = None
    fill_price: Optional[float] = None
    execution_id: Optional[str] = None

@dataclass(frozen=True)
class PositionSnapshotEvent(PrivateEvent):
    """Snapshot of normalized open positions."""
    positions: List["Position"] = field(default_factory=list)

@dataclass(frozen=True)
class PositionUpdateEvent(PrivateEvent):
    """A position replacement or removal."""
    market_id: str
    position: Optional["Position"]
    removed: bool = False

@dataclass(frozen=True)
class BalanceSnapshotEvent(PrivateEvent):
    """Snapshot of account balances keyed by currency."""
    balances: Dict[str, float] = field(default_factory=dict)

@dataclass(frozen=True)
class BalanceUpdateEvent(PrivateEvent):
    """A normalized account balance change."""
    currency: str
    balance: float

@dataclass(frozen=True)
class OrderBookSnapshotEvent(MarketEvent):
    """Full orderbook replace."""
    bids: Dict[float, float]   # price -> size
    asks: Dict[float, float]


@dataclass(frozen=True)
class LiteOrderBookSnapshotEvent(MarketEvent):
    """Top-of-book quote when full depth is unavailable."""
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_price: Optional[float] = None

@dataclass(frozen=True)
class TradeEvent(MarketEvent):
    """Normalized public or private fill."""
    price: float
    quantity: float
    side: Optional["Side"] = None
    trade_id: Optional[str] = None


@dataclass(frozen=True)
class RfqEvent(PrivateEvent):
    """RFQ lifecycle notification using common identifiers."""
    event_type: str
    market_id: Optional[str] = None
    request_id: Optional[str] = None
    trade: Optional[TradeEvent] = None

@dataclass(frozen=True) # no use for this yet, leaving just in case another exchange uses it
class PriceChangeEvent(MarketEvent):
    """Incremental update — merge into existing book, don't replace."""
    price: float
    size: float
    side: str  # "BID" or "ASK"

@dataclass
class OrderBook:
    market_id: str
    exchange: str
    outcome: str
    bids: Dict[float, float]   # price -> size
    asks: Dict[float, float]
    timestamp: float

    def apply_snapshot(self, event: OrderBookSnapshotEvent) -> None:
        # full replace — snapshot is authoritative
        self.bids = dict(event.bids)
        self.asks = dict(event.asks)
        self.timestamp = event.timestamp

    def apply_price_change(self, event: PriceChangeEvent) -> None:
        book_side = self.bids if event.side == "BID" else self.asks
        if event.size == 0:
            book_side.pop(event.price, None)   # size 0 = level removed
        else:
            book_side[event.price] = event.size
        self.timestamp = event.timestamp

class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

#consider making this more descriptive by adding more values example: subject, volume24hr, volume1hr, etc.
@dataclass
class MarketInfo:
    market_id: str
    exchange: str
    slug: str
    token_ids: Dict[str, str]
    active: bool
    tradable: Dict[str, bool]
    volume: float = 0.0
    prices: Dict[str, float] = field(default_factory=dict)

class OrderType(str, Enum):
    LIMIT = "ORDER_TYPE_LIMIT"
    MARKET = "ORDER_TYPE_MARKET"


class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"

class TimeInForce(str, Enum):
    GTC = "GTC"   # good till canceled
    GTD = "GTD"   # good till date
    IOC = "IOC"   # immediate or cancel
    FOK = "FOK"   # fill or kill

@dataclass
class OrderIntent:
    market_id: str
    exchange: str
    side: Side
    outcome: str
    size: float
    price: float
    strategy: str
    reason: str = ""
    tif: TimeInForce = TimeInForce.FOK
    order_type: OrderType = OrderType.LIMIT
    slippage: Optional[float] = None


@dataclass
class OrderRecord:
    order_id: str
    market_id: str
    exchange: str
    outcome: str
    side: Side
    size: float
    price: float
    status: OrderStatus = OrderStatus.NEW
    strategy: str = ""
    tif: TimeInForce = TimeInForce.FOK
    filled_quantity: Optional[float] = None


@dataclass
class Position:
    market_id: str
    exchange: str
    outcome: str
    size: float
    entry_price: float
    side: Side
    strategy: str

class TradingConfig:
    def __init__(self):
        #consider adding a strategies dict where we list the strateigies we want and specific details about them
        self.paper_trading: Dict[str, bool] = {}
        self.poll_interval_seconds: float = 1.0
        self.max_markets_per_subscription: int = 200
        self.max_open_positions: int = 25
        self.max_total_exposure: float = 1000.0
        self.max_position_size: float = 100.0
        self.stop_loss_pct: float = 10.0
        self.api_key: Optional[str] = None
        self.private_key: Optional[str] = None


@dataclass(frozen=True)
class Snapshot:
    timestamp: float
    markets: Dict[str, MarketInfo]              # from state.markets
    orderbooks: Dict[str, OrderBook]        # from state.orderbooks, keyed by market_id/outcome
    positions: Dict[str, Position]               # from state.positions
    open_orders: Dict[str, OrderRecord]          # from state.open_orders
    balance: Dict[str, float]                    # from state.balance
    exposure: float                              # derived, see below
    market_quotes: Dict[str, LiteOrderBookSnapshotEvent] = field(default_factory=dict)
