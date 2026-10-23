from dataclasses import dataclass, field
from typing import Optional, Dict, List, Iterator
from enum import Enum
from itertools import chain
import time
import math
from uuid import uuid4

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
class AccountConnectionEvent(PrivateEvent):
    connected: bool

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
    balances: Dict[str, "AccountBalance"] = field(default_factory=dict)

@dataclass(frozen=True)
class BalanceUpdateEvent(PrivateEvent):
    """A normalized account balance change."""
    currency: str
    balance: "AccountBalance"

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
    category: str = "other"
    resolution_date: Optional[float] = None
    currency: str = "USD"
    tick_size: Optional[float] = None
    quantity_step: Optional[float] = None
    min_quantity: Optional[float] = None
    min_notional: float = 0.0
    # Conservative maximum fee per contract, in dollars. None means unknown.
    fee_per_contract: Optional[float] = None

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

@dataclass(frozen=True, kw_only=True)
class OrderIntent:
    """An unsized signal. Probability refers to the specified outcome."""
    market_id: str
    exchange: str
    side: Side
    outcome: str
    price: float
    strategy: str
    estimated_probability: Optional[float] = None
    resolution_date: Optional[float] = None
    intent_id: str = field(default_factory=lambda: str(uuid4()))
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    reduce_only: bool = False
    close_fraction: float = 1.0
    reason: str = ""
    tif: TimeInForce = TimeInForce.FOK
    order_type: OrderType = OrderType.LIMIT
    slippage: Optional[int] = None  # maximum ticks from price for market orders
    good_till_date: Optional[float] = None


class OrderNotSubmittedError(Exception):
    """An adapter knows that it never dispatched an exchange submission."""

@dataclass(frozen=True, kw_only=True)
class ApprovedOrder:
    intent: OrderIntent
    portfolio_id: str
    category: str
    size: float
    price: float  # worst permitted execution price
    fee_per_contract: float
    approved_until: float
    batch_id: str = ""

    @property
    def commitment(self) -> float:
        if self.side != Side.BUY:
            return 0.0
        return self.size * (self.price + self.fee_per_contract)

    @property
    def remaining_quantity(self) -> float:
        """An approval has not been submitted, so its entire size is pending."""
        return self.size

    @property
    def market_id(self) -> str:
        return self.intent.market_id

    @property
    def exchange(self) -> str:
        return self.intent.exchange

    @property
    def outcome(self) -> str:
        return self.intent.outcome

    @property
    def side(self) -> Side:
        return self.intent.side

    @property
    def strategy(self) -> str:
        return self.intent.strategy

    @property
    def intent_id(self) -> str:
        return self.intent.intent_id

    @property
    def tif(self) -> TimeInForce:
        return self.intent.tif

    @property
    def order_type(self) -> OrderType:
        return self.intent.order_type

    @property
    def slippage(self) -> Optional[int]:
        return self.intent.slippage

    @property
    def good_till_date(self) -> Optional[float]:
        return self.intent.good_till_date


@dataclass(frozen=True, kw_only=True)
class OrderIntentBatch:
    legs: List[OrderIntent]
    quantity_ratios: List[int]
    capital_ceiling: float
    batch_id: str = field(default_factory=lambda: str(uuid4()))
    expires_at: Optional[float] = None
    execution_policy: str = "assess_only"  # paper_all_or_none is also supported


TERMINAL_ORDER_STATUSES = frozenset({
    OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED,
})

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
    portfolio_id: str = "LIVE"
    category: str = "other"
    intent_id: str = ""
    batch_id: str = ""
    fee_per_contract: float = 0.0
    risk_price: Optional[float] = None
    average_fill_price: Optional[float] = None
    # Total fees reported by the venue; used for P&L when available.
    fees_paid: Optional[float] = None

    @property
    def commitment(self) -> float:
        """USD still reserved by the unfilled part of an entry order."""
        if self.side != Side.BUY:
            return 0.0
        return self.remaining_quantity * (self.commitment_price + self.fee_per_contract)

    @property
    def commitment_price(self) -> float:
        return self.risk_price if self.risk_price is not None else self.price

    @property
    def remaining_quantity(self) -> float:
        if self.status in TERMINAL_ORDER_STATUSES:
            return 0.0
        return max(0.0, self.size - (self.filled_quantity or 0.0))

@dataclass
class Position:
    market_id: str
    exchange: str
    outcome: str
    size: float
    entry_price: float
    side: Side
    strategy: str
    portfolio_id: str = "LIVE"
    category: str = "other"

@dataclass
class AccountBalance:
    """USD account amounts; cash includes order-held cash, excludes positions.

    Adapters must leave cash=None when that meaning cannot be established.
    Buying power is already net of exchange holds for live accounts.
    """
    buying_power: float
    cash: Optional[float] = None
    timestamp: float = field(default_factory=time.time)
    buying_power_includes_holds: bool = True
    current_balance: Optional[float] = None  # unmodified venue value
    reserved_cash: Optional[float] = None
    source_timestamp: Optional[float] = None

    def __post_init__(self):
        amount_fields = (
            "buying_power", "cash", "timestamp", "current_balance",
            "reserved_cash", "source_timestamp",
        )
        for name in amount_fields:
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not math.isfinite(value) or value < 0):
                raise ValueError(f"Invalid account {name}")

@dataclass
class RiskConfig:
    """Portfolio limits in USD; Kelly/category fractions use 0–1, loss limits 0–100."""
    long_term_expiration_minimum: float = 604_800
    kelly_criterion: Dict[str, float] = field(
        default_factory=lambda: {"long_term": 0.25, "short_term": 0.5}
    )
    diversification: Dict[str, float] = field(
        default_factory=lambda: {
            "crypto": 0.25, "politics": 0.25, "sports": 0.25, "other": 0.25,
        }
    )
    max_open_positions: int = 25
    max_total_exposure: float = 1000.0
    max_position_exposure: float = 100.0
    stop_loss_pct: float = 10.0
    max_drawdown_pct: Optional[float] = None
    capital_allocation: Optional[float] = None
    max_signal_age_seconds: float = 30.0
    max_quote_age_seconds: float = 5.0
    max_account_age_seconds: float = 30.0
    approval_ttl_seconds: float = 2.0
    currency: str = "USD"

    def validate(self) -> None:
        """Reject invalid settings before a portfolio can start sizing orders."""
        positive_settings = (
            "max_signal_age_seconds", "max_quote_age_seconds",
            "max_account_age_seconds", "approval_ttl_seconds",
        )
        nonnegative_settings = (
            "long_term_expiration_minimum", "max_total_exposure",
            "max_position_exposure", "stop_loss_pct",
        )
        for name in positive_settings + nonnegative_settings:
            value = getattr(self, name)
            invalid_number = isinstance(value, bool) or not math.isfinite(value) or value < 0
            zero_not_allowed = name in positive_settings and value == 0
            if invalid_number or zero_not_allowed:
                raise ValueError(f"Invalid risk setting: {name}")
        if type(self.max_open_positions) is not int or self.max_open_positions < 0:
            raise ValueError("max_open_positions must be a nonnegative integer")
        for name in ("capital_allocation", "max_drawdown_pct"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not math.isfinite(value) or value < 0):
                raise ValueError(f"Invalid risk setting: {name}")
        if self.stop_loss_pct > 100 or (self.max_drawdown_pct is not None and self.max_drawdown_pct > 100):
            raise ValueError("Loss percentages must be between 0 and 100")
        if set(self.kelly_criterion) != {"long_term", "short_term"}:
            raise ValueError("Both Kelly horizons are required")
        fractions = list(self.kelly_criterion.values()) + list(self.diversification.values())
        if any(
            isinstance(fraction, bool) or not math.isfinite(fraction) or not 0 <= fraction <= 1
            for fraction in fractions
        ):
            raise ValueError("Kelly and category fractions must be in [0, 1]")
        if "other" not in self.diversification or sum(self.diversification.values()) > 1 + 1e-12:
            raise ValueError("Categories require other and cannot allocate more than 100%")
        if any(not isinstance(category, str) or not category for category in self.diversification):
            raise ValueError("Category names must be nonempty strings")
        if self.currency != "USD":
            raise ValueError("Only USD is supported")


class TradingConfig:
    def __init__(self):
        self.paper_trading: Dict[str, bool] = {}
        self.strategy_portfolios: Dict[str, str] = {}
        self.risk_configs: Dict[str, RiskConfig] = {"LIVE": RiskConfig(), "PAPER": RiskConfig()}
        self.paper_starting_balances: Dict[str, Dict[str, float]] = {}
        self.market_overrides: Dict[str, Dict] = {}
        self.poll_interval_seconds = 1.0
        self.max_markets_per_subscription = 200
        self.api_key: Optional[str] = None
        self.private_key: Optional[str] = None

@dataclass
class PortfolioState:
    """One portfolio's ledger, submission history, readiness, and performance.

    Account positions determine exposure. strategy_positions records ownership
    of that same inventory and is never added to account exposure a second time.
    """
    portfolio_id: str
    paper: bool

    # Balances and active inventory/commitments.
    balances: Dict[str, Dict[str, AccountBalance]] = field(default_factory=dict)
    positions: Dict[str, Position] = field(default_factory=dict)
    strategy_positions: Dict[str, Dict[str, Position]] = field(default_factory=dict)
    open_orders: Dict[str, OrderRecord] = field(default_factory=dict)
    reservations: Dict[str, ApprovedOrder] = field(default_factory=dict)
    # History and IDs prevent replayed signals or fills from changing cash twice.
    order_history: Dict[str, OrderRecord] = field(default_factory=dict)
    # Execution deltas in accounting order, retained for late cost corrections.
    strategy_fills: List[OrderRecord] = field(default_factory=list)
    processed_batches: set = field(default_factory=set)
    processed_intents: set = field(default_factory=set)
    unresolved_intents: set = field(default_factory=set)
    filled_by_order: Dict[str, float] = field(default_factory=dict)
    fill_notional_by_order: Dict[str, float] = field(default_factory=dict)
    fees_by_order: Dict[str, float] = field(default_factory=dict)
    seen_executions: set = field(default_factory=set)
    # Readiness and revisions protect against incomplete live account snapshots.
    positions_updated_at: Dict[str, float] = field(default_factory=dict)
    disconnected_exchanges: set = field(default_factory=set)
    positions_ready: set = field(default_factory=set)
    orders_ready: set = field(default_factory=set)
    account_revisions: Dict[str, int] = field(default_factory=dict)
    dirty_exchanges: set = field(default_factory=set)
    # Performance is kept independently for each paper or live portfolio.
    equity_high_water: float = 0.0
    realized_pnl: float = 0.0
    net_cash_flows: float = 0.0
    entries_halted: bool = False

    @property
    def requires_reconciliation(self) -> bool:
        """Whether incomplete live account information prevents new risk."""
        return bool(self.dirty_exchanges or self.disconnected_exchanges or self.unresolved_intents)

    @property
    def pending_orders(self) -> Iterator[OrderRecord | ApprovedOrder]:
        """Iterate exchange orders and local approvals without copying them."""
        return chain(self.open_orders.values(), self.reservations.values())

@dataclass(frozen=True)
class Exposure:
    total: float
    by_market: Dict[str, float]
    by_category: Dict[str, float]
    by_allocation: Dict[str, float]
    market_slots: frozenset

@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reason: str
    orders: List[ApprovedOrder] = field(default_factory=list)
    sizing: Dict[str, float] = field(default_factory=dict)
    limiting_budget: str = ""

@dataclass(frozen=True)
class SubmissionResult:
    decision: RiskDecision
    records: List[OrderRecord] = field(default_factory=list)
    status: str = "rejected"

@dataclass(frozen=True)
class Snapshot:
    timestamp: float
    markets: Dict[str, MarketInfo]
    orderbooks: Dict[str, OrderBook]
    portfolios: Dict[str, PortfolioState]
    market_quotes: Dict[str, LiteOrderBookSnapshotEvent] = field(default_factory=dict)
