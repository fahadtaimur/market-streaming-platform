"""Interface for brokerage related actions."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Order:
    """A request to buy or sell a security.

    side: "buy" or "sell"
    order_type: "market" or "limit"
    limit_price: required when order_type is "limit", ignored otherwise
    """

    symbol: str
    qty: int
    side: str
    order_type: str
    limit_price: Decimal | None = None


@dataclass
class Position:
    """A currently held position in a single security."""

    symbol: str
    qty: int
    market_value: Decimal
    avg_entry_price: Decimal
    unrealized_pl: Decimal


@dataclass
class AccountInfo:
    """A snapshot of account-level metrics.

    cash: uninvested cash balance
    portfolio_value: cash + market value of all open positions
    buying_power: what can actually be spent today (may differ from cash due to margin)
    is_pattern_day_trader: accounts under $25K face PDT restrictions on round-trips
    """

    cash: Decimal
    portfolio_value: Decimal
    buying_power: Decimal
    is_pattern_day_trader: bool


class Broker(ABC):
    """Abstract interface for all broker interactions. Concrete implementations (AlpacaBroker, PaperBroker) must implement every method."""

    @abstractmethod
    def submit_order(self, order: Order) -> str:
        """Submit an order. Returns the broker-assigned order ID."""
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> None:
        """Cancel an open order using its ID."""
        ...

    @abstractmethod
    def get_order_status(self, order_id: str) -> str:
        """Return current order status: 'filled', 'cancelled', etc."""
        ...

    @abstractmethod
    def get_open_orders(self) -> list[Order]:
        """Return all orders not yet filled or cancelled."""
        ...

    @abstractmethod
    def get_positions(self) -> list[Position]:
        """Return all currently held positions."""
        ...

    @abstractmethod
    def get_position(self, symbol: str) -> Position | None:
        """Return the position for a single symbol or None if not held"""
        ...

    @abstractmethod
    def close_position(self, symbol: str) -> str:
        """Liquidate the entire position in a symbol. Returns the order ID."""
        ...

    @abstractmethod
    def close_all_positions(self) -> None:
        """Liquidate all open positions. Used by risk layer on a stop-out."""
        ...

    @abstractmethod
    def get_account(self) -> AccountInfo:
        """Return a snapshot of account-level metrics."""
        ...

    @abstractmethod
    def is_market_open(self) -> bool:
        """Returns True if the exchange is currently in regular trading hours."""
        ...
