"""Local in-memory implementation of brokerage related actions."""

import uuid
from decimal import Decimal

from msp.execution.broker import Order, Position, AccountInfo, Broker


class MemoryBroker(Broker):
    """
    In-memory broker for testing and dry-run mode.
    All orders fill immediately at the provided price.
    """

    def __init__(self, starting_cash: Decimal):
        self._cash = starting_cash
        self._positions: dict[str, Position] = {}
        self._filled_orders: dict[str, Order] = {}

    def submit_order(
        self,
        order: Order,
        current_prices: dict[str, Decimal] | None = None,
    ) -> str:
        """
        Fill an order immediately. Market orders require current_prices to contain the symbol.
        """
        order_id = str(uuid.uuid4())

        if order.order_type == "limit":
            price = order.limit_price
            if price is None:
                raise ValueError("limit_price must be set for limit orders")
        else:
            if current_prices is None or order.symbol not in current_prices:
                raise ValueError(
                    f"current_prices must contain '{order.symbol}' for market orders"
                )
            price = current_prices[order.symbol]

        if order.side == "buy":
            cost = price * order.qty
            if cost > self._cash:
                raise ValueError("Insufficient cash for order")
            self._cash -= cost

            if order.symbol in self._positions:
                existing = self._positions[order.symbol]
                total_qty = existing.qty + order.qty
                avg_price = (
                    (existing.avg_entry_price * existing.qty) + (price * order.qty)
                ) / total_qty
                self._positions[order.symbol] = Position(
                    symbol=order.symbol,
                    qty=total_qty,
                    market_value=price * total_qty,
                    avg_entry_price=avg_price,
                    unrealized_pl=(price - avg_price) * total_qty,
                )
            else:
                self._positions[order.symbol] = Position(
                    symbol=order.symbol,
                    qty=order.qty,
                    market_value=price * order.qty,
                    avg_entry_price=price,
                    unrealized_pl=Decimal("0"),
                )

        elif order.side == "sell":
            if order.symbol not in self._positions:
                raise ValueError(f"No positions in {order.symbol} to sell")

            existing = self._positions[order.symbol]
            if order.qty > existing.qty:
                raise ValueError(
                    f"Cannot sell {order.qty} shares as they exceed current qty of {existing.qty}"
                )
            self._cash += price * order.qty
            remaining = existing.qty - order.qty
            if remaining == 0:
                del self._positions[order.symbol]
            else:
                self._positions[order.symbol] = Position(
                    symbol=order.symbol,
                    qty=remaining,
                    market_value=price * remaining,
                    avg_entry_price=existing.avg_entry_price,
                    unrealized_pl=((price - existing.avg_entry_price) * remaining),
                )

        self._filled_orders[order_id] = order
        return order_id

    def cancel_order(self, order_id: str) -> None:
        """
        Not implemented for memory brokerage.
        All orders fill immediately in memory mode.
        """
        pass

    def get_order_status(self, order_id: str) -> str:
        """Return current order status: 'filled', 'cancelled', etc."""
        return "filled" if order_id in self._filled_orders else "not_found"

    def get_open_orders(self) -> list[Order]:
        """
        Not implemented for local brokerage.
        All orders fill synchronously in memory mode.
        """
        return []

    def get_positions(self) -> list[Position]:
        """Return all currently held positions."""
        return list(self._positions.values())

    def get_position(self, symbol: str) -> Position | None:
        """Return the position for a single symbol or None if not held"""
        return self._positions.get(symbol)

    def close_position(self, symbol: str) -> str:
        """Liquidate the entire position in a symbol. Returns the order ID."""
        if symbol not in self._positions:
            raise ValueError(f"No position in {symbol}")
        position = self._positions[symbol]
        last_price = position.market_value / position.qty
        return self.submit_order(
            Order(
                symbol=symbol,
                qty=position.qty,
                side="sell",
                order_type="limit",
                limit_price=last_price,
            )
        )

    def close_all_positions(self) -> None:
        """Liquidate all open positions. Used by risk layer on a stop-out."""
        for symbol in list(self._positions.keys()):
            self.close_position(symbol)

    def get_account(self) -> AccountInfo:
        """Return a snapshot of account-level metrics."""
        portfolio_value = self._cash + sum(
            p.market_value for p in self._positions.values()
        )
        return AccountInfo(
            cash=self._cash,
            portfolio_value=portfolio_value,
            buying_power=self._cash,
            is_pattern_day_trader=False,
        )

    def is_market_open(self) -> bool:
        """Returns True if the exchange is currently in regular trading hours."""
        return True
