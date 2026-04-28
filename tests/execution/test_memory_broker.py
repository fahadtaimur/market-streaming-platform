"""Tests for MemoryBroker. Fixtures in conftest."""

from msp.execution.broker import Order
import pytest
from decimal import Decimal

CURRENT_PRICES = {"AAPL": Decimal("200")}


def test_market_buy_order_reduces_cash(broker, market_buy_order):
    # Purchase 10 shares @ $200, $2000 deduced from cash
    broker.submit_order(order=market_buy_order, current_prices=CURRENT_PRICES)
    assert broker.get_account().cash == Decimal("8000")


def test_market_buy_order_avg_price(broker, market_buy_order):
    # Average price of two market buy orders should be a weighted avg
    broker.submit_order(order=market_buy_order, current_prices=CURRENT_PRICES)
    broker.submit_order(order=market_buy_order, current_prices={"AAPL": Decimal("210")})
    assert broker.get_position("AAPL").avg_entry_price == Decimal("205")


def test_market_sell_order_increases_cash(broker, market_buy_order, market_sell_order):
    broker.submit_order(order=market_buy_order, current_prices=CURRENT_PRICES)
    broker.submit_order(order=market_sell_order, current_prices=CURRENT_PRICES)
    # bought @ 200, sold @ 200 → back to 10000
    assert broker.get_account().cash == Decimal("10000")


def test_market_buy_order_creates_position(broker, market_buy_order):
    # Test position of a ticker after purchase
    broker.submit_order(order=market_buy_order, current_prices=CURRENT_PRICES)
    position = broker.get_position(symbol="AAPL")
    assert position is not None
    assert position.qty == 10
    assert position.avg_entry_price == Decimal("200")


def test_insufficient_cash_raises_value_error(broker):
    with pytest.raises(ValueError, match="Insufficient cash for order"):
        broker.submit_order(
            order=Order(symbol="AAPL", qty=1000, side="buy", order_type="market"),
            current_prices={"AAPL": Decimal("1000")},
        )


def test_market_sell_symbol_not_in_position(broker):
    with pytest.raises(ValueError, match="No positions in APLD to sell"):
        broker.submit_order(
            order=Order(symbol="APLD", qty=1000, side="sell", order_type="market"),
            current_prices={"APLD": Decimal("30")},
        )


def test_account_totals(broker, market_buy_order, market_sell_order):
    broker.submit_order(order=market_buy_order, current_prices=CURRENT_PRICES)
    assert broker.get_account().cash == Decimal("8000")
    assert broker.get_account().portfolio_value == Decimal("10000")
    assert broker.get_account().buying_power == Decimal("8000")


def test_limit_buy_uses_limit_price(broker, limit_buy_order):
    broker.submit_order(order=limit_buy_order)
    position = broker.get_position("AAPL")
    assert position.avg_entry_price == Decimal("180")


def test_limit_order_without_price_raises(broker):
    with pytest.raises(ValueError, match="limit_price must be set"):
        broker.submit_order(
            order=Order(
                symbol="AAPL", qty=10, side="buy", order_type="limit", limit_price=None
            )
        )


def test_close_position_removes_it(broker, market_buy_order):
    broker.submit_order(order=market_buy_order, current_prices=CURRENT_PRICES)
    broker.close_position("AAPL")
    assert broker.get_position("AAPL") is None


def test_close_all_positions_clears_everything(broker, market_buy_order):
    broker.submit_order(order=market_buy_order, current_prices=CURRENT_PRICES)
    broker.submit_order(
        order=Order(symbol="MSFT", qty=5, side="buy", order_type="market"),
        current_prices={"MSFT": Decimal("400")},
    )
    broker.close_all_positions()
    assert broker.get_positions() == []
