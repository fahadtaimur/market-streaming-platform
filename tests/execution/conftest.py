"""Shared fixtures for execution module tests."""

import pytest
from decimal import Decimal
from msp.execution.memory_broker import MemoryBroker
from msp.execution.broker import Order


@pytest.fixture
def broker():
    return MemoryBroker(starting_cash=10000)


@pytest.fixture
def market_buy_order():
    return Order(symbol="AAPL", qty=10, side="buy", order_type="market")


@pytest.fixture
def market_sell_order():
    return Order(symbol="AAPL", qty=10, side="sell", order_type="market")


@pytest.fixture
def limit_buy_order():
    return Order(
        symbol="AAPL",
        qty=10,
        side="buy",
        order_type="limit",
        limit_price=Decimal("180"),
    )
