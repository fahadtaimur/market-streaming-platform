"""Shared fixtures for ingestion module tests."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import fakeredis

from msp.ingestion.streaming import CryptoStreamProducer, StockStreamProducer


@pytest.fixture
def make_bar():
    """Factory fixture"""

    def _make(
        symbol="AAPL",
        close=300.00,
        volume=2.0,
        vwap=302.00,
        timestamp=datetime(2026, 1, 15, 13, 30, tzinfo=timezone.utc),
    ):
        bar = MagicMock()
        bar.symbol = symbol
        bar.close = close
        bar.volume = volume
        bar.vwap = vwap
        bar.timestamp = timestamp
        return bar

    return _make


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def producer(fake_redis):
    return StockStreamProducer(symbols=["AAPL", "MSFT"], redis_client=fake_redis)


@pytest.fixture
def producer_crypto(fake_redis):
    return CryptoStreamProducer(symbols=["BTC/USD", "ETH/USD"], redis_client=fake_redis)
