"""Shared fixtures for ingestion module tests."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import fakeredis
import pandas as pd

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


@pytest.fixture
def raw_metrics() -> pd.DataFrame:
    """Shaped like obb.equity.fundamental.metrics output, in yfinance's units."""
    return pd.DataFrame(
        {
            "symbol": ["AAA", "NEQ", "NODEBT", "GAP"],
            "market_cap": [5e9, 3e9, 8e9, 1e9],
            "debt_to_equity": [150.0, None, None, None],
            "dividend_yield": [2.5, 0.0, 0.0, 0.0],
            "book_value": [10.0, -4.0, 12.0, 6.0],
            "operating_margin": [0.12, 0.05, -0.08, 0.02],
            "revenue_growth": [0.04, 0.10, 0.30, 0.01],
        }
    )
