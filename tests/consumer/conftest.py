"""Shared fixtures for consumer module tests."""

import pytest
import fakeredis

from msp.consumer.redis_consumer import RedisConsumer


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def consumer(fake_redis):
    return RedisConsumer(symbols=["AAPL"], redis_client=fake_redis)


@pytest.fixture
def bar_zero_volume():
    return {
        "symbol": "AAPL",
        "close": "300.0",
        "volume": "0.0",
        "vwap": "302.0",
        "timestamp": "2026-01-15T13:30:00+00:00",
    }


@pytest.fixture
def bar_above_threshold():
    return {
        "symbol": "AAPL",
        "close": "310.0",
        "volume": "1.0",
        "vwap": "302.0",
        "timestamp": "2026-01-15T13:30:00+00:00",
    }


@pytest.fixture
def bar_below_threshold():
    return {
        "symbol": "AAPL",
        "close": "290.0",
        "volume": "1.0",
        "vwap": "302.0",
        "timestamp": "2026-01-15T13:30:00+00:00",
    }


@pytest.fixture
def bar_flat():
    return {
        "symbol": "AAPL",
        "close": "302.0",
        "volume": "1.0",
        "vwap": "302.0",
        "timestamp": "2026-01-15T13:30:00+00:00",
    }
