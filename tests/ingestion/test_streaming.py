"""Tests for streaming.py: _on_bar and stream_key. Fixtures in conftest."""

from datetime import datetime, timezone

import pytest

from msp.ingestion.streaming import stream_key


def test_stream_key_format():
    assert stream_key("BTC/USD") == "bars:live:BTC/USD"
    assert stream_key("AAPL") == "bars:live:AAPL"


def test_stop_before_start_does_not_raise(producer):
    producer.stop()


@pytest.mark.asyncio
async def test_on_bar_writes_to_correct_stream_key(producer, make_bar, fake_redis):
    bar = make_bar()
    await producer._on_bar(bar)
    assert fake_redis.xlen("bars:live:AAPL") == 1


@pytest.mark.asyncio
async def test_multi_symbol_on_bar_write_to_separate_stream_keys(
    producer, make_bar, fake_redis
):
    for bar in (make_bar(symbol="MSFT"), make_bar(symbol="GOOG")):
        await producer._on_bar(bar)

    assert fake_redis.xlen("bars:live:MSFT") == 1
    assert fake_redis.xlen("bars:live:GOOG") == 1


@pytest.mark.asyncio
async def test_stream_accumulates_multiple_bars(producer, make_bar, fake_redis):
    bar1 = make_bar(
        symbol="AAPL", timestamp=datetime(2026, 1, 15, 13, 30, tzinfo=timezone.utc)
    )
    bar2 = make_bar(
        symbol="AAPL", timestamp=datetime(2026, 1, 15, 13, 31, tzinfo=timezone.utc)
    )

    for bar in (bar1, bar2):
        await producer._on_bar(bar)

    assert fake_redis.xlen("bars:live:AAPL") == 2


@pytest.mark.asyncio
async def test_on_bar_all_fields_present_and_correct(producer, make_bar, fake_redis):
    bar = make_bar()
    await producer._on_bar(bar)
    _, fields = fake_redis.xrange("bars:live:AAPL")[0]

    EXPECTED_FIELDS = {"symbol", "timestamp", "close", "volume", "vwap"}
    assert set(fields.keys()) == EXPECTED_FIELDS
    assert fields["symbol"] == bar.symbol
    assert fields["timestamp"] == bar.timestamp.isoformat()
    assert float(fields["close"]) == bar.close
    assert float(fields["volume"]) == bar.volume
    assert float(fields["vwap"]) == bar.vwap
