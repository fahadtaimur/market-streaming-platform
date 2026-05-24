"""Tests for redis_consumer.py: _process_bar and signal_key. Fixtures in conftest."""

from msp.consumer.redis_consumer import signal_key


def test_signal_key_format():
    assert signal_key("AAPL") == "signals:AAPL"
    assert signal_key("BTC/USD") == "signals:BTC/USD"


def test_consumer_process_bar_above_threshold(
    consumer, fake_redis, bar_above_threshold
):
    consumer._process_bar(symbol="AAPL", fields=bar_above_threshold, threshold=0.01)
    assert fake_redis.hgetall("signals:AAPL")["signal"] == "SHORT"


def test_consumer_process_bar_below_threshold(
    consumer, fake_redis, bar_below_threshold
):
    consumer._process_bar(symbol="AAPL", fields=bar_below_threshold, threshold=0.01)
    assert fake_redis.hgetall("signals:AAPL")["signal"] == "LONG"


def test_consumer_process_bar_flat(consumer, fake_redis, bar_flat):
    consumer._process_bar(symbol="AAPL", fields=bar_flat)
    assert fake_redis.hgetall("signals:AAPL")["signal"] == "FLAT"


def test_consumer_process_bar_zero_volume_no_signal(
    consumer, fake_redis, bar_zero_volume
):
    consumer._process_bar(symbol="AAPL", fields=bar_zero_volume)
    msg = fake_redis.hgetall("signals:AAPL")
    assert len(msg) == 0
