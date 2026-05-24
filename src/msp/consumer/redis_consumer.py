"""Redis Stream consumer — reads live bars, computes VWAP-deviation signals, writes results back."""

import logging
import threading

import redis

logger = logging.getLogger(__name__)

PUBLISHER_STREAM_KEY_PREFIX = "bars:live"
SIGNAL_KEY_PREFIX = "signals"


def stream_key(symbol: str) -> str:
    """Return the Redis stream key for a symbol.

    Args:
        symbol: ticker symbol, e.g. "BTC/USD"

    Returns:
        Redis key string, e.g. "bars:live:BTC/USD"
    """
    return f"{PUBLISHER_STREAM_KEY_PREFIX}:{symbol}"


def signal_key(symbol: str) -> str:
    """Return the Redis key for a symbol's latest signal.

    Args:
        symbol: ticker symbol, e.g. "BTC/USD"

    Returns:
        Redis key string, e.g. "signals:BTC/USD"
    """
    return f"{SIGNAL_KEY_PREFIX}:{symbol}"


class RedisConsumer:
    """Reads live bars from Redis Streams and publishes VWAP-deviation signals.

    On each bar: skips zero-volume bars, computes the deviation of close from
    VWAP, derives a LONG/SHORT/FLAT signal against a configurable threshold,
    and writes results to a Redis Hash.

    Args:
        symbols: list of symbols to consume
        redis_host: Redis host, defaults to localhost
        redis_port: Redis port, defaults to 6379
        redis_client: injectable Redis client for testing (fakeredis)
    """

    def __init__(
        self,
        symbols: list[str],
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_client: redis.Redis | None = None,
    ) -> None:
        self.symbols = symbols
        self.redis_client = redis_client or redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True
        )
        self.positions: dict[str, dict] = {
            sym: {"signal": "FLAT", "entry": None} for sym in symbols
        }
        self._stop_flag = threading.Event()

    def _process_bar(self, symbol: str, fields: dict, threshold: float = 0.003) -> None:
        """Compute a VWAP-deviation signal for one bar and write it to Redis.

        Skips bars with zero volume (auction prints, stale ticks). Signal is
        SHORT when price is significantly above VWAP (fade the rally), LONG
        when significantly below (fade the sell-off), and FLAT otherwise.

        Args:
            symbol: ticker symbol, e.g. "BTC/USD"
            fields: raw bar fields from the Redis stream (string values)
            threshold: fractional VWAP deviation that triggers a signal, defaults to 0.003 (0.3%)
        """
        if float(fields["volume"]) == 0.0:
            return
        timestamp = fields["timestamp"]
        close = float(fields["close"])
        vwap = float(fields["vwap"])
        vwap_deviation = (close - vwap) / vwap
        if vwap_deviation > threshold:
            signal = "SHORT"
        elif vwap_deviation < -threshold:
            signal = "LONG"
        else:
            signal = "FLAT"
        self.redis_client.hset(
            name=signal_key(symbol),
            mapping={
                "timestamp": timestamp,
                "close": str(close),
                "vwap_dev": str(round(vwap_deviation, 6)),
                "signal": signal,
            },
        )
        logger.info(
            "[%s]: symbol=%s, close=%.2f, signal=%s", timestamp, symbol, close, signal
        )

    def run(self) -> None:
        cursors = {stream_key(symbol): "0" for symbol in self.symbols}
        while not self._stop_flag.is_set():
            messages = self.redis_client.xread(cursors, count=1, block=500)  # type: ignore[arg-type]
            if not messages:
                continue
            for stream_name, entries in messages:
                for msg_id, fields in entries:
                    print(fields)
                    symbol = fields["symbol"]
                    self._process_bar(symbol, fields)
                    cursors[stream_name] = msg_id

    def stop(self) -> None:
        self._stop_flag.set()
