"""Alpaca WebSocket producer: streams live bars into Redis Streams."""

import logging
import os
import threading
from abc import ABC, abstractmethod

import redis
from alpaca.data.live import CryptoDataStream, StockDataStream
from alpaca.data.models import Bar
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

STREAM_KEY_PREFIX = "bars:live"


def stream_key(symbol: str) -> str:
    """Return the Redis stream key for a symbol.

    Args:
        symbol: ticker symbol, e.g. "BTC/USD"

    Returns:
        Redis key string, e.g. "bars:live:BTC/USD"
    """
    return f"{STREAM_KEY_PREFIX}:{symbol}"


class BaseStreamProducer(ABC):
    """Abstract base for Alpaca WebSocket producers.

    Handles the shared lifecycle: Redis connection, bar publishing, stream
    start/stop, and optional dev-mode consumer thread. Subclasses implement
    _create_stream() to return the appropriate Alpaca stream type.

    Args:
        symbols: list of symbols to subscribe to
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
            host=redis_host, port=redis_port
        )

    async def _on_bar(self, bar: Bar) -> None:
        """Publish an incoming bar to its Redis stream key.

        Called by the Alpaca stream once per bar per subscribed symbol.
        Writes symbol, close, volume, vwap, and timestamp as string fields
        via XADD. Processing (signals, positions) happens in the consumer,
        not here.

        Args:
            bar: Alpaca Bar object with symbol, close, volume, vwap, timestamp
        """
        fields: dict = {
            "symbol": bar.symbol,
            "timestamp": bar.timestamp.isoformat(),
            "close": str(bar.close),
            "volume": str(bar.volume),
            "vwap": str(bar.vwap),
        }
        self.redis_client.xadd(name=stream_key(bar.symbol), fields=fields)
        logger.info("published bar", extra=fields)

    @abstractmethod
    def _create_stream(self) -> CryptoDataStream | StockDataStream:
        """Instantiate and return the Alpaca stream for this producer type.

        Called once at the start of start(). Subclasses return the
        appropriate stream class (CryptoDataStream or StockDataStream)
        initialised with API credentials.
        """
        ...

    def start(self) -> None:
        """Subscribe to all symbols and start the WebSocket stream.

        Blocks the calling thread until stop() is called or an unrecoverable
        error occurs. Intended to be the main blocking call in a producer
        process or thread.
        """
        self._stream = self._create_stream()
        self._stream.subscribe_bars(self._on_bar, *self.symbols)  # type: ignore[arg-type]
        try:
            self._stream.run()
        except Exception:
            logger.exception("stream exited unexpectedly.")
            raise

    def stop(self) -> None:
        """Stop the WebSocket stream and signal the producer to shut down.

        Safe to call from a separate thread or a signal handler.
        Guards against being called before start() has created self._stream.
        """
        if hasattr(self, "_stream"):
            self._stream.stop()

    def _start_consumer_thread(
        self, symbol: str, stop_flag: threading.Event
    ) -> threading.Thread:
        """Spin up a background thread that tails the Redis stream for a symbol.

        Used in local dev to confirm bars are being published without needing
        a separate consumer process. Not used in production.

        Args:
            symbol: ticker symbol to tail, e.g. "BTC/USD"
            stop_flag: Event that signals the thread to exit its read loop

        Returns:
            The started daemon thread
        """

        def consume():
            last_id = "0"
            while not stop_flag.is_set():
                messages = self.redis_client.xread(
                    {stream_key(symbol): last_id}, count=1, block=500
                )
                if messages:
                    for _, entries in messages:
                        for msg_id, fields in entries:
                            logger.info("consumed bar", extra=fields)
                            last_id = msg_id

        t = threading.Thread(target=consume, daemon=True)
        t.start()
        return t


class CryptoStreamProducer(BaseStreamProducer):
    """Connects to Alpaca's crypto WebSocket and publishes each bar to Redis.

    Lifecycle:
        producer = CryptoStreamProducer(symbols=["BTC/USD", "ETH/USD"])
        producer.start()   # blocks until stopped
        producer.stop()    # call from another thread or signal handler

    Args:
        symbols: list of crypto symbols to subscribe to
        redis_host: Redis host, defaults to localhost
        redis_port: Redis port, defaults to 6379
        redis_client: injectable Redis client for testing (fakeredis)
    """

    def _create_stream(self) -> CryptoDataStream:
        return CryptoDataStream(
            api_key=os.environ["ALPACA_API_KEY"],
            secret_key=os.environ["ALPACA_SECRET_KEY"],
        )


class StockStreamProducer(BaseStreamProducer):
    """Alpaca equity WebSocket producer — publishes stock bars to Redis.

    Lifecycle:
        producer = StockStreamProducer(symbols=["AAPL", "NVDA"])
        producer.start()   # blocks until stopped
        producer.stop()    # call from another thread or signal handler

    Note: equity markets trade 9:30am-4pm ET on weekdays. The stream
    sits idle outside market hours — bars only arrive during open sessions.

    Args:
        symbols: list of equity ticker symbols to subscribe to
        redis_host: Redis host, defaults to localhost
        redis_port: Redis port, defaults to 6379
        redis_client: injectable Redis client for testing (fakeredis)
    """

    def _create_stream(self) -> StockDataStream:
        return StockDataStream(
            api_key=os.environ["ALPACA_API_KEY"],
            secret_key=os.environ["ALPACA_SECRET_KEY"],
        )
