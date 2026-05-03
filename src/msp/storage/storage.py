"""Interface for storage related actions."""

from abc import ABC, abstractmethod
from datetime import date, datetime
from dataclasses import dataclass

import pandas as pd


@dataclass
class PriceBar:
    """Data contract for bar price storage."""

    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class PriceStorage(ABC):
    """Read/write interface for OHLCV price data. Implementations may use a SQL DB, Parquet, or a remote store."""

    @abstractmethod
    def write(self, df: pd.DataFrame) -> None:
        """Write OHLCV bars to storage. DataFrame must contain: symbol, timestamp, open, high, low, close, volume."""
        ...

    @abstractmethod
    def read(
        self,
        symbol: str,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
    ) -> pd.DataFrame:
        """Read OHLCV from storage for a single symbol. Omit dates to return all stored data."""
        ...

    @abstractmethod
    def read_many(
        self,
        symbols: list[str],
        start_date: date | str | None = None,
        end_date: date | str | None = None,
    ) -> pd.DataFrame:
        """Read OHLCV from storage for multiple symbols. Omit dates to return all stored data."""
        ...

    @abstractmethod
    def get_latest_date(self, symbol: str) -> date | None:
        """Get the latest date in storage for symbol to support change data capture operations."""
        ...

    @abstractmethod
    def exists(self, symbol: str) -> bool:
        """Determine if symbol exists in storage."""
        ...

    @abstractmethod
    def delete(self, symbol: str) -> None:
        """Delete all stored data for a symbol."""
        ...

    @abstractmethod
    def delete_many(self, symbols: list[str]) -> None:
        """Delete all stored data for each symbol in the list."""
        ...
