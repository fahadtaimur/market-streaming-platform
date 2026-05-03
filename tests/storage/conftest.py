"""Shares fixtures for storage module tests."""

from pathlib import Path

import pytest
import pandas as pd

from msp.storage.parquet_storage import ParquetPriceStorage


@pytest.fixture
def sample_bars_single() -> pd.DataFrame:
    """Sample OHLCV bars for a single symbol (AAPL) used in single-symbol read and write tests."""
    return pd.DataFrame(
        data={
            "symbol": ["AAPL", "AAPL", "AAPL"],
            "timestamp": pd.to_datetime(
                ["2026-01-01", "2026-01-02", "2026-02-01"], utc=True
            ),
            "open": [200.0, 205.5, 210.0],
            "high": [202.0, 206.5, 212.0],
            "low": [199.0, 201.3, 208.0],
            "close": [200.3, 205.5, 211.0],
            "volume": [1000, 2500, 1800],
        }
    )


@pytest.fixture
def sample_bars_multi() -> pd.DataFrame:
    """Sample OHLCV bars for multiple symbols (AAPL and MSFT) used in multi-symbol read and write tests."""
    return pd.DataFrame(
        data={
            "symbol": ["AAPL", "AAPL", "MSFT"],
            "timestamp": pd.to_datetime(
                ["2026-01-01", "2026-02-01", "2026-01-01"], utc=True
            ),
            "open": [200.0, 205.5, 400.2],
            "high": [202.0, 206.5, 403.2],
            "low": [199.0, 201.3, 399.5],
            "close": [200.3, 205.5, 400.1],
            "volume": [1000, 2500, 1500],
        }
    )


@pytest.fixture
def store(tmp_path: Path) -> ParquetPriceStorage:
    """Isolated ParquetPriceStorage instance backed by a pytest temporary directory, cleaned up after each test."""
    return ParquetPriceStorage(base_path=tmp_path)
