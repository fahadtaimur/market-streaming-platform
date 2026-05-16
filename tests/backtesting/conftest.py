"""Shared fixtures for backtesting tests."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from msp.storage.parquet_storage import ParquetPriceStorage


@pytest.fixture
def store(tmp_path: Path) -> ParquetPriceStorage:
    return ParquetPriceStorage(base_path=tmp_path)


@pytest.fixture
def ohlcv_with_signal() -> pd.DataFrame:
    """300 bars of synthetic daily close prices with a pre-computed binary signal.

    Signal is 0 for the first half and 1 for the second, giving the backtester
    a non-trivial position to simulate without encoding a specific trade count.
    """
    n = 300
    rng = np.random.default_rng(0)
    close = 100 + np.cumsum(rng.normal(0, 1, n))
    signal = np.where(np.arange(n) > n // 2, 1, 0)
    return pd.DataFrame({"close": close, "signal": signal})


@pytest.fixture
def single_ticker_df() -> pd.DataFrame:
    """600 business-day OHLCV bars for a synthetic ticker."""
    n = 600
    rng = np.random.default_rng(42)
    close = 200 + np.cumsum(rng.normal(0, 1, n))
    spread = rng.uniform(0.5, 1.5, n)
    # Business days only to mimic daily bars
    dates = pd.date_range("2022-01-03", periods=n, freq="B", tz="UTC")
    return pd.DataFrame(
        {
            "symbol": "TEST",
            "timestamp": dates,
            "open": close - spread,
            "high": close + spread * 2,
            "low": close - spread * 2,
            "close": close,
            "volume": rng.integers(100_000, 500_000, n).astype(float),
        }
    )


@pytest.fixture
def stored_ticker(
    store: ParquetPriceStorage,
    single_ticker_df: pd.DataFrame,
) -> tuple[ParquetPriceStorage, str]:
    """Write single_ticker_df into the tmp store, returning (store, symbol)."""
    store.write(single_ticker_df)
    return store, "TEST"
