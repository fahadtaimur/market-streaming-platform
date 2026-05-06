"""Shared fixtures for technical indicators module"""

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def ohlcv() -> pd.DataFrame:
    """
    50 bars of synthetic OHLCV data seeded for reproducibility.
    50 bars is enough for all indicators to warm up, including
    MACD (slow=26, signal=9).
    """
    n = 50
    rng = np.random.default_rng(42)

    close = 100 + np.cumsum(rng.normal(0, 1, n))
    high = close + rng.uniform(0.1, 2.0, n)
    low = close - rng.uniform(0.1, 2.0, n)
    open_ = low + rng.uniform(0, 1, n) * (high - low)
    volume = rng.integers(10_000, 100_000, n).astype(float)

    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": volume}
    )


@pytest.fixture
def ohlcv_intraday(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Same OHLCV data with a timezone-aware DatetimeIndex for Volume-Weighted Average Price testing."""
    return ohlcv.set_index(
        pd.date_range(
            start="2026-01-02 09:30",
            periods=len(ohlcv),
            freq="1min",
            tz="America/New_York",
        )
    )
