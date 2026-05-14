"""Shared fixtures for backtesting module."""

import numpy as np
import pandas as pd
import pytest


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
