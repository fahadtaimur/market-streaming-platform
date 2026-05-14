"""Abstract base class for all backtester implementations."""

from abc import ABC, abstractmethod
from datetime import date

import numpy as np
import pandas as pd


class Backtester(ABC):
    """Interface that all backtester implementations must fulfil.

    Subclasses own their signal logic and state (cash, positions, trade log).
    Anything downstream (parameter optimisation, candidate filtering, the risk
    layer) interacts only with this interface.

    Typical usage::

        bt = MACrossBacktester(initial_cash=100_000)
        bt.run(df, signal_col="signal")
        metrics = bt.summary()
    """

    @abstractmethod
    def run(self, df: pd.DataFrame, signal_col: str) -> pd.DataFrame:
        """Simulate the strategy on ``df`` and return an annotated equity curve.

        Args:
            df: OHLCV DataFrame with a pre-computed signal column.
            signal_col: Name of the column containing the 0/1 signal.

        Returns:
            Copy of ``df`` with equity curve, position, and trade columns appended.
        """
        ...

    @abstractmethod
    def summary(self) -> dict:
        """Return performance metrics for the most recent ``run`` call.

        Returns:
            Dict containing at minimum: ``total_return``, ``bnh_return``,
            ``beats_bnh``, ``sharpe``, ``max_drawdown``, and ``n_trades``.
        """
        ...


def train_test_split(
    df: pd.DataFrame,
    split_date: str | date,
    tz: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a time-indexed DataFrame into train and test sets at ``split_date``.

    ``split_date`` belongs to the test set. All rows before it form the training
    set. SMAs and other indicators should be computed on the full DataFrame before
    splitting so the training warmup period does not bleed into the test set.

    For intraday DataFrames with a timezone-aware index, pass ``tz`` to avoid a
    comparison error between tz-aware and tz-naive timestamps.

    Args:
        df: Time-indexed DataFrame to split.
        split_date: First date of the test set (e.g. ``"2025-01-01"``).
        tz: Optional timezone string (e.g. ``"America/New_York"``). Only needed
            when ``df`` has a timezone-aware DatetimeIndex.

    Returns:
        Tuple of ``(train, test)`` DataFrames. Both are independent slices;
        mutating either does not affect the original.
    """
    ts = pd.Timestamp(split_date, tz=tz) if tz else pd.Timestamp(split_date)
    train = df.loc[df.index < ts]
    test = df.loc[df.index >= ts]
    return train, test


def compute_signal_returns(prices: pd.Series, signal: pd.Series) -> pd.Series:
    """Compute log returns scaled by the prior bar's signal.

    Args:
        prices: Price series (typically ``close``) for one symbol.
        signal: Integer signal series (1 = long, 0 = flat) aligned to ``prices``.

    Returns:
        Series of signal-scaled log returns, ready to be exponentiated into a
        cumulative return or summed for the equity curve.
    """
    log_returns = np.log(prices / prices.shift(1))
    return signal.shift(1) * log_returns


def compute_buy_and_hold_return(prices: pd.Series) -> float:
    """Compute the cumulative buy-and-hold return for a price series.

    Measures what a passive investor would have earned by buying on the first
    bar and holding through the last, with no trading. Used as the benchmark
    that every strategy return is compared against in ``summary()``.

    Args:
        prices: Price series (typically ``close``) for one symbol.

    Returns:
        Cumulative return as a multiplier (e.g. 1.25 = 25% gain).
    """
    ratio = prices.div(prices.shift(1)).dropna()
    return np.exp(np.log(ratio).sum()).item()
