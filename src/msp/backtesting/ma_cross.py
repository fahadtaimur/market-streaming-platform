"""Moving average crossover backtester."""

import numpy as np
import pandas as pd
import scipy.optimize

from .base import Backtester, compute_signal_returns, compute_buy_and_hold_return
from msp.features.technical import add_sma
from msp.signals.signals import add_sma_signal


class MACrossBacktester(Backtester):
    """Backtester for a golden cross / death cross SMA strategy.

    Args:
        initial_cash: Starting portfolio value in dollars (default 100,000).
    """

    def __init__(self, initial_cash: float = 100_000):
        self._initial_cash = initial_cash
        self._results: pd.DataFrame | None = None
        self._signal_col: str = "signal"

    def _sma_cumulative_return(self, df: pd.DataFrame, sma_s: int, sma_l: int) -> float:
        """Compute cumulative strategy return for a given SMA window pair.

        Args:
            df: OHLCV DataFrame with a ``close`` column.
            sma_s: Short SMA window.
            sma_l: Long SMA window.

        Returns:
            Cumulative return as a multiplier (e.g. 1.25 = 25% gain).
        """
        df = df.copy()
        df = add_sma(df, periods=[sma_s, sma_l], labels=["sma_s", "sma_l"])
        df = add_sma_signal(df)
        df["signal_return"] = compute_signal_returns(df["close"], df["signal"])
        return np.exp(df["signal_return"].dropna().sum()).item()

    def optimize(
        self,
        df: pd.DataFrame,
        sma_s_range: tuple[int, int, int] = (10, 60, 1),
        sma_l_range: tuple[int, int, int] = (100, 252, 1),
    ) -> tuple[int, int]:
        """Brute-force search for the SMA window pair that maximises cumulative return.

        Searches the full Cartesian product of ``sma_s_range`` and ``sma_l_range``
        and returns the integer window pair with the highest strategy return.

        Pass training data only; never the full dataset, to avoid lookahead bias.
        ``finish=None`` keeps results on the integer grid rather than running a
        float polish step after the search.

        Args:
            df: Training-period OHLCV DataFrame.
            sma_s_range: ``(start, stop, step)`` for the short window grid.
            sma_l_range: ``(start, stop, step)`` for the long window grid.

        Returns:
            Tuple of ``(sma_s, sma_l)`` optimal window lengths.
        """

        def _objective(windows: tuple) -> float:
            return -self._sma_cumulative_return(
                df, sma_s=int(windows[0]), sma_l=int(windows[1])
            )

        result = scipy.optimize.brute(
            func=_objective,
            ranges=(sma_s_range, sma_l_range),
            finish=None,
        )
        return int(result[0]), int(result[1])

    def run(
        self,
        df: pd.DataFrame,
        signal_col: str = "signal",
    ) -> pd.DataFrame:
        """Simulate the strategy on ``df`` and return an annotated equity curve.

        Expects ``df`` to already have SMA columns and ``signal_col`` computed.
        Run ``add_sma`` and ``add_sma_signal`` before calling this method.
        Pass test-period data only; optimization and train/test splitting are
        the caller's responsibility.

        Args:
            df: Test-period OHLCV DataFrame with a pre-computed signal column.
            signal_col: Name of the 0/1 signal column (default ``"signal"``).

        Returns:
            Copy of ``df`` with ``signal_return``, ``strategy_equity``,
            ``bnh_return``, and ``bnh_equity`` columns appended.
        """
        df = df.copy()
        self._signal_col = signal_col
        df["signal_return"] = compute_signal_returns(df["close"], df[signal_col])
        df["strategy_equity"] = self._initial_cash * np.exp(
            df["signal_return"].cumsum()
        )
        df["bnh_return"] = np.log(df["close"] / df["close"].shift(1))
        df["bnh_equity"] = self._initial_cash * np.exp(df["bnh_return"].cumsum())

        self._results = df
        return df

    def summary(self) -> dict:
        """Return performance metrics for the most recent ``run`` call.

        Compares the strategy equity curve against buy-and-hold over the same
        period. Sharpe is annualised assuming 252 trading days.

        Returns:
            Dict with ``total_return``, ``bnh_return``, ``beats_bnh``,
            ``sharpe``, ``max_drawdown``, and ``n_trades``.

        Raises:
            RuntimeError: If called before ``run``.
        """
        if self._results is None:
            raise RuntimeError("Call run() before summary().")

        df = self._results.dropna(subset=["signal_return"])
        total_return = np.exp(df["signal_return"].sum()).item()
        bnh_return = compute_buy_and_hold_return(df["close"])

        equity = df["strategy_equity"]
        max_drawdown = float(((equity - equity.cummax()) / equity.cummax()).min())

        daily = df["signal_return"]
        sharpe = (
            float(daily.mean() / daily.std() * np.sqrt(252)) if daily.std() > 0 else 0.0
        )

        n_trades = int((df[self._signal_col].diff() == 1).sum())

        return {
            "total_return": round(total_return, 4),
            "bnh_return": round(bnh_return, 4),
            "beats_bnh": total_return > bnh_return,
            "sharpe": round(sharpe, 4),
            "max_drawdown": round(max_drawdown, 4),
            "n_trades": n_trades,
        }
