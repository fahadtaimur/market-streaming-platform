"""Walk-forward evaluation engine for the MA cross strategy.

Sits at the top of the dependency graph — imports from every layer below
(storage, features, signals, backtesting) and nothing imports from it.

Typical usage::

    from pathlib import Path
    from msp.storage.parquet_storage import ParquetPriceStorage

    storage = ParquetPriceStorage(Path("data/bars_daily"))
    symbols = ["AAPL", "NVDA", "MSFT", "APLD"]

    result = evaluate_ticker("AAPL", storage, split_date="2024-01-01")
    candidates = evaluate_universe(symbols, storage, split_date="2024-01-01")

The ``candidates`` DataFrame is the hand-off to the streaming layer: each row
is a ticker that beat buy-and-hold on the test period, with the optimal SMA
windows the live signal pipeline should use.
"""

from __future__ import annotations

import pandas as pd

from .base import train_test_split
from .ma_cross import MACrossBacktester
from ..features.technical import add_sma
from ..signals.signals import add_sma_signal
from ..storage.parquet_storage import ParquetPriceStorage


def evaluate_ticker(
    symbol: str,
    storage: ParquetPriceStorage,
    split_date: str,
    sma_s_range: tuple[int, int, int] = (10, 60, 1),
    sma_l_range: tuple[int, int, int] = (100, 252, 1),
    initial_cash: float = 100_000,
) -> dict | None:
    """Run a single walk-forward evaluation for one ticker.

    Reads all stored daily bars for ``symbol``, splits at ``split_date``,
    optimises SMA windows on the training set, then evaluates the out-of-sample
    test period. Returns ``None`` if no data exists for the symbol.

    The caller is responsible for choosing a ``split_date`` that leaves enough
    bars on each side: a rule of thumb is at least 252 bars in training (one
    year) and 63 bars in test (one quarter).

    Args:
        symbol: Ticker symbol to evaluate (e.g. ``"AAPL"``).
        storage: Initialised ``ParquetPriceStorage`` pointed at the daily-bar
            directory (e.g. ``data/bars_daily/``).
        split_date: ISO date string (``"YYYY-MM-DD"``) that marks the first
            day of the out-of-sample test period.
        sma_s_range: ``(start, stop, step)`` grid for the short SMA window.
        sma_l_range: ``(start, stop, step)`` grid for the long SMA window.
        initial_cash: Starting portfolio value passed to ``MACrossBacktester``.

    Returns:
        Dict with keys ``symbol``, ``sma_s``, ``sma_l``, and the six summary
        metrics from ``MACrossBacktester.summary()`` (``total_return``,
        ``bnh_return``, ``beats_bnh``, ``sharpe``, ``max_drawdown``,
        ``n_trades``). Returns ``None`` if the symbol has no stored data.
    """
    df = storage.read(symbol=symbol)
    if df.empty:
        return None

    df = df.set_index("timestamp")
    train, test = train_test_split(df, split_date=split_date)
    if train.empty or test.empty:
        return None

    bt = MACrossBacktester(initial_cash=initial_cash)
    sma_s_opt, sma_l_opt = bt.optimize(
        df=train,
        sma_s_range=sma_s_range,
        sma_l_range=sma_l_range,
    )

    # Compute SMAs on the full series so the long window has enough warmup bars
    # from the training period, then slice back to the test window.
    df_with_sma = add_sma(df, periods=[sma_s_opt, sma_l_opt], labels=["sma_s", "sma_l"])
    test = add_sma_signal(df_with_sma.loc[test.index])
    bt.run(test)

    return {"symbol": symbol, "sma_s": sma_s_opt, "sma_l": sma_l_opt, **bt.summary()}


def evaluate_universe(
    symbols: list[str],
    storage: ParquetPriceStorage,
    split_date: str,
    sma_s_range: tuple[int, int, int] = (10, 60, 1),
    sma_l_range: tuple[int, int, int] = (100, 252, 1),
    initial_cash: float = 100_000,
) -> pd.DataFrame:
    """Evaluate a population of tickers and return streaming candidates.

    Calls ``evaluate_ticker`` for each symbol, silently skips symbols with no
    stored data, and returns a DataFrame sorted by ``total_return`` descending.
    The result is the direct input to the streaming layer: filter on
    ``beats_bnh == True`` to get the candidate list.

    Args:
        symbols: List of ticker symbols to evaluate.
        storage: Initialised ``ParquetPriceStorage`` pointed at the daily-bar
            directory (e.g. ``data/bars_daily/``).
        split_date: ISO date string passed through to ``evaluate_ticker``.
        sma_s_range: ``(start, stop, step)`` grid for the short SMA window.
        sma_l_range: ``(start, stop, step)`` grid for the long SMA window.
        initial_cash: Starting portfolio value passed to ``MACrossBacktester``.

    Returns:
        DataFrame with one row per symbol that had sufficient data. Columns are
        ``symbol``, ``sma_s``, ``sma_l``, ``total_return``, ``bnh_return``,
        ``beats_bnh``, ``sharpe``, ``max_drawdown``, ``n_trades``. Empty
        DataFrame if no symbols had stored data.
    """
    rows = []
    for symbol in symbols:
        try:
            result = evaluate_ticker(
                symbol=symbol,
                storage=storage,
                split_date=split_date,
                sma_s_range=sma_s_range,
                sma_l_range=sma_l_range,
                initial_cash=initial_cash,
            )
            if result is not None:
                rows.append(result)
        except Exception as e:
            print(f"{symbol}: skipped — {e}")
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        "total_return", ascending=False, ignore_index=True
    )
