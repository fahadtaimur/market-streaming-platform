"""Signal functions that operate on indicator columns to produce buy/sell decisions."""

import numpy as np
import pandas as pd


def add_sma_signal(
    df: pd.DataFrame,
    sma_s: str = "sma_s",
    sma_l: str = "sma_l",
    label: str | None = None,
) -> pd.DataFrame:
    """Append a golden cross signal column to df.

    Produces 1 (long) when the short SMA is above the long SMA, 0 otherwise.
    Expects the SMA columns to already exist on ``df``; run ``add_sma`` first.

    Column is named ``signal`` by default. Pass ``label`` to rename it, useful
    when stacking multiple signals on the same DataFrame.

    Args:
        df: DataFrame containing ``sma_s`` and ``sma_l`` columns.
        sma_s: Name of the short SMA column (default ``"sma_s"``).
        sma_l: Name of the long SMA column (default ``"sma_l"``).
        label: Optional name for the output column.

    Returns:
        Copy of ``df`` with the signal column appended.
    """
    df = df.copy()
    col = label if label is not None else "signal"
    df[col] = np.where(df[sma_s] > df[sma_l], 1, 0)
    return df
