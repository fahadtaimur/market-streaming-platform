"""Technical indicator functions for OHLCV DataFrames."""

import pandas as pd
import pandas_ta  # noqa: F401, side-effect import that registers the .ta accessor on pd.DataFrame


def add_sma(
    df: pd.DataFrame,
    periods: list[int] = [10, 20],
    col: str = "close",
    labels: list[str] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Append SMA columns for each period.

    Columns are named ``SMA_<period>`` by default (e.g. ``SMA_10``, ``SMA_20``).
    Pass ``labels`` to rename them in the same order as ``periods``.

    Args:
        df: DataFrame containing ``col``.
        periods: Window lengths to compute.
        col: Column to compute SMA on (default ``"close"``).
        labels: Optional rename list, e.g. ``["sma_s", "sma_l"]``. Length must
            match ``periods``.
        **kwargs: Forwarded to ``pandas_ta.sma``.

    Returns:
        Copy of ``df`` with SMA columns appended.
    """
    if labels is not None:
        if len(labels) != len(periods):
            raise ValueError(
                f"labels length {len(labels)} must match periods length {len(periods)}"
            )

    df = df.copy()
    ta_columns = []
    for period in periods:
        df.ta.sma(close=col, length=period, append=True, **kwargs)
        ta_columns.append(f"SMA_{period}")

    if labels is not None:
        df = df.rename(columns=dict(zip(ta_columns, labels)))
    return df


def add_ema(
    df: pd.DataFrame,
    periods: list[int] = [10, 20],
    col: str = "close",
    labels: list[str] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Append EMA columns for each period.

    Columns are named ``EMA_<period>`` by default (e.g. ``EMA_10``, ``EMA_20``).
    EMA weights recent prices more heavily than SMA, making it faster to react
    to price changes. Pass ``labels`` to rename in the same order as ``periods``.

    Args:
        df: DataFrame containing ``col``.
        periods: Window lengths to compute.
        col: Column to compute EMA on (default ``"close"``).
        labels: Optional rename list, e.g. ``["ema_s", "ema_l"]``. Length must
            match ``periods``.
        **kwargs: Forwarded to ``pandas_ta.ema``.

    Returns:
        Copy of ``df`` with EMA columns appended.
    """
    if labels is not None:
        if len(labels) != len(periods):
            raise ValueError(
                f"labels length {len(labels)} must match periods length {len(periods)}"
            )

    df = df.copy()
    ta_columns = []
    for period in periods:
        df.ta.ema(close=col, length=period, append=True, **kwargs)
        ta_columns.append(f"EMA_{period}")

    if labels is not None:
        df = df.rename(columns=dict(zip(ta_columns, labels)))
    return df


def add_rsi(
    df: pd.DataFrame,
    col: str = "close",
    period: int = 14,
    label: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Append RSI column to df.

    Column is named ``RSI_<period>``. RSI oscillates between 0 and 100;
    values above 70 suggest overbought conditions, below 30 oversold.

    Args:
        df: OHLCV DataFrame containing ``col``.
        col: Column to compute RSI on (default ``"close"``).
        period: Lookback window (default 14).
        label: Optional name for the output column.
        **kwargs: Forwarded to ``pandas_ta.rsi``.

    Returns:
        Copy of ``df`` with ``RSI_<period>`` appended.
    """
    df = df.copy()
    df.ta.rsi(close=col, length=period, append=True, **kwargs)
    if label is not None:
        df = df.rename(columns={f"RSI_{period}": label})
    return df


def add_macd(
    df: pd.DataFrame,
    col: str = "close",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    labels: list[str] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Append MACD, signal line, and histogram columns to df.

    Adds three columns: ``MACD_<fast>_<slow>_<signal>`` (MACD line),
    ``MACDs_<fast>_<slow>_<signal>`` (signal line), and
    ``MACDh_<fast>_<slow>_<signal>`` (histogram).

    Args:
        df: DataFrame containing ``col``.
        col: Column to compute MACD on (default ``"close"``).
        fast: Fast EMA period (default 12).
        slow: Slow EMA period (default 26).
        signal: Signal line EMA period (default 9).
        labels: Optional list of 3 names to replace the default column names,
            in order: ``[macd, signal, histogram]``.
        **kwargs: Forwarded to ``pandas_ta.macd``.

    Returns:
        Copy of ``df`` with MACD columns appended.
    """
    if labels is not None:
        if len(labels) != 3:
            raise ValueError(f"labels must have exactly 3 entries, got {len(labels)}")

    df = df.copy()
    df.ta.macd(close=col, fast=fast, slow=slow, signal=signal, append=True, **kwargs)
    if labels is not None:
        df = df.rename(
            columns={
                f"MACD_{fast}_{slow}_{signal}": labels[0],
                f"MACDs_{fast}_{slow}_{signal}": labels[1],
                f"MACDh_{fast}_{slow}_{signal}": labels[2],
            }
        )
    return df


def add_bollinger_bands(
    df: pd.DataFrame,
    col: str = "close",
    period: int = 20,
    std: float = 2.0,
    labels: list[str] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Append Bollinger Band columns to df.

    Appends five columns: upper band, mid (SMA), lower band, bandwidth (BBB),
    and percent-b (BBP). Default names are the pandas_ta defaults
    (e.g. ``BBU_20_2.0_2.0``); pass ``labels`` to override all five in order
    ``[upper, mid, lower, bandwidth, pct_b]``.

    BBB (bandwidth) = (upper - lower) / mid x 100. Low values signal a
    volatility squeeze; a large move is possibly imminent.

    BBP (percent-b) = (price - lower) / (upper - lower). Normalised 0-1
    position within the bands; useful as an ML feature.

    Args:
        df: DataFrame containing ``col``.
        col: Column to compute bands on (default ``"close"``).
        period: Rolling window for the mid SMA (default 20).
        std: Number of standard deviations for upper/lower bands (default 2.0).
        labels: Optional list of 5 names in order
            ``[upper, mid, lower, bandwidth, pct_b]``.
        **kwargs: Forwarded to ``pandas_ta.bbands``.

    Returns:
        Copy of ``df`` with Bollinger Band columns appended.
    """
    if labels is not None:
        if len(labels) != 5:
            raise ValueError(
                f"labels must have exactly 5 entries [upper, mid, lower, bandwidth, pct_b], "
                f"got {len(labels)}"
            )

    df = df.copy()
    df.ta.bbands(close=col, length=period, std=std, append=True, **kwargs)

    # Prefix selection guards against pandas_ta version differences in column name format
    upper_col = next(c for c in df.columns if c.startswith("BBU_"))
    mid_col = next(c for c in df.columns if c.startswith("BBM_"))
    lower_col = next(c for c in df.columns if c.startswith("BBL_"))
    bw_col = next(c for c in df.columns if c.startswith("BBB_"))
    pct_col = next(c for c in df.columns if c.startswith("BBP_"))

    if labels is not None:
        df = df.rename(
            columns={
                upper_col: labels[0],
                mid_col: labels[1],
                lower_col: labels[2],
                bw_col: labels[3],
                pct_col: labels[4],
            }
        )
    return df


def add_atr(
    df: pd.DataFrame,
    period: int = 14,
    label: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Append Average True Range column to df.

    ATR measures volatility as the average of true ranges over ``period`` bars.
    True range is the largest of: High-Low, |High-PrevClose|, |Low−PrevClose|.
    Wilder smoothing (RMA) is applied, identical to RSI's avg gain/loss.

    Column is named ``ATRr_<period>`` by default (the ``r`` suffix denotes RMA).
    Pass ``label`` to rename it (e.g. ``"atr"``).

    Useful for position sizing (``risk / ATR = shares``) and setting adaptive
    stop-loss distances that widen in high-volatility regimes.

    Args:
        df: OHLCV DataFrame with ``high``, ``low``, and ``close`` columns.
        period: Lookback window (default 14).
        label: Optional name for the output column.
        **kwargs: Forwarded to ``pandas_ta.atr``.

    Returns:
        Copy of ``df`` with ``ATRr_<period>`` appended.
    """
    df = df.copy()
    df.ta.atr(length=period, append=True, **kwargs)
    if label is not None:
        df = df.rename(columns={f"ATRr_{period}": label})
    return df


def add_obv(
    df: pd.DataFrame,
    label: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Append On-Balance Volume column to df.

    OBV starts at 0 and adds the full bar's volume on an up-close, subtracts
    it on a down-close, and adds 0 on an unchanged close. It is a running
    cumulative sum, so its absolute level is meaningless; only the trend and
    divergence from price matter.

    Column is named ``OBV`` by default (no period suffix; there is no window).
    Pass ``label`` to rename it.

    Divergence signal: price makes a new high but OBV does not → weakening
    buying pressure, potential reversal. The reverse applies for lows.

    Args:
        df: OHLCV DataFrame with ``close`` and ``volume`` columns.
        label: Optional name for the output column.
        **kwargs: Forwarded to ``pandas_ta.obv``.

    Returns:
        Copy of ``df`` with ``OBV`` appended.
    """
    df = df.copy()
    df.ta.obv(append=True, **kwargs)
    if label is not None:
        df = df.rename(columns={"OBV": label})
    return df


def add_vwap(
    df: pd.DataFrame,
    label: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Append VWAP column to df. Intraday use only.

    Prefer Alpaca's pre-computed ``vwap`` column when available; it is derived
    from actual trade ticks and is more accurate than this bar-level approximation.
    Use this function only for intraday data from sources that do not include VWAP
    (e.g. yfinance intraday bars).

    VWAP = cumsum(typical_price x volume) / cumsum(volume), where
    typical_price = (high + low + close) / 3. It resets each session.

    Requires a timezone-aware ``DatetimeIndex`` so pandas_ta can detect session
    boundaries. Column is named ``VWAP_D`` by default.

    Args:
        df: Intraday OHLCV DataFrame with ``high``, ``low``, ``close``, and
            ``volume`` columns, and a timezone-aware DatetimeIndex.
        label: Optional name for the output column.
        **kwargs: Forwarded to ``pandas_ta.vwap``.

    Returns:
        Copy of ``df`` with ``VWAP_D`` appended.
    """
    df = df.copy()
    df.ta.vwap(append=True, **kwargs)
    if label is not None:
        df = df.rename(columns={"VWAP_D": label})
    return df


def add_zscore(
    df: pd.DataFrame,
    col: str = "close",
    period: int = 20,
    label: str | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Append rolling z-score of a price series to df.

    Computes ``(price - rolling_mean) / rolling_std`` over ``period`` bars.
    The result is unitless and scale-free, making it useful for comparing
    mean-reversion signals across instruments with very different price levels.

    Mathematically equivalent to Bollinger Bands percent-b but centred at 0
    and expressed in standard-deviation units rather than normalised 0-1.
    Values beyond ±2 indicate the price is statistically extreme relative to
    its recent history.

    Column is named ``ZS_<period>`` by default. Pass ``label`` to rename it.

    Args:
        df: OHLCV DataFrame containing ``col``.
        period: Rolling window for mean and std (default 20).
        col: Column to compute z-score on (default ``"close"``).
        label: Optional name for the output column.
        **kwargs: Forwarded to ``pandas_ta.zscore``.

    Returns:
        Copy of ``df`` with ``ZS_<period>`` appended.
    """
    df = df.copy()
    df.ta.zscore(close=col, length=period, append=True, **kwargs)
    if label is not None:
        df = df.rename(columns={f"ZS_{period}": label})
    return df
