"""Tests for MACrossBacktester."""

import pytest
import pandas as pd

from msp.backtesting.ma_cross import MACrossBacktester


def test_ma_cross_optimize_returns_int_pair(ohlcv_with_signal: pd.DataFrame):
    bt = MACrossBacktester(initial_cash=100_000)
    sma_s, sma_l = bt.optimize(
        df=ohlcv_with_signal,
        sma_s_range=(5, 10, 1),
        sma_l_range=(15, 20, 1),
    )
    assert isinstance(sma_s, int) and isinstance(sma_l, int)


def test_ma_cross_run_output_columns(ohlcv_with_signal: pd.DataFrame):
    bt = MACrossBacktester(initial_cash=100_000)
    result = bt.run(ohlcv_with_signal, signal_col="signal")
    for col in ("signal_return", "strategy_equity", "bnh_return", "bnh_equity"):
        assert col in result.columns


def test_ma_cross_equity_starts_at_initial_cash(ohlcv_with_signal: pd.DataFrame):
    bt = MACrossBacktester(initial_cash=100_000)
    result = bt.run(ohlcv_with_signal, signal_col="signal")
    assert result["strategy_equity"].dropna().iloc[0] == 100_000


def test_ma_cross_summary_raises_before_run():
    bt = MACrossBacktester(initial_cash=100_000)
    with pytest.raises(RuntimeError):
        bt.summary()
