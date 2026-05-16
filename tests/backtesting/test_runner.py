"""Tests for the walk-forward evaluation engine (runner.py)."""

from msp.backtesting.runner import evaluate_ticker
from msp.storage.parquet_storage import ParquetPriceStorage


def test_evaluate_ticker_returns_summary(
    stored_ticker: tuple[ParquetPriceStorage, str],
):
    store, symbol = stored_ticker
    result = evaluate_ticker(symbol=symbol, storage=store, split_date="2024-01-01")
    assert isinstance(result, dict)
    assert result.keys() == {
        "symbol",
        "sma_s",
        "sma_l",
        "total_return",
        "bnh_return",
        "beats_bnh",
        "sharpe",
        "max_drawdown",
        "n_trades",
    }
