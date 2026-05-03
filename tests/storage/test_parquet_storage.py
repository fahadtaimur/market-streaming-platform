"""Tests for ParquetPriceStorage"""

import pytest
import pandas as pd

from msp.storage.parquet_storage import ParquetPriceStorage
from tests.helpers import assert_valid_dataframe


SYMBOL = "AAPL"
SYMBOLS = ["AAPL", "MSFT"]
START_DATE = "2026-01-01"
END_DATE = "2026-01-31"
EXPECTED_COLUMNS = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "timestamp",
    "symbol",
}


def test_parquet_write_single_symbol(
    store: ParquetPriceStorage, sample_bars_single: pd.DataFrame
):
    store.write(df=sample_bars_single)
    df = store.read(symbol=SYMBOL)
    assert_valid_dataframe(df, EXPECTED_COLUMNS)
    assert len(df) == 3


def test_parquet_write_multi_symbol(
    store: ParquetPriceStorage, sample_bars_multi: pd.DataFrame
):
    store.write(df=sample_bars_multi)
    df = store.read_many(symbols=SYMBOLS)
    assert_valid_dataframe(df, EXPECTED_COLUMNS)
    assert set(df["symbol"]) == set(SYMBOLS)
    assert len(df) == 3


def test_parquet_read_single_symbol_with_time_window(
    store: ParquetPriceStorage, sample_bars_single: pd.DataFrame
):
    store.write(df=sample_bars_single)
    df = store.read(symbol=SYMBOL, start_date=START_DATE, end_date=END_DATE)
    assert_valid_dataframe(df, EXPECTED_COLUMNS)
    assert len(df) == 2


def test_parquet_read_multi_symbol_with_time_window(
    store: ParquetPriceStorage, sample_bars_multi: pd.DataFrame
):
    store.write(df=sample_bars_multi)
    df = store.read_many(symbols=SYMBOLS, start_date=START_DATE, end_date=END_DATE)
    assert_valid_dataframe(df, EXPECTED_COLUMNS)
    assert len(df) == 2


def test_parquet_delete(store: ParquetPriceStorage, sample_bars_single: pd.DataFrame):
    store.write(df=sample_bars_single)
    assert store.exists(symbol=SYMBOL)
    store.delete(symbol=SYMBOL)
    assert not store.exists(symbol=SYMBOL)
    assert store.read(symbol=SYMBOL).empty


@pytest.mark.integration
def test_intraday_bars_write_and_read(store: ParquetPriceStorage):
    from msp.ingestion.historical import get_intraday_equity_bars

    df = get_intraday_equity_bars(
        symbols="AAPL",
        start_date="2026-01-02",
        end_date="2026-01-02",
    )
    store.write(df)
    result = store.read(symbol="AAPL", start_date="2026-01-02", end_date="2026-01-02")

    assert not result.empty
    assert set(result["symbol"].unique()) == {"AAPL"}
    store.delete(symbol="AAPL")
    assert not store.exists(symbol="AAPL")
