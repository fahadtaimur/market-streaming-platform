"""Tests for technical indicator module"""

import pandas as pd

from msp.features.technical import add_sma, add_bollinger_bands
from tests.helpers import assert_valid_dataframe


def test_sma_default_columns(ohlcv: pd.DataFrame):
    result = add_sma(ohlcv, periods=[10, 20])
    assert_valid_dataframe(result, {"SMA_10", "SMA_20"})


def test_sma_custom_labels(ohlcv: pd.DataFrame):
    result = add_sma(ohlcv, periods=[10, 20], labels=["sma_s", "sma_l"])
    assert_valid_dataframe(result, {"sma_s", "sma_l"})
    assert "SMA_10" not in result.columns
    assert "SMA_20" not in result.columns


def test_sma_no_mutation(ohlcv: pd.DataFrame):
    original_cols = list(ohlcv.columns)
    add_sma(ohlcv, periods=[10])
    assert list(ohlcv.columns) == original_cols


def test_bollinger_bands(ohlcv: pd.DataFrame):
    result = add_bollinger_bands(ohlcv, period=20, std=2)
    assert_valid_dataframe(
        result, {"BBL_20_2.0_2.0", "BBU_20_2.0_2.0", "BBM_20_2.0_2.0"}
    )


def test_bollinger_bands_all_columns_present(ohlcv: pd.DataFrame):
    result = add_bollinger_bands(ohlcv)
    assert any(c.startswith("BBU_") for c in result.columns)
    assert any(c.startswith("BBM_") for c in result.columns)
    assert any(c.startswith("BBL_") for c in result.columns)
    assert any(c.startswith("BBB_") for c in result.columns)
    assert any(c.startswith("BBP_") for c in result.columns)
