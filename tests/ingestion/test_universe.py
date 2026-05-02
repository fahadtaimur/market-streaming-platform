"""Unit tests for msp.ingestion.universe.stock_screener. Integration tests for discovery endpoints."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from msp.ingestion.universe import (
    stock_screener,
    get_gainers,
    get_losers,
    get_active,
    get_growth_tech,
    get_aggressive_small_caps,
    get_undervalued_growth,
    get_undervalued_large_caps,
)

SAMPLE_ROWS = [
    {"symbol": "AAPL", "exchange": "NMS", "price": 180.0, "volume": 1_000_000},
    {"symbol": "SHOP", "exchange": "TSX", "price": 90.0, "volume": 700_000},
]


def _mock_obb(rows: list[dict]):
    """
    Patch obb so screener returns a fake DataFrame instead of hitting the API.
    """
    mock = MagicMock()
    mock.equity.screener.return_value.to_dataframe.return_value = pd.DataFrame(rows)
    return patch("msp.ingestion.universe.obb", mock)


def test_no_exchange_filter_when_none():
    """
    exchanges=None skips exchange filtering — TSX ticker must appear in result.
    """
    with _mock_obb(SAMPLE_ROWS):
        result = stock_screener(exchanges=None, min_price=0.0)

    assert "SHOP" in result


def test_passes_screener_limit():
    """screener_limit must be forwarded to the API call, not silently dropped."""
    with _mock_obb(SAMPLE_ROWS) as mock_obb:
        stock_screener(screener_limit=250)

    mock_obb.equity.screener.assert_called_once_with(limit=250)


# Discovery
@pytest.mark.integration
def test_get_gainers_returns_dataframe():
    df = get_gainers()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_losers_returns_dataframe():
    df = get_losers()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_active_returns_dataframe():
    df = get_active()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_growth_tech_returns_dataframe():
    df = get_growth_tech()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_aggressive_small_caps_returns_dataframe():
    df = get_aggressive_small_caps()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_undervalued_growth_returns_dataframe():
    df = get_undervalued_growth()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_undervalued_large_caps_returns_dataframe():
    df = get_undervalued_large_caps()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
