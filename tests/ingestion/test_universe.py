"""Unit tests for msp.ingestion.universe.stock_screener."""

from unittest.mock import MagicMock, patch

import pandas as pd

from msp.ingestion.universe import stock_screener

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
