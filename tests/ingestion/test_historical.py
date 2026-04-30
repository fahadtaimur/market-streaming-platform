"""Integration tests for historical data feeds"""

import pytest
import pandas as pd

from msp.ingestion.historical import get_historical_equity_price


@pytest.mark.integration
def test_get_historical_equity_price_returns_dataframe():
    df = get_historical_equity_price(
        symbol="AAPL", start_date="2026-01-01", end_date="2026-01-01"
    )

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    EXPECTED_COLUMNS = {"open", "high", "low", "close", "volume"}
    assert EXPECTED_COLUMNS.issubset(df.columns)
