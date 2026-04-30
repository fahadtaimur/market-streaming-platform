"""Historical data loaders for equities, macro, fixed income, currency, and news."""

import os
from datetime import date

import pandas as pd
from openbb import obb

# Remove later - call from runner
obb.user.credentials.fred_api_key = os.getenv("FRED_API_KEY")  # type: ignore[union-attr]


# Equity related functions
def get_historical_equity_price(
    symbol: list[str] | str,
    start_date: date | str,
    end_date: date | str,
    provider: str = "yfinance",
    interval: str = "1d",
    **kwargs,
) -> pd.DataFrame:
    """Fetch OHLCV bars for one or more symbols.

    Args:
        symbol: single ticker or list of tickers
        start_date: start of the date range (YYYY-MM-DD or date object)
        end_date: end of the date range (YYYY-MM-DD or date object)
        provider: data provider, defaults to yfinance
        interval: bar frequency, e.g. "1d", "1h", "5m"
        **kwargs: forwarded to obb.equity.price.historical
    """
    return obb.equity.price.historical(  # type: ignore[union-attr]
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        interval=interval,
        **kwargs,
    ).to_dataframe()
