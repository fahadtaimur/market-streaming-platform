"""Equity universe construction via OpenBB screener and discovery endpoints."""

import pandas as pd
from openbb import obb

from msp.config.settings import settings


def stock_screener(
    exchanges: set[str] | None = None,
    min_price: float = settings.universe_min_price,
    top_n_by_volume: int = settings.universe_top_n_by_volume,
    screener_limit: int = settings.universe_screener_limit,
) -> list[str]:
    """Return a filtered list of equity tickers from the OpenBB screener."""
    if exchanges is None:
        exchanges = set(settings.universe_exchanges)
    df = obb.equity.screener(limit=screener_limit).to_dataframe()  # type: ignore[union-attr]
    df = df[df["exchange"].isin(exchanges)]

    df = df.query(f"price > {min_price}").nlargest(top_n_by_volume, columns="volume")

    return df["symbol"].unique().tolist()


# Discovery
def get_gainers(**kwargs) -> pd.DataFrame:
    """Fetch top gaining stocks for the current session.

    Args:
        **kwargs: forwarded to obb.equity.discovery.gainers
    """
    return obb.equity.discovery.gainers(**kwargs).to_dataframe()  # type: ignore[union-attr]


def get_losers(**kwargs) -> pd.DataFrame:
    """Fetch top losing stocks for the current session.

    Args:
        **kwargs: forwarded to obb.equity.discovery.losers
    """
    return obb.equity.discovery.losers(**kwargs).to_dataframe()  # type: ignore[union-attr]


def get_active(**kwargs) -> pd.DataFrame:
    """Fetch most actively traded stocks by volume for the current session.

    Args:
        **kwargs: forwarded to obb.equity.discovery.active
    """
    return obb.equity.discovery.active(**kwargs).to_dataframe()  # type: ignore[union-attr]


def get_growth_tech(**kwargs) -> pd.DataFrame:
    """Fetch high-growth technology stocks.

    Args:
        **kwargs: forwarded to obb.equity.discovery.growth_tech
    """
    return obb.equity.discovery.growth_tech(**kwargs).to_dataframe()  # type: ignore[union-attr]


def get_aggressive_small_caps(**kwargs) -> pd.DataFrame:
    """Fetch small-cap stocks with aggressive growth characteristics.

    Args:
        **kwargs: forwarded to obb.equity.discovery.aggressive_small_caps
    """
    return obb.equity.discovery.aggressive_small_caps(**kwargs).to_dataframe()  # type: ignore[union-attr]


def get_undervalued_growth(**kwargs) -> pd.DataFrame:
    """Fetch undervalued stocks with strong growth metrics.

    Args:
        **kwargs: forwarded to obb.equity.discovery.undervalued_growth
    """
    return obb.equity.discovery.undervalued_growth(**kwargs).to_dataframe()  # type: ignore[union-attr]


def get_undervalued_large_caps(**kwargs) -> pd.DataFrame:
    """Fetch undervalued large-cap stocks.

    Args:
        **kwargs: forwarded to obb.equity.discovery.undervalued_large_caps
    """
    return obb.equity.discovery.undervalued_large_caps(**kwargs).to_dataframe()  # type: ignore[union-attr]
