"""Equity universe construction via OpenBB screener."""

from openbb import obb


def stock_screener(
    exchanges: set[str] | None = None,
    min_price: float = 20.0,
    top_n_by_volume: int = 100,
    screener_limit: int = 500,
) -> list[str]:
    """Return a filtered list of equity tickers from the OpenBB screener.

    Defaults reflect config/universe_equities.yaml. Pass exchanges=None to
    skip exchange filtering.
    """
    df = obb.equity.screener(limit=screener_limit).to_dataframe()  # type: ignore[union-attr]

    if exchanges is not None:
        df = df[df["exchange"].isin(exchanges)]

    df = df.query(f"price > {min_price}").nlargest(top_n_by_volume, columns="volume")

    return df["symbol"].unique().tolist()
