"""Historical data loaders for equities, macro, fixed income, currency, and news."""

import os
from datetime import date
import time

import requests
from dotenv import load_dotenv
import pandas as pd
from openbb import obb

load_dotenv()

# Remove later - call from runner
obb.user.credentials.fred_api_key = os.getenv("FRED_API_KEY")

# alpaca market
headers = {
    "APCA-API-KEY-ID": os.getenv("ALPACA_API_KEY"),
    "APCA-API-SECRET-KEY": os.getenv("ALPACA_SECRET_KEY"),
}


# Equity
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
    return obb.equity.price.historical(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        interval=interval,
        **kwargs,
    ).to_dataframe()


def get_intraday_equity_bars(
    symbols: str | list[str],
    start_date: str,
    end_date: str,
    limit: int = 10_000,
    frequency_timeframe: str = "1Min",
    timeout: int = 30,
    **kwargs,
) -> pd.DataFrame:
    """Fetch intraday OHLCV bars for one or more symbols via Alpaca.

    Args:
        symbols: single ticker or list of tickers
        start_date: start of the date range (YYYY-MM-DD)
        end_date: end of the date range (YYYY-MM-DD)
        limit: max bars per page, defaults to 10000
        frequency_timeframe: bar frequency — e.g. "1Min", "5Min", "1Hour", "1Day"
        **kwargs: additional Alpaca bars parameters (e.g. feed, adjustment, sort)
    """
    params = {
        **kwargs,
        "timeframe": frequency_timeframe,
        "start": start_date,
        "end": end_date,
        "limit": limit,
    }

    is_multi_symbol = isinstance(symbols, list)
    if not is_multi_symbol:
        endpoint = f"https://data.alpaca.markets/v2/stocks/{symbols}/bars"
    else:
        endpoint = "https://data.alpaca.markets/v2/stocks/bars"
        params["symbols"] = ",".join(symbols)

    bars: dict | list = [] if not is_multi_symbol else {}

    while True:
        try:
            response = requests.get(
                url=endpoint,
                headers=headers,
                params=params,
                timeout=timeout,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise TimeoutError(
                f"Alpaca request timed out after {timeout}s, try increasing timeout"
            )
        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                "Could not reach Alpaca API: Check network connection"
            )
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status == 401:
                raise PermissionError(
                    "Alpaca authentication failed: Check ALPACA_API_KEY and ALPACA_SECRET_KEY in .env"
                )
            if status == 403:
                raise PermissionError(
                    "Alpaca access denied: Your plan may not include this data"
                )
            if status == 429:
                raise RuntimeError(
                    "Alpaca rate limit exceeded: Reduce request frequency or increase timeout"
                )
            raise RuntimeError(f"Alpaca request failed with HTTP {status}: {e}")

        rate_limit_remaining = int(response.headers["X-Ratelimit-Remaining"])
        reset_at_time = int(response.headers["X-Ratelimit-Reset"])
        if rate_limit_remaining < 5:
            sleep_for = max(reset_at_time - time.time(), 0) + 1
            time.sleep(sleep_for)

        try:
            data = response.json()
            if not is_multi_symbol:
                bars.extend(data["bars"])
            else:
                for symbol, records in data["bars"].items():
                    bars.setdefault(symbol, []).extend(records)
        except KeyError as e:
            raise ValueError(
                f"Unexpected Alpaca response structure — missing key {e}: {data}"
            )

        if not data["next_page_token"]:
            break
        params["page_token"] = data["next_page_token"]

    columns = {
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "t": "timestamp",
        "vw": "vwap",
        "n": "trade_count",
    }

    if not is_multi_symbol:
        df = pd.DataFrame(bars).assign(symbol=symbols).rename(columns=columns)
    else:
        frames = [pd.DataFrame(val).assign(symbol=key) for key, val in bars.items()]
        df = pd.concat(frames, ignore_index=True).rename(columns=columns)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def get_equity_profile(
    symbol: list[str] | str,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch sector, industry, and company profile for one or more symbols.

    Args:
        symbol: single ticker or list of tickers
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.equity.profile
    """
    return obb.equity.profile(symbol=symbol, provider=provider, **kwargs).to_dataframe()


def get_insider_trades(symbol: str, provider: str = "sec", **kwargs) -> pd.DataFrame:
    """Fetch insider buy/sell transactions for a symbol via SEC EDGAR.

    Args:
        symbol: ticker symbol
        provider: data provider, defaults to sec
        **kwargs: forwarded to obb.equity.ownership.insider_trading
    """
    return obb.equity.ownership.insider_trading(
        symbol=symbol, provider=provider, **kwargs
    ).to_dataframe()


def get_historical_dividends(
    symbol: str,
    start_date: str | date | None,
    end_date: str | date | None,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch dividend history for a symbol.

    Args:
        symbol: ticker symbol
        start_date: start of the date range
        end_date: end of the date range
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.equity.fundamental.dividends
    """
    return obb.equity.fundamental.dividends(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        **kwargs,
    ).to_dataframe()


def get_earnings_estimates(
    symbol: list[str] | str,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch analyst EPS and revenue estimates for one or more symbols.

    Args:
        symbol: single ticker or list of tickers
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.equity.estimates.consensus
    """
    return obb.equity.estimates.consensus(
        symbol=symbol, provider=provider, **kwargs
    ).to_dataframe()


# Fixed Income
def get_treasury_rates(
    start_date: date | str,
    end_date: date | str,
    provider: str = "federal_reserve",
    **kwargs,
) -> pd.DataFrame:
    """Fetch historical treasury yields across maturities (2Y, 5Y, 10Y, 30Y).

    Args:
        start_date: start of the date range
        end_date: end of the date range
        provider: data provider, defaults to federal_reserve
        **kwargs: forwarded to obb.fixedincome.government.treasury_rates
    """
    return obb.fixedincome.government.treasury_rates(
        provider=provider,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    ).to_dataframe()


def get_effective_federal_funds_rate(
    start_date: date | str,
    end_date: date | str,
    provider: str = "federal_reserve",
    **kwargs,
) -> pd.DataFrame:
    """Fetch the effective federal funds rate over time.

    Args:
        start_date: start of the date range
        end_date: end of the date range
        provider: data provider, defaults to federal_reserve
        **kwargs: forwarded to obb.fixedincome.rate.effr
    """
    return obb.fixedincome.rate.effr(
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        **kwargs,
    ).to_dataframe()


def get_yield_curve(**kwargs) -> pd.DataFrame:
    """Fetch the current yield curve snapshot.

    Args:
        **kwargs: forwarded to obb.fixedincome.government.yield_curve
    """
    return obb.fixedincome.government.yield_curve(**kwargs).to_dataframe()


# Macro
def get_cpi(
    start_date: date | str,
    end_date: date | str,
    provider: str = "fred",
    **kwargs,
) -> pd.DataFrame:
    """Fetch CPI inflation data.

    Args:
        start_date: start of the date range
        end_date: end of the date range
        provider: data provider, defaults to fred
        **kwargs: forwarded to obb.economy.cpi
    """
    return obb.economy.cpi(
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        **kwargs,
    ).to_dataframe()


def get_unemployment(
    start_date: date | str,
    end_date: date | str,
    **kwargs,
) -> pd.DataFrame:
    """Fetch unemployment rate (UNRATE) via FRED.

    Args:
        start_date: start of the date range
        end_date: end of the date range
        **kwargs: forwarded to obb.economy.fred_series
    """
    return obb.economy.fred_series(
        symbol="UNRATE",
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    ).to_dataframe()


def get_m2(
    start_date: date | str,
    end_date: date | str,
    provider: str = "federal_reserve",
    **kwargs,
) -> pd.DataFrame:
    """Fetch M2 money supply over time.

    Args:
        start_date: start of the date range
        end_date: end of the date range
        provider: data provider, defaults to federal_reserve
        **kwargs: forwarded to obb.economy.money_measures
    """
    return obb.economy.money_measures(
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        **kwargs,
    ).to_dataframe()


def get_composite_leading_indicator(
    start_date: date | str,
    end_date: date | str,
    **kwargs,
) -> pd.DataFrame:
    """Fetch OECD composite leading indicator. Crossing above 100 signals expansion.

    Args:
        start_date: start of the date range
        end_date: end of the date range
        **kwargs: forwarded to obb.economy.composite_leading_indicator
    """
    return obb.economy.composite_leading_indicator(
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    ).to_dataframe()


# Index
def get_index_historical(
    symbol: list[str] | str,
    start_date: date | str,
    end_date: date | str,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch historical prices for market indices (e.g. ^GSPC, ^VIX, ^RUT).

    Args:
        symbol: index symbol or list of symbols
        start_date: start of the date range
        end_date: end of the date range
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.index.price.historical
    """
    return obb.index.price.historical(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        **kwargs,
    ).to_dataframe()


# Currency
def get_fx_historical(
    symbol: list[str] | str,
    start_date: date | str,
    end_date: date | str,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch historical FX rates for currency pairs (e.g. EURUSD, GBPUSD).

    Args:
        symbol: currency pair or list of pairs
        start_date: start of the date range
        end_date: end of the date range
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.currency.price.historical
    """
    return obb.currency.price.historical(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        **kwargs,
    ).to_dataframe()


# Fundamentals
def get_income_statement(
    symbol: str,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch income statement for a symbol.

    Args:
        symbol: ticker symbol
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.equity.fundamental.income
    """
    return obb.equity.fundamental.income(
        symbol=symbol, provider=provider, **kwargs
    ).to_dataframe()


def get_balance_sheet(
    symbol: str,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch balance sheet for a symbol.

    Args:
        symbol: ticker symbol
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.equity.fundamental.balance
    """
    return obb.equity.fundamental.balance(
        symbol=symbol, provider=provider, **kwargs
    ).to_dataframe()


def get_cash_flow(
    symbol: str,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch cash flow statement for a symbol.

    Args:
        symbol: ticker symbol
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.equity.fundamental.cash
    """
    return obb.equity.fundamental.cash(
        symbol=symbol, provider=provider, **kwargs
    ).to_dataframe()


def get_equity_quote(
    symbol: list[str] | str,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch the latest price quote for one or more symbols.

    Args:
        symbol: single ticker or list of tickers
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.equity.price.quote
    """
    return obb.equity.price.quote(
        symbol=symbol, provider=provider, **kwargs
    ).to_dataframe()


# Commodity
def get_petroleum_status_report(
    start_date: date | str,
    end_date: date | str,
    **kwargs,
) -> pd.DataFrame:
    """Fetch EIA weekly petroleum inventory report. Surprise draw is bullish for energy names.

    Args:
        start_date: start of the date range
        end_date: end of the date range
        **kwargs: forwarded to obb.commodity.petroleum_status_report
    """
    return obb.commodity.petroleum_status_report(
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    ).to_dataframe()


# News
def get_equity_news(
    symbol: list[str] | str,
    limit: int = 50,
    provider: str = "yfinance",
    **kwargs,
) -> pd.DataFrame:
    """Fetch recent news headlines for one or more symbols.

    Args:
        symbol: single ticker or list of tickers
        limit: number of articles to return
        provider: data provider, defaults to yfinance
        **kwargs: forwarded to obb.news.company
    """
    return obb.news.company(
        symbol=symbol,
        limit=limit,
        provider=provider,
        **kwargs,
    ).to_dataframe()
