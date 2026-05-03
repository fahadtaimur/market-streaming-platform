"""Integration tests for historical data feeds."""

import pytest
import pandas as pd

from tests.helpers import assert_valid_dataframe
from msp.ingestion.historical import (
    get_historical_equity_price,
    get_intraday_equity_bars,
    get_equity_profile,
    get_insider_trades,
    get_historical_dividends,
    get_earnings_estimates,
    get_treasury_rates,
    get_effective_federal_funds_rate,
    get_cpi,
    get_unemployment,
    get_m2,
    get_index_historical,
    get_fx_historical,
    get_income_statement,
    get_balance_sheet,
    get_cash_flow,
    get_equity_quote,
    get_petroleum_status_report,
    get_composite_leading_indicator,
    get_yield_curve,
    get_equity_news,
)

SYMBOL = "AAPL"
SYMBOLS = ["AAPL", "MSFT"]
START_DATE = "2024-01-01"
END_DATE = "2024-03-31"


@pytest.mark.integration
def test_get_historical_equity_price_returns_ohlcv():
    df = get_historical_equity_price(
        symbol=SYMBOL, start_date=START_DATE, end_date=END_DATE
    )
    assert_valid_dataframe(df, {"open", "high", "low", "close", "volume"})


@pytest.mark.integration
def test_get_intraday_equity_bars_single_symbol():
    df = get_intraday_equity_bars(
        symbols=SYMBOL,
        start_date="2026-01-01",
        end_date="2026-01-01",
    )
    assert_valid_dataframe(
        df,
        {
            "open",
            "high",
            "low",
            "close",
            "volume",
            "timestamp",
            "trade_count",
            "symbol",
        },
    )


@pytest.mark.integration
def test_get_intraday_equity_bars_multiple_symbols():
    df = get_intraday_equity_bars(
        symbols=SYMBOLS,
        start_date="2026-01-01",
        end_date="2026-01-01",
    )
    assert_valid_dataframe(
        df,
        {
            "open",
            "high",
            "low",
            "close",
            "volume",
            "timestamp",
            "trade_count",
            "symbol",
        },
    )
    assert set(df["symbol"]) == set(SYMBOLS)


@pytest.mark.integration
def test_get_equity_profile_returns_sector_and_industry():
    df = get_equity_profile(symbol=SYMBOLS)
    assert_valid_dataframe(df, {"sector"})


@pytest.mark.integration
def test_get_insider_trades_returns_transaction_data():
    df = get_insider_trades(symbol=SYMBOL)
    assert_valid_dataframe(
        df,
        {
            "acquisition_or_disposition",
            "securities_transacted",
            "securities_owned",
            "owner_title",
            "transaction_type",
            "transaction_price",
        },
    )


@pytest.mark.integration
def test_get_dividends_returns_dividend_amount():
    df = get_historical_dividends(
        symbol=SYMBOL, start_date=START_DATE, end_date=END_DATE
    )
    assert_valid_dataframe(df, {"amount"})


@pytest.mark.integration
def test_get_earnings_estimates_returns_estimates():
    df = get_earnings_estimates(symbol=SYMBOLS)
    assert_valid_dataframe(
        df,
        {
            "target_high",
            "target_low",
            "target_consensus",
            "target_median",
            "recommendation",
            "number_of_analysts",
        },
    )


@pytest.mark.integration
def test_get_treasury_rates_returns_historical_yields():
    df = get_treasury_rates(start_date=START_DATE, end_date=END_DATE)
    assert_valid_dataframe(df.reset_index(), {"date", "month_3", "year_2", "year_10"})


@pytest.mark.integration
def test_get_effective_federal_funds_rate():
    df = get_effective_federal_funds_rate(start_date=START_DATE, end_date=END_DATE)
    assert_valid_dataframe(df.reset_index(), {"date", "rate"})


# Macro
@pytest.mark.integration
def test_get_cpi_returns_inflation_data():
    df = get_cpi(start_date=START_DATE, end_date=END_DATE)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_unemployment_returns_unrate():
    df = get_unemployment(start_date=START_DATE, end_date=END_DATE)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_m2_returns_money_supply():
    df = get_m2(start_date=START_DATE, end_date=END_DATE)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_composite_leading_indicator_returns_data():
    df = get_composite_leading_indicator(start_date=START_DATE, end_date=END_DATE)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_yield_curve_returns_data():
    df = get_yield_curve()
    assert_valid_dataframe(df, {"maturity", "rate"})


# Index
@pytest.mark.integration
def test_get_index_historical_returns_ohlcv():
    df = get_index_historical(symbol="^GSPC", start_date=START_DATE, end_date=END_DATE)
    assert_valid_dataframe(df, {"open", "high", "low", "close", "volume"})


# Currency
@pytest.mark.integration
def test_get_fx_historical_returns_ohlcv():
    df = get_fx_historical(symbol="EURUSD", start_date=START_DATE, end_date=END_DATE)
    assert_valid_dataframe(df, {"open", "high", "low", "close"})


# Fundamentals
@pytest.mark.integration
def test_get_income_statement_returns_data():
    df = get_income_statement(symbol=SYMBOL)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_balance_sheet_returns_data():
    df = get_balance_sheet(symbol=SYMBOL)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_cash_flow_returns_data():
    df = get_cash_flow(symbol=SYMBOL)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.integration
def test_get_equity_quote_returns_price():
    df = get_equity_quote(symbol=SYMBOL)
    assert_valid_dataframe(df, {"last_price"})


# Commodity
@pytest.mark.integration
def test_get_petroleum_status_report_returns_data():
    df = get_petroleum_status_report(start_date=START_DATE, end_date=END_DATE)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


# News
@pytest.mark.integration
def test_get_equity_news_returns_headlines():
    df = get_equity_news(symbol=SYMBOL, limit=10)
    assert_valid_dataframe(df, {"title", "url"})
